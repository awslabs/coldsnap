// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Download Amazon EBS snapshots.
*/

use crate::block_device::get_block_device_size;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use rusoto_ebs::{Ebs, EbsClient};
use rusoto_ebs::{GetSnapshotBlockRequest, ListSnapshotBlocksRequest};
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::SeekFrom;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

const GIBIBYTE: i64 = 1024 * 1024 * 1024;
const SNAPSHOT_BLOCK_WORKERS: usize = 64;
const SNAPSHOT_BLOCK_ATTEMPTS: u8 = 3;
const SHA256_ALGORITHM: &str = "SHA256";

// ListSnapshotBlocks allows us to specify how many blocks are returned in each
// query, from the default of 100 to the maximum of 10000. Since we fetch all
// the block information up front in a loop, we ask for the maximum so that we
// need fewer API calls.
const LIST_REQUEST_MAX_RESULTS: i64 = 10000;

pub struct SnapshotDownloader {
    ebs_client: EbsClient,
}

impl SnapshotDownloader {
    pub fn new(ebs_client: EbsClient) -> Self {
        SnapshotDownloader { ebs_client }
    }

    /// Download a snapshot into the file at the specified path.
    /// * `snapshot_id` is the snapshot to download.
    /// * `path` is the destination file for the snapshot. It will be extended to the volume size
    /// of the snapshot. If the snapshot is sparse, i.e. not all blocks are present, then the file
    /// will contain holes that return zeroes when read.
    /// * `progress_bar` is optional, since output to the terminal may not be wanted.
    pub async fn download_to_file<P: AsRef<Path>>(
        &self,
        snapshot_id: &str,
        path: P,
        progress_bar: Option<ProgressBar>,
    ) -> Result<()> {
        let path = path.as_ref();
        let _ = path.file_name().context(error::ValidateFileName { path })?;

        // Find the overall volume size, the block size, and the metadata we need for each block:
        // the index, which lets us calculate the offset into the volume; and the token, which we
        // need to retrieve it.
        let snapshot: Snapshot = self.list_snapshot_blocks(snapshot_id).await?;

        let mut target = if BlockDeviceTarget::is_valid(path).await? {
            BlockDeviceTarget::new_target(path)?
        } else {
            // If not block assume file for now
            FileTarget::new_target(path)?
        };

        target.grow(snapshot.volume_size * GIBIBYTE).await?;
        self.write_snapshot_blocks(snapshot, target.write_path()?, progress_bar)
            .await?;
        target.finalize()?;

        Ok(())
    }

    async fn write_snapshot_blocks(
        &self,
        snapshot: Snapshot,
        write_path: &Path,
        progress_bar: Option<ProgressBar>,
    ) -> Result<()> {
        // Collect errors encountered while downloading blocks, since we can't
        // return a result directly through `for_each_concurrent`.
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));

        // We may have a progress bar to update.
        let progress_bar = match progress_bar {
            Some(pb) => {
                let pb_length = snapshot.blocks.len();
                let pb_length = u64::try_from(pb_length).with_context(|| error::ConvertNumber {
                    what: "progress bar length",
                    number: pb_length.to_string(),
                    target: "u64",
                })?;
                pb.set_length(pb_length);
                Arc::new(Some(pb))
            }
            None => Arc::new(None),
        };

        // Create a context for each block that can be moved to another thread.
        let mut block_contexts = Vec::new();
        for SnapshotBlock { index, token } in snapshot.blocks {
            block_contexts.push(BlockContext {
                path: write_path.to_path_buf(),
                block_index: index,
                block_token: token,
                block_size: snapshot.block_size,
                snapshot_id: snapshot.snapshot_id.clone(),
                block_errors: Arc::clone(&block_errors),
                progress_bar: Arc::clone(&progress_bar),
                ebs_client: self.ebs_client.clone(),
            });
        }

        // Distribute the work across a fixed number of concurrent workers.
        // New threads will be created by the runtime as needed, but we'll
        // only process this many blocks at once to limit resource usage.
        let download = stream::iter(block_contexts).for_each_concurrent(
            SNAPSHOT_BLOCK_WORKERS,
            |context| async move {
                for _ in 0..SNAPSHOT_BLOCK_ATTEMPTS {
                    let block_result = self.download_block(&context).await;
                    let mut block_errors = context.block_errors.lock().expect("poisoned");
                    if let Err(e) = block_result {
                        block_errors.insert(context.block_index, e);
                        continue;
                    }
                    block_errors.remove(&context.block_index);
                    break;
                }
            },
        );
        download.await;

        // At this point, all the concurrent jobs have finished, so all of the Arcs we copied have
        // been dropped. Hence there's exactly one strong reference and it's safe to `try_unwrap`
        // and `unwrap` the result to recover the contents. Any of the Mutexes inside are safe to
        // unwrap unless they've been poisoned by a panic, in which case we also panic.

        // Summarize any fatal errors.
        let block_errors = Arc::try_unwrap(block_errors)
            .expect("referenced")
            .into_inner()
            .expect("poisoned");
        let block_errors_count = block_errors.keys().len();
        if block_errors_count != 0 {
            let error_report: String = block_errors
                .iter()
                .map(|(_, e)| format!("\n{}", e))
                .collect();
            error::GetSnapshotBlocks {
                error_count: block_errors_count,
                snapshot_id: snapshot.snapshot_id,
                error_report,
            }
            .fail()?;
        }

        Ok(())
    }

    /// Retrieve the index and token for all snapshot blocks.
    async fn list_snapshot_blocks(&self, snapshot_id: &str) -> Result<Snapshot> {
        let mut blocks = Vec::new();
        let max_results = Some(LIST_REQUEST_MAX_RESULTS);
        let mut next_token = None;
        let mut volume_size;
        let mut block_size;

        loop {
            let request = ListSnapshotBlocksRequest {
                snapshot_id: snapshot_id.to_string(),
                next_token,
                max_results,
                ..Default::default()
            };

            let response = self
                .ebs_client
                .list_snapshot_blocks(request)
                .await
                .context(error::ListSnapshotBlocks { snapshot_id })?;

            volume_size = response
                .volume_size
                .context(error::FindVolumeSize { snapshot_id })?;

            block_size = response
                .block_size
                .context(error::FindBlockSize { snapshot_id })?;

            for block in response.blocks.unwrap_or_else(Vec::new).iter() {
                let index = block
                    .block_index
                    .context(error::FindBlockIndex { snapshot_id })?;

                let token = String::from(block.block_token.as_ref().context(
                    error::FindBlockProperty {
                        snapshot_id,
                        block_index: index,
                        property: "token",
                    },
                )?);

                blocks.push(SnapshotBlock { index, token });
            }

            next_token = response.next_token;
            if next_token.is_none() {
                break;
            }
        }

        Ok(Snapshot {
            snapshot_id: snapshot_id.to_string(),
            volume_size,
            block_size,
            blocks,
        })
    }

    /// Download a single block from the snapshot in context and write it to the file.
    async fn download_block(&self, context: &BlockContext) -> Result<()> {
        let snapshot_id = &context.snapshot_id;
        let block_index = context.block_index;
        let block_token = &context.block_token;
        let block_size = context.block_size;

        let block_request = GetSnapshotBlockRequest {
            snapshot_id: snapshot_id.to_string(),
            block_index,
            block_token: block_token.to_string(),
        };

        let response = context
            .ebs_client
            .get_snapshot_block(block_request)
            .await
            .context(error::GetSnapshotBlock {
                snapshot_id,
                block_index,
            })?;

        let expected_hash = response.checksum.context(error::FindBlockProperty {
            snapshot_id,
            block_index,
            property: "checksum",
        })?;

        let checksum_algorithm = response
            .checksum_algorithm
            .context(error::FindBlockProperty {
                snapshot_id,
                block_index,
                property: "checksum algorithm",
            })?;

        let data_length = response.data_length.context(error::FindBlockProperty {
            snapshot_id,
            block_index,
            property: "data length",
        })?;

        let block_data = response.block_data.context(error::FindBlockProperty {
            snapshot_id,
            block_index,
            property: "data",
        })?;

        ensure!(
            checksum_algorithm == SHA256_ALGORITHM,
            error::UnexpectedBlockChecksumAlgorithm {
                snapshot_id,
                block_index,
                checksum_algorithm,
            }
        );

        let block_data_length = block_data.len();
        let block_data_length =
            i64::try_from(block_data_length).with_context(|| error::ConvertNumber {
                what: "block data length",
                number: block_data_length.to_string(),
                target: "i64",
            })?;

        ensure!(
            data_length > 0 && data_length <= block_size && data_length == block_data_length,
            error::UnexpectedBlockDataLength {
                snapshot_id,
                block_index,
                data_length,
            }
        );

        let mut block_digest = Sha256::new();
        block_digest.update(&block_data);
        let hash_bytes = block_digest.finalize();
        let block_hash = base64::encode(&hash_bytes);

        ensure!(
            block_hash == expected_hash,
            error::BadBlockChecksum {
                snapshot_id,
                block_index,
                block_hash,
                expected_hash,
            }
        );

        // Blocks of all zeroes can be omitted from the file.
        let sparse = block_data.iter().all(|&byte| byte == 0u8);
        if sparse {
            if let Some(ref progress_bar) = *context.progress_bar {
                progress_bar.inc(1);
            }
            return Ok(());
        }

        let path: &Path = context.path.as_ref();
        let mut f = OpenOptions::new()
            .write(true)
            .open(path)
            .await
            .context(error::OpenFile { path })?;

        let offset = context.block_index * block_size;
        let offset = u64::try_from(offset).with_context(|| error::ConvertNumber {
            what: "file offset",
            number: offset.to_string(),
            target: "u64",
        })?;

        f.seek(SeekFrom::Start(offset))
            .await
            .context(error::SeekFileOffset { path, offset })?;

        let count = usize::try_from(data_length).with_context(|| error::ConvertNumber {
            what: "byte count",
            number: data_length.to_string(),
            target: "usize",
        })?;

        f.write_all(&block_data)
            .await
            .context(error::WriteFileBytes { path, count })?;

        f.flush().await.context(error::FlushFile { path })?;

        if let Some(ref progress_bar) = *context.progress_bar {
            progress_bar.inc(1);
        }

        Ok(())
    }
}

/// Stores the metadata about the snapshot contents.
struct Snapshot {
    snapshot_id: String,
    volume_size: i64,
    block_size: i64,
    blocks: Vec<SnapshotBlock>,
}

/// Stores the metadata about a snapshot block.
struct SnapshotBlock {
    index: i64,
    token: String,
}

/// Stores the context needed to download a snapshot block.
struct BlockContext {
    path: PathBuf,
    block_index: i64,
    block_token: String,
    block_size: i64,
    snapshot_id: String,
    block_errors: Arc<Mutex<BTreeMap<i64, Error>>>,
    progress_bar: Arc<Option<ProgressBar>>,
    ebs_client: EbsClient,
}

/// Potential errors while downloading a snapshot and writing to a local file.
mod error {
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub(super)")]
    pub(super) enum Error {
        #[snafu(display("Failed to read metadata for '{}': {}", path.display(), source))]
        ReadFileMetadata {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("{}", source))]
        GetBlockDeviceSize { source: crate::block_device::Error },

        #[snafu(display(
            "Block device too small: block device size {} GiB, needed at least {} GiB",
            block_device_size,
            needed
        ))]
        BlockDeviceTooSmall { block_device_size: i64, needed: i64 },

        #[snafu(display("Failed to validate file name '{}'", path.display()))]
        ValidateFileName { path: PathBuf },

        #[snafu(display("Failed to find parent directory for file name '{}'", path.display()))]
        ValidateParentDirectory { path: PathBuf },

        #[snafu(display("Failed to create temporary file in '{}': {}", path.display(), source))]
        CreateTempFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to extend temporary file '{}': {}", path.display(), source))]
        ExtendTempFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to persist temporary file '{}': {}", path.display(), source))]
        PersistTempFile {
            path: PathBuf,
            source: tempfile::PathPersistError,
        },

        #[snafu(display("Missing temporary file"))]
        MissingTempFile {},

        #[snafu(display("Failed to list snapshot blocks for '{}': {}", snapshot_id, source))]
        ListSnapshotBlocks {
            snapshot_id: String,
            source: rusoto_core::RusotoError<rusoto_ebs::ListSnapshotBlocksError>,
        },

        #[snafu(display("Failed to find volume size for '{}'", snapshot_id))]
        FindVolumeSize { snapshot_id: String },

        #[snafu(display("Failed to find index for block in '{}'", snapshot_id))]
        FindBlockIndex { snapshot_id: String },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        FindBlockProperty {
            snapshot_id: String,
            block_index: i64,
            property: String,
        },

        #[snafu(display("Failed to find block size for '{}'", snapshot_id))]
        FindBlockSize { snapshot_id: String },

        #[snafu(display(
            "Found unexpected checksum algorithm '{}' for block {} in '{}'",
            checksum_algorithm,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockChecksumAlgorithm {
            snapshot_id: String,
            block_index: i64,
            checksum_algorithm: String,
        },

        #[snafu(display(
            "Found unexpected data length {} for block {} in '{}'",
            data_length,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockDataLength {
            snapshot_id: String,
            block_index: i64,
            data_length: i64,
        },

        #[snafu(display(
            "Bad checksum for block {} in '{}': expected '{}', got '{}'",
            block_index,
            snapshot_id,
            expected_hash,
            block_hash,
        ))]
        BadBlockChecksum {
            snapshot_id: String,
            block_index: i64,
            block_hash: String,
            expected_hash: String,
        },

        #[snafu(display(
            "Failed to get block {} for snapshot '{}': {}",
            block_index,
            snapshot_id,
            source
        ))]
        GetSnapshotBlock {
            snapshot_id: String,
            block_index: i64,
            source: rusoto_core::RusotoError<rusoto_ebs::GetSnapshotBlockError>,
        },

        #[snafu(display(
            "Failed to get {} blocks for snapshot '{}': {}",
            error_count,
            snapshot_id,
            error_report
        ))]
        GetSnapshotBlocks {
            error_count: usize,
            snapshot_id: String,
            error_report: String,
        },

        #[snafu(display("Failed to flush '{}': {}", path.display(), source))]
        FlushFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to open '{}': {}", path.display(), source))]
        OpenFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to seek to {} in '{}': {}", offset, path.display(), source))]
        SeekFileOffset {
            path: PathBuf,
            offset: u64,
            source: std::io::Error,
        },

        #[snafu(display("Failed to write {} bytes to '{}': {}", count, path.display(), source))]
        WriteFileBytes {
            path: PathBuf,
            count: usize,
            source: std::io::Error,
        },

        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },
    }
}

/// Shared interface for write targets.
#[async_trait]
trait SnapshotWriteTarget: AsRef<Path> {
    // grow the target to the desired length
    async fn grow(&mut self, length: i64) -> Result<()>;

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path>;

    // persist the contents to disk
    fn finalize(&mut self) -> Result<()>;
}

/// Implements file operations for block devices.
struct BlockDeviceTarget {
    path: PathBuf,
}

impl BlockDeviceTarget {
    fn new_target<P: AsRef<Path>>(path: P) -> Result<Box<dyn SnapshotWriteTarget>> {
        let path = path.as_ref();
        Ok(Box::new(BlockDeviceTarget { path: path.into() }))
    }

    async fn is_valid<P: AsRef<Path>>(path: P) -> Result<bool> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(false);
        }

        let file_meta = fs::metadata(path)
            .await
            .context(error::ReadFileMetadata { path })?;

        if file_meta.file_type().is_block_device() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl AsRef<Path> for BlockDeviceTarget {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

#[async_trait]
impl SnapshotWriteTarget for BlockDeviceTarget {
    // ensures existing size >= length, but otherwise leaves untouched
    async fn grow(&mut self, length: i64) -> Result<()> {
        let path = self.as_ref();
        let block_device_size = get_block_device_size(path).context(error::GetBlockDeviceSize)?;

        // Make sure the block device is big enough to hold the snapshot
        ensure!(
            block_device_size >= length,
            error::BlockDeviceTooSmall {
                block_device_size: block_device_size / GIBIBYTE,
                needed: length / GIBIBYTE,
            }
        );

        Ok(())
    }

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path> {
        Ok(self.as_ref())
    }

    // no-op
    fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Implements file operations for filesystem files.
struct FileTarget {
    path: PathBuf,
    temp_file: Option<NamedTempFile>,
}

impl FileTarget {
    fn new_target<P: AsRef<Path>>(path: P) -> Result<Box<dyn SnapshotWriteTarget>> {
        let path = path.as_ref();
        Ok(Box::new(FileTarget {
            path: path.into(),
            temp_file: None,
        }))
    }
}

impl AsRef<Path> for FileTarget {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

#[async_trait]
impl SnapshotWriteTarget for FileTarget {
    // truncate file to desired size
    async fn grow(&mut self, length: i64) -> Result<()> {
        let path = self.as_ref();

        // Create a temporary file and extend it to the required size.
        let target_dir = path
            .parent()
            .context(error::ValidateParentDirectory { path })?;

        let temp_file = NamedTempFile::new_in(target_dir)
            .context(error::CreateTempFile { path: target_dir })?;

        let temp_file_len = length;
        let temp_file_len = u64::try_from(temp_file_len).with_context(|| error::ConvertNumber {
            what: "temp file length",
            number: temp_file_len.to_string(),
            target: "u64",
        })?;

        temp_file
            .as_file()
            .set_len(temp_file_len)
            .context(error::ExtendTempFile {
                path: temp_file.as_ref(),
            })?;

        self.temp_file.replace(temp_file);

        Ok(())
    }

    fn write_path(&self) -> Result<&Path> {
        let write_path = self.temp_file.as_ref().context(error::MissingTempFile {})?;

        Ok(write_path.as_ref())
    }

    // persist file to destination
    fn finalize(&mut self) -> Result<()> {
        let temp_file = self.temp_file.take().context(error::MissingTempFile {})?;

        let path = self.as_ref();
        temp_file
            .into_temp_path()
            .persist(&path)
            .context(error::PersistTempFile { path })?;

        Ok(())
    }
}
