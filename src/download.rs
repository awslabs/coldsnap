// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Download Amazon EBS snapshots.
*/

use crate::block_device::get_block_device_size;
use async_trait::async_trait;
use aws_sdk_ebs::Client as EbsClient;
use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine as _;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::SeekFrom;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::time;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

const GIBIBYTE: i64 = 1024 * 1024 * 1024;
const SNAPSHOT_BLOCK_WORKERS: usize = 64;
// How long to wait between attempts; this number * attempt number, in seconds.
const SNAPSHOT_BLOCK_RETRY_SCALE: u64 = 2;
// 5 retries with scale 2 gives us 20 seconds of backoff, matching upload behavior.
const SNAPSHOT_BLOCK_ATTEMPTS: u64 = 5;
const SHA256_ALGORITHM: &str = "SHA256";

// ListSnapshotBlocks allows us to specify how many blocks are returned in each
// query, from the default of 100 to the maximum of 10000. Since we fetch all
// the block information up front in a loop, we ask for the maximum so that we
// need fewer API calls.
const LIST_REQUEST_MAX_RESULTS: i32 = 10000;
const CHECKPOINT_FLUSH_INTERVAL: usize = 100;

/// Specify how checkpointing should be handled for resumable downloads.
#[derive(Copy, Clone, PartialEq)]
pub enum CheckpointBehavior {
    /// Disable checkpointing, ignore existing checkpoint.
    Disable,
    /// Enable checkpointing for resumable downloads.
    Enable,
    /// Enable checkpointing, keep checkpoint file after successful download.
    EnableAndKeep,
}

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
    ///   of the snapshot. If the snapshot is sparse, i.e. not all blocks are present, then the file
    ///   will contain holes that return zeroes when read.
    /// * `progress_bar` is optional, since output to the terminal may not be wanted.
    /// * `checkpoint` specifies checkpointing behavior for resumable downloads.
    pub async fn download_to_file<P: AsRef<Path>>(
        &self,
        snapshot_id: &str,
        path: P,
        progress_bar: Option<ProgressBar>,
        checkpoint: Option<CheckpointBehavior>,
    ) -> Result<()> {
        let checkpoint = checkpoint.unwrap_or(CheckpointBehavior::Disable);
        let path = path.as_ref();
        let _ = path
            .file_name()
            .context(error::ValidateFileNameSnafu { path })?;

        // Find the overall volume size, the block size, and the metadata we need for each block:
        // the index, which lets us calculate the offset into the volume; and the token, which we
        // need to retrieve it.
        let mut snapshot: Snapshot = self.list_snapshot_blocks(snapshot_id).await?;

        // Check for checkpoint file and filter out already-completed blocks
        let checkpoint_file = progress_path(path);
        let mut previously_completed: Vec<i32> = Vec::new();
        let mut resuming = false;

        let checkpoint_data = match checkpoint != CheckpointBehavior::Disable {
            true => tokio::fs::read_to_string(&checkpoint_file).await.ok(),
            false => None,
        }
        .and_then(|data| serde_json::from_str::<ProgressFile>(&data).ok())
        .filter(|p| p.snapshot_id == snapshot_id);

        if let Some(progress) = checkpoint_data {
            let completed: std::collections::BTreeSet<i32> =
                progress.completed_blocks.iter().copied().collect();
            let original_count = snapshot.blocks.len();
            snapshot.blocks.retain(|b| !completed.contains(&b.index));
            previously_completed = progress.completed_blocks;
            debug!(
                "Resuming download: {} of {} blocks remaining",
                snapshot.blocks.len(),
                original_count
            );
            resuming = true;
        }

        let mut target = if BlockDeviceTarget::is_valid(path).await? {
            BlockDeviceTarget::new_target(path)?
        } else {
            FileTarget::new_target(path)?
        };

        if !resuming {
            debug!("Writing {}G to {}...", snapshot.volume_size, path.display());
            target.grow(snapshot.volume_size * GIBIBYTE).await?;
        }

        self.write_snapshot_blocks(
            snapshot,
            target.write_path()?,
            path,
            progress_bar,
            checkpoint,
            previously_completed,
        )
        .await?;
        target.finalize().await?;

        // Clean up checkpoint file on success
        if checkpoint != CheckpointBehavior::EnableAndKeep {
            let _ = std::fs::remove_file(&checkpoint_file);
        }

        Ok(())
    }

    async fn write_snapshot_blocks(
        &self,
        snapshot: Snapshot,
        write_path: &Path,
        progress_path_base: &Path,
        progress_bar: Option<ProgressBar>,
        checkpoint: CheckpointBehavior,
        previously_completed: Vec<i32>,
    ) -> Result<()> {
        // Collect errors encountered while downloading blocks, since we can't
        // return a result directly through `for_each_concurrent`.
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let completed_blocks = Arc::new(Mutex::new(previously_completed));
        let last_flush_count = Arc::new(Mutex::new(0usize));

        // We may have a progress bar to update.
        let progress_bar = match progress_bar {
            Some(pb) => {
                let pb_length = snapshot.blocks.len();
                let pb_length =
                    u64::try_from(pb_length).with_context(|_| error::ConvertNumberSnafu {
                        what: "progress bar length",
                        number: pb_length.to_string(),
                        target: "u64",
                    })?;
                pb.set_length(pb_length);
                Arc::new(Some(pb))
            }
            None => Arc::new(None),
        };

        let snapshot_id_for_flush = snapshot.snapshot_id.clone();
        let progress_path_for_flush = progress_path_base.to_path_buf();

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
        let download =
            stream::iter(block_contexts).for_each_concurrent(SNAPSHOT_BLOCK_WORKERS, |context| {
                let completed_blocks = Arc::clone(&completed_blocks);
                let last_flush_count = Arc::clone(&last_flush_count);
                let snapshot_id = snapshot_id_for_flush.clone();
                let progress_file_path = progress_path_for_flush.clone();
                async move {
                    for attempt in 0..SNAPSHOT_BLOCK_ATTEMPTS {
                        if attempt > 0 {
                            let backoff = Duration::from_secs(attempt * SNAPSHOT_BLOCK_RETRY_SCALE);
                            debug!(
                                "block {}: retry {}/{}, backoff {}s",
                                context.block_index,
                                attempt,
                                SNAPSHOT_BLOCK_ATTEMPTS,
                                backoff.as_secs()
                            );
                            time::sleep(backoff).await;
                        }

                        let block_result = self.download_block(&context).await;
                        {
                            let mut block_errors = context.block_errors.lock().expect("poisoned");
                            if let Err(e) = block_result {
                                warn!(
                                    "block {}: attempt {}/{} failed: {}",
                                    context.block_index,
                                    attempt + 1,
                                    SNAPSHOT_BLOCK_ATTEMPTS,
                                    e
                                );
                                block_errors.insert(context.block_index, e);
                                continue;
                            }
                            block_errors.remove(&context.block_index);
                        }

                        if checkpoint == CheckpointBehavior::Disable {
                            break;
                        }

                        // Track completion and flush checkpoint periodically
                        let completed_count = {
                            let mut completed = completed_blocks.lock().expect("poisoned");
                            completed.push(context.block_index);
                            completed.len()
                        };

                        let should_flush = {
                            let mut last_flush = last_flush_count.lock().expect("poisoned");
                            if completed_count - *last_flush >= CHECKPOINT_FLUSH_INTERVAL {
                                *last_flush = completed_count;
                                true
                            } else {
                                false
                            }
                        };

                        if should_flush {
                            let blocks: Vec<i32> =
                                completed_blocks.lock().expect("poisoned").clone();
                            write_progress(&progress_file_path, &snapshot_id, &blocks).await;
                        }
                        break;
                    }
                }
            });
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
            // Final flush before returning error
            if checkpoint != CheckpointBehavior::Disable {
                let blocks: Vec<i32> = completed_blocks.lock().expect("poisoned").clone();
                write_progress(progress_path_base, &snapshot.snapshot_id, &blocks).await;
            }

            let failed_blocks: Vec<i32> = block_errors.keys().copied().collect();
            let error_report = format!("blocks {:?}", failed_blocks);
            error::GetSnapshotBlocksSnafu {
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
        let max_results = LIST_REQUEST_MAX_RESULTS;
        let mut next_token = None;
        let mut volume_size;
        let mut block_size;

        loop {
            let response = self
                .ebs_client
                .list_snapshot_blocks()
                .snapshot_id(snapshot_id)
                .set_next_token(next_token)
                .max_results(max_results)
                .send()
                .await
                .context(error::ListSnapshotBlocksSnafu { snapshot_id })?;

            volume_size = response
                .volume_size
                .context(error::FindVolumeSizeSnafu { snapshot_id })?;

            block_size = response
                .block_size
                .context(error::FindBlockSizeSnafu { snapshot_id })?;

            for block in response.blocks.unwrap_or_default().iter() {
                let index = block
                    .block_index
                    .context(error::FindBlockIndexSnafu { snapshot_id })?;

                let token = String::from(block.block_token.as_ref().context(
                    error::FindBlockPropertySnafu {
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

        let response = context
            .ebs_client
            .get_snapshot_block()
            .snapshot_id(snapshot_id)
            .block_index(block_index)
            .block_token(block_token)
            .send()
            .await
            .context(error::GetSnapshotBlockSnafu {
                snapshot_id,
                block_index,
            })?;

        let expected_hash = response.checksum.context(error::FindBlockPropertySnafu {
            snapshot_id,
            block_index,
            property: "checksum",
        })?;

        let checksum_algorithm = response
            .checksum_algorithm
            .context(error::FindBlockPropertySnafu {
                snapshot_id,
                block_index,
                property: "checksum algorithm",
            })?
            .as_str()
            .to_string();

        let data_length = response
            .data_length
            .context(error::FindBlockPropertySnafu {
                snapshot_id,
                block_index,
                property: "data length",
            })?;

        let block_data_stream =
            response
                .block_data
                .collect()
                .await
                .context(error::CollectByteStreamSnafu {
                    snapshot_id,
                    block_index,
                    property: "data",
                })?;

        let block_data = block_data_stream.into_bytes();

        ensure!(
            checksum_algorithm == SHA256_ALGORITHM,
            error::UnexpectedBlockChecksumAlgorithmSnafu {
                snapshot_id,
                block_index,
                checksum_algorithm,
            }
        );
        let block_data_length = block_data.len();
        let block_data_length =
            i32::try_from(block_data_length).with_context(|_| error::ConvertNumberSnafu {
                what: "block data length",
                number: block_data_length.to_string(),
                target: "i32",
            })?;

        ensure!(
            data_length > 0 && data_length <= block_size && data_length == block_data_length,
            error::UnexpectedBlockDataLengthSnafu {
                snapshot_id,
                block_index,
                data_length,
            }
        );

        let mut block_digest = Sha256::new();
        block_digest.update(&block_data);
        let hash_bytes = block_digest.finalize();
        let block_hash = base64_engine.encode(hash_bytes);

        ensure!(
            block_hash == expected_hash,
            error::BadBlockChecksumSnafu {
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
            .context(error::OpenFileSnafu { path })?;

        // Calculate the offset to write the block into the target file
        let block_index_u64 =
            u64::try_from(context.block_index).with_context(|_| error::ConvertNumberSnafu {
                what: "block index",
                number: context.block_index.to_string(),
                target: "u64",
            })?;
        let block_size_u64 =
            u64::try_from(block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block size",
                number: block_size.to_string(),
                target: "u64",
            })?;
        let offset = block_index_u64 * block_size_u64;

        f.seek(SeekFrom::Start(offset))
            .await
            .context(error::SeekFileOffsetSnafu { path, offset })?;

        let count = usize::try_from(data_length).with_context(|_| error::ConvertNumberSnafu {
            what: "byte count",
            number: data_length.to_string(),
            target: "usize",
        })?;

        f.write_all(&block_data)
            .await
            .context(error::WriteFileBytesSnafu { path, count })?;

        f.flush().await.context(error::FlushFileSnafu { path })?;

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
    block_size: i32,
    blocks: Vec<SnapshotBlock>,
}

/// Stores the metadata about a snapshot block.
struct SnapshotBlock {
    index: i32,
    token: String,
}

/// Stores the context needed to download a snapshot block.
struct BlockContext {
    path: PathBuf,
    block_index: i32,
    block_token: String,
    block_size: i32,
    snapshot_id: String,
    block_errors: Arc<Mutex<BTreeMap<i32, Error>>>,
    progress_bar: Arc<Option<ProgressBar>>,
    ebs_client: EbsClient,
}

/// Shared interface for write targets.
#[async_trait]
trait SnapshotWriteTarget {
    // grow the target to the desired length
    async fn grow(&mut self, length: i64) -> Result<()>;

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path>;

    // persist the contents to disk
    async fn finalize(&mut self) -> Result<()>;
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
            .context(error::ReadFileMetadataSnafu { path })?;

        if file_meta.file_type().is_block_device() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl SnapshotWriteTarget for BlockDeviceTarget {
    // ensures existing size >= length, but otherwise leaves untouched
    async fn grow(&mut self, length: i64) -> Result<()> {
        let path = self.path.as_path();
        let block_device_size =
            get_block_device_size(path).context(error::GetBlockDeviceSizeSnafu)?;

        // Make sure the block device is big enough to hold the snapshot
        ensure!(
            block_device_size >= length,
            error::BlockDeviceTooSmallSnafu {
                block_device_size: block_device_size / GIBIBYTE,
                needed: length / GIBIBYTE,
            }
        );

        Ok(())
    }

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path> {
        Ok(self.path.as_path())
    }

    // no-op
    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Implements file operations for filesystem files.
struct FileTarget {
    path: PathBuf,
    partial_path: PathBuf,
}

impl FileTarget {
    fn new_target<P: AsRef<Path>>(path: P) -> Result<Box<dyn SnapshotWriteTarget>> {
        let path = path.as_ref();
        let mut partial_path = path.as_os_str().to_owned();
        partial_path.push(".partial");
        Ok(Box::new(FileTarget {
            path: path.into(),
            partial_path: PathBuf::from(partial_path),
        }))
    }
}

#[async_trait]
impl SnapshotWriteTarget for FileTarget {
    // truncate file to desired size
    async fn grow(&mut self, length: i64) -> Result<()> {
        let file_len = u64::try_from(length).with_context(|_| error::ConvertNumberSnafu {
            what: "file length",
            number: length.to_string(),
            target: "u64",
        })?;

        let file = std::fs::File::create(&self.partial_path).context(error::CreateFileSnafu {
            path: &self.partial_path,
        })?;

        file.set_len(file_len).context(error::ExtendFileSnafu {
            path: &self.partial_path,
        })?;

        Ok(())
    }

    fn write_path(&self) -> Result<&Path> {
        Ok(self.partial_path.as_path())
    }

    // persist file to destination
    async fn finalize(&mut self) -> Result<()> {
        tokio::fs::rename(&self.partial_path, &self.path)
            .await
            .context(error::RenameFileSnafu {
                from: &self.partial_path,
                to: &self.path,
            })?;
        Ok(())
    }
}

/// Checkpoint progress file for resumable downloads.
#[derive(Serialize, Deserialize)]
struct ProgressFile {
    snapshot_id: String,
    completed_blocks: Vec<i32>,
}

/// Returns the path to the checkpoint progress file for a given target path.
fn progress_path(target_path: &Path) -> PathBuf {
    let mut path = target_path.as_os_str().to_owned();
    path.push(".coldsnap-progress");
    PathBuf::from(path)
}

/// Writes checkpoint progress to disk.
async fn write_progress(target_path: &Path, snapshot_id: &str, completed_blocks: &[i32]) {
    let progress = ProgressFile {
        snapshot_id: snapshot_id.to_string(),
        completed_blocks: completed_blocks.to_vec(),
    };
    if let Ok(data) = serde_json::to_string(&progress) {
        let _ = tokio::fs::write(progress_path(target_path), data).await;
    }
}

/// Potential errors while downloading a snapshot and writing to a local file.
mod error {
    use aws_sdk_ebs::{
        self,
        operation::{
            get_snapshot_block::GetSnapshotBlockError,
            list_snapshot_blocks::ListSnapshotBlocksError,
        },
    };
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
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

        #[snafu(display("Failed to create file '{}': {}", path.display(), source))]
        CreateFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to extend file '{}': {}", path.display(), source))]
        ExtendFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to rename '{}' to '{}': {}", from.display(), to.display(), source))]
        RenameFile {
            from: PathBuf,
            to: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to list snapshot blocks '{snapshot_id}': {source}", source = crate::error_stack(source, 2)))]
        ListSnapshotBlocks {
            snapshot_id: String,
            #[snafu(source(from(aws_sdk_ebs::error::SdkError<ListSnapshotBlocksError>, Box::new)))]
            source: Box<aws_sdk_ebs::error::SdkError<ListSnapshotBlocksError>>,
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
            block_index: i32,
            property: String,
        },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        CollectByteStream {
            snapshot_id: String,
            block_index: i32,
            property: String,
            #[snafu(source(from(aws_sdk_ebs::primitives::ByteStreamError, Box::new)))]
            source: Box<aws_sdk_ebs::primitives::ByteStreamError>,
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
            #[snafu(source(from(aws_sdk_ebs::error::SdkError<GetSnapshotBlockError>, Box::new)))]
            source: Box<aws_sdk_ebs::error::SdkError<GetSnapshotBlockError>>,
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

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn progress_path_appends_suffix() {
        let path = Path::new("/tmp/disk.img");
        let progress = progress_path(path);
        assert_eq!(progress, PathBuf::from("/tmp/disk.img.coldsnap-progress"));
    }

    #[test]
    fn progress_file_roundtrip() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("disk.img");

        let progress = ProgressFile {
            snapshot_id: "snap-123".to_string(),
            completed_blocks: vec![0, 5, 10],
        };

        let path = progress_path(&target);
        let data = serde_json::to_string(&progress).unwrap();
        std::fs::write(&path, &data).unwrap();

        let loaded: ProgressFile =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();

        assert_eq!(loaded.snapshot_id, "snap-123");
        assert_eq!(loaded.completed_blocks, vec![0, 5, 10]);
    }

    #[test]
    fn progress_file_filters_completed_blocks() {
        let all_blocks = vec![
            SnapshotBlock {
                index: 0,
                token: "a".into(),
            },
            SnapshotBlock {
                index: 1,
                token: "b".into(),
            },
            SnapshotBlock {
                index: 2,
                token: "c".into(),
            },
            SnapshotBlock {
                index: 3,
                token: "d".into(),
            },
        ];

        let completed: std::collections::BTreeSet<i32> = vec![0, 2].into_iter().collect();
        let remaining: Vec<_> = all_blocks
            .into_iter()
            .filter(|b| !completed.contains(&b.index))
            .collect();

        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].index, 1);
        assert_eq!(remaining[1].index, 3);
    }

    #[test]
    fn progress_file_ignores_mismatched_snapshot_id() {
        let progress = ProgressFile {
            snapshot_id: "snap-different".to_string(),
            completed_blocks: vec![0, 1, 2],
        };

        let current_snapshot_id = "snap-123";
        let should_resume = progress.snapshot_id == current_snapshot_id;

        assert!(!should_resume);
    }
}
