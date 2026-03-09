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
use log::debug;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::io::{SeekFrom, Write};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
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
const LIST_REQUEST_MAX_RESULTS: i32 = 10000;

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
    pub async fn download_to_file<P: AsRef<Path>>(
        &self,
        snapshot_id: &str,
        path: P,
        progress_bar: Option<ProgressBar>,
    ) -> Result<()> {
        let path = path.as_ref();
        let _ = path
            .file_name()
            .context(error::ValidateFileNameSnafu { path })?;

        let snapshot: Snapshot = self.list_snapshot_blocks(snapshot_id).await?;

        let mut target = if BlockDeviceTarget::is_valid(path).await? {
            BlockDeviceTarget::new_target(path)?
        } else {
            FileTarget::new_target(path)?
        };

        // Load or create download manifest for resume support
        let completed_blocks = match DownloadManifest::load(path)? {
            Some(manifest) => {
                // Validate that the partial file exists when we have a manifest
                if let Ok(write_path) = target.write_path() {
                    if !write_path.exists() {
                        debug!(
                            "Manifest exists but partial file '{}' is missing, starting fresh",
                            write_path.display()
                        );
                        DownloadManifest::remove(path)?;
                        Arc::new(Mutex::new(HashSet::new()))
                    } else {
                        ensure!(
                            manifest.snapshot_id == snapshot_id
                                && manifest.volume_size == snapshot.volume_size
                                && manifest.block_size == snapshot.block_size,
                            error::ManifestMismatchSnafu { path }
                        );
                        debug!(
                            "Resuming download: {}/{} blocks already completed",
                            manifest.completed_blocks.len(),
                            manifest.total_blocks
                        );
                        Arc::new(Mutex::new(manifest.completed_blocks))
                    }
                } else {
                    ensure!(
                        manifest.snapshot_id == snapshot_id
                            && manifest.volume_size == snapshot.volume_size
                            && manifest.block_size == snapshot.block_size,
                        error::ManifestMismatchSnafu { path }
                    );
                    debug!(
                        "Resuming download: {}/{} blocks already completed",
                        manifest.completed_blocks.len(),
                        manifest.total_blocks
                    );
                    Arc::new(Mutex::new(manifest.completed_blocks))
                }
            }
            None => Arc::new(Mutex::new(HashSet::new())),
        };

        debug!("Writing {}G to {}...", snapshot.volume_size, path.display());
        let snapshot_volume_size = snapshot.volume_size;
        let snapshot_block_size = snapshot.block_size;
        let snapshot_total_blocks = snapshot.blocks.len();
        target.grow(snapshot_volume_size * GIBIBYTE).await?;

        let write_path = target.write_path()?;
        self.write_snapshot_blocks(
            snapshot,
            write_path,
            progress_bar,
            Arc::clone(&completed_blocks),
            path,
        )
        .await?;

        // Save a final manifest before finalize so that if we crash between here
        // and the rename, the next resume won't re-download the last batch of blocks.
        {
            let completed = completed_blocks.lock().expect("poisoned");
            let manifest = DownloadManifest {
                snapshot_id: snapshot_id.to_string(),
                volume_size: snapshot_volume_size,
                block_size: snapshot_block_size,
                total_blocks: snapshot_total_blocks,
                completed_blocks: completed.clone(),
            };
            manifest.save(path)?;
        }

        target.finalize()?;
        DownloadManifest::remove(path)?;

        Ok(())
    }

    async fn write_snapshot_blocks(
        &self,
        snapshot: Snapshot,
        write_path: &Path,
        progress_bar: Option<ProgressBar>,
        completed_blocks: Arc<Mutex<HashSet<i32>>>,
        manifest_path: &Path,
    ) -> Result<()> {
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));

        let total_blocks = snapshot.blocks.len();

        // Filter out already-completed blocks
        let already_done = {
            let completed = completed_blocks.lock().expect("poisoned");
            completed.len()
        };

        let block_size_u64 =
            u64::try_from(snapshot.block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block size",
                number: snapshot.block_size.to_string(),
                target: "u64",
            })?;

        let progress_bar = match progress_bar {
            Some(pb) => {
                let total_bytes = u64::try_from(total_blocks).with_context(|_| {
                    error::ConvertNumberSnafu {
                        what: "total blocks",
                        number: total_blocks.to_string(),
                        target: "u64",
                    }
                })? * block_size_u64;
                pb.set_length(total_bytes);
                let already_done_bytes = u64::try_from(already_done).with_context(|_| {
                    error::ConvertNumberSnafu {
                        what: "already done blocks",
                        number: already_done.to_string(),
                        target: "u64",
                    }
                })? * block_size_u64;
                pb.set_position(already_done_bytes);
                if already_done > 0 {
                    pb.reset_eta();
                }
                Arc::new(Some(pb))
            }
            None => Arc::new(None),
        };

        let mut block_contexts = Vec::new();
        {
            let completed = completed_blocks.lock().expect("poisoned");
            for SnapshotBlock { index, token } in snapshot.blocks {
                if completed.contains(&index) {
                    continue;
                }
                block_contexts.push(BlockContext {
                    path: write_path.to_path_buf(),
                    block_index: index,
                    block_token: token,
                    block_size: snapshot.block_size,
                    snapshot_id: snapshot.snapshot_id.clone(),
                    block_errors: Arc::clone(&block_errors),
                    progress_bar: Arc::clone(&progress_bar),
                    ebs_client: self.ebs_client.clone(),
                    completed_blocks: Arc::clone(&completed_blocks),
                });
            }
        }

        // Track how many blocks have completed since last manifest save, so we
        // can periodically persist progress and survive crashes/kills.
        let blocks_since_save = Arc::new(AtomicU32::new(0));
        // Save the manifest every N blocks to limit data loss on crash.
        const MANIFEST_SAVE_INTERVAL: u32 = 100;

        let manifest_meta = Arc::new(ManifestMeta {
            snapshot_id: snapshot.snapshot_id.clone(),
            volume_size: snapshot.volume_size,
            block_size: snapshot.block_size,
            total_blocks,
            manifest_path: manifest_path.to_path_buf(),
        });

        let download = stream::iter(block_contexts).for_each_concurrent(
            SNAPSHOT_BLOCK_WORKERS,
            |context| {
                let blocks_since_save = Arc::clone(&blocks_since_save);
                let manifest_meta = Arc::clone(&manifest_meta);
                async move {
                    for i in 0..SNAPSHOT_BLOCK_ATTEMPTS {
                        let block_result = self.download_block(&context).await;
                        let mut block_errors = context.block_errors.lock().expect("poisoned");
                        if let Err(e) = block_result {
                            debug!(
                                "Error downloading block, attempt {} of {}",
                                i + 1,
                                SNAPSHOT_BLOCK_ATTEMPTS
                            );
                            block_errors.insert(context.block_index, e);
                            continue;
                        }
                        block_errors.remove(&context.block_index);
                        // Track this block as completed
                        context
                            .completed_blocks
                            .lock()
                            .expect("poisoned")
                            .insert(context.block_index);

                        // Periodically save manifest to survive crashes
                        let count = blocks_since_save.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        if count >= MANIFEST_SAVE_INTERVAL {
                            blocks_since_save.store(0, AtomicOrdering::SeqCst);
                            let completed = context.completed_blocks.lock().expect("poisoned");
                            let manifest = DownloadManifest {
                                snapshot_id: manifest_meta.snapshot_id.clone(),
                                volume_size: manifest_meta.volume_size,
                                block_size: manifest_meta.block_size,
                                total_blocks: manifest_meta.total_blocks,
                                completed_blocks: completed.clone(),
                            };
                            if let Err(e) = manifest.save(&manifest_meta.manifest_path) {
                                debug!("Failed to save periodic manifest: {}", e);
                            }
                        }
                        break;
                    }
                }
            },
        );
        download.await;

        let block_errors = Arc::try_unwrap(block_errors)
            .expect("referenced")
            .into_inner()
            .expect("poisoned");
        let block_errors_count = block_errors.keys().len();
        if block_errors_count != 0 {
            // Save progress before returning error so next run can resume
            let completed = completed_blocks.lock().expect("poisoned");
            let manifest = DownloadManifest {
                snapshot_id: snapshot.snapshot_id.clone(),
                volume_size: snapshot.volume_size,
                block_size: snapshot.block_size,
                total_blocks,
                completed_blocks: completed.clone(),
            };
            manifest.save(manifest_path)?;

            let error_report: String = block_errors.values().map(|e| e.to_string()).collect();
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

        let block_size_u64 =
            u64::try_from(block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block size",
                number: block_size.to_string(),
                target: "u64",
            })?;

        // Blocks of all zeroes can be omitted from the file.
        let sparse = block_data.iter().all(|&byte| byte == 0u8);
        if sparse {
            if let Some(ref progress_bar) = *context.progress_bar {
                progress_bar.inc(block_size_u64);
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
        let offset = block_index_u64
            .checked_mul(block_size_u64)
            .context(error::OffsetOverflowSnafu {
                block_index: context.block_index,
                block_size,
            })?;

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
            progress_bar.inc(block_size_u64);
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

/// Tracks download progress so that interrupted downloads can be resumed.
#[derive(Debug, Serialize, Deserialize)]
struct DownloadManifest {
    snapshot_id: String,
    volume_size: i64,
    block_size: i32,
    total_blocks: usize,
    completed_blocks: HashSet<i32>,
}

impl DownloadManifest {
    fn manifest_path(download_path: &Path) -> PathBuf {
        let mut manifest = download_path.as_os_str().to_owned();
        manifest.push(".coldsnap-manifest");
        PathBuf::from(manifest)
    }

    fn load(path: &Path) -> Result<Option<Self>> {
        let manifest_path = Self::manifest_path(path);
        if !manifest_path.exists() {
            return Ok(None);
        }
        let data = std::fs::read_to_string(&manifest_path)
            .context(error::ReadManifestSnafu { path: &manifest_path })?;
        let manifest: DownloadManifest = serde_json::from_str(&data)
            .context(error::ParseManifestSnafu { path: &manifest_path })?;
        Ok(Some(manifest))
    }

    fn save(&self, download_path: &Path) -> Result<()> {
        let manifest_path = Self::manifest_path(download_path);
        let data = serde_json::to_string(self)
            .context(error::SerializeManifestSnafu { path: &manifest_path })?;
        let mut file = std::fs::File::create(&manifest_path)
            .context(error::WriteManifestSnafu { path: &manifest_path })?;
        file.write_all(data.as_bytes())
            .context(error::WriteManifestSnafu { path: &manifest_path })?;
        Ok(())
    }

    fn remove(download_path: &Path) -> Result<()> {
        let manifest_path = Self::manifest_path(download_path);
        if manifest_path.exists() {
            std::fs::remove_file(&manifest_path)
                .context(error::RemoveManifestSnafu { path: &manifest_path })?;
        }
        Ok(())
    }
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
    completed_blocks: Arc<Mutex<HashSet<i32>>>,
}

/// Holds snapshot metadata needed for periodic manifest saves.
struct ManifestMeta {
    snapshot_id: String,
    volume_size: i64,
    block_size: i32,
    total_blocks: usize,
    manifest_path: PathBuf,
}

/// Shared interface for write targets.
#[async_trait]
trait SnapshotWriteTarget {
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
    fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Implements file operations for filesystem files.
struct FileTarget {
    path: PathBuf,
    partial_path: PathBuf,
    is_resuming: bool,
}

impl FileTarget {
    fn new_target<P: AsRef<Path>>(path: P) -> Result<Box<dyn SnapshotWriteTarget>> {
        let path = path.as_ref();
        let mut partial = path.as_os_str().to_owned();
        partial.push(".coldsnap-partial");
        let partial_path = PathBuf::from(partial);
        let is_resuming = partial_path.exists();
        Ok(Box::new(FileTarget {
            path: path.into(),
            partial_path,
            is_resuming,
        }))
    }
}

#[async_trait]
impl SnapshotWriteTarget for FileTarget {
    async fn grow(&mut self, length: i64) -> Result<()> {
        let file_len = u64::try_from(length).with_context(|_| error::ConvertNumberSnafu {
            what: "file length",
            number: length.to_string(),
            target: "u64",
        })?;
        if self.is_resuming {
            // Verify the partial file is the expected size; re-extend if truncated
            let meta = std::fs::metadata(&self.partial_path)
                .context(error::ReadFileMetadataSnafu { path: &self.partial_path })?;
            if meta.len() != file_len {
                debug!(
                    "Partial file size {} doesn't match expected {}, re-extending",
                    meta.len(),
                    file_len
                );
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&self.partial_path)
                    .context(error::CreatePartialFileSnafu { path: &self.partial_path })?;
                file.set_len(file_len)
                    .context(error::ExtendPartialFileSnafu { path: &self.partial_path })?;
            }
            return Ok(());
        }
        let file = std::fs::File::create(&self.partial_path)
            .context(error::CreatePartialFileSnafu { path: &self.partial_path })?;
        file.set_len(file_len)
            .context(error::ExtendPartialFileSnafu { path: &self.partial_path })?;
        Ok(())
    }

    fn write_path(&self) -> Result<&Path> {
        Ok(self.partial_path.as_path())
    }

    fn finalize(&mut self) -> Result<()> {
        std::fs::rename(&self.partial_path, &self.path)
            .context(error::PersistPartialFileSnafu { path: &self.path })?;
        Ok(())
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

        #[snafu(display("Failed to create partial file '{}': {}", path.display(), source))]
        CreatePartialFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to extend partial file '{}': {}", path.display(), source))]
        ExtendPartialFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to persist partial file '{}': {}", path.display(), source))]
        PersistPartialFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to read manifest '{}': {}", path.display(), source))]
        ReadManifest {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to parse manifest '{}': {}", path.display(), source))]
        ParseManifest {
            path: PathBuf,
            source: serde_json::Error,
        },

        #[snafu(display("Failed to serialize manifest '{}': {}", path.display(), source))]
        SerializeManifest {
            path: PathBuf,
            source: serde_json::Error,
        },

        #[snafu(display("Failed to write manifest '{}': {}", path.display(), source))]
        WriteManifest {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to remove manifest '{}': {}", path.display(), source))]
        RemoveManifest {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display(
            "Manifest mismatch for '{}': snapshot or parameters changed since last attempt",
            path.display()
        ))]
        ManifestMismatch { path: PathBuf },

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
            block_index: i32,
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
            block_index: i32,
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
            block_index: i32,
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
            block_index: i32,
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

        #[snafu(display(
            "Offset overflow: block_index {} * block_size {} overflows u64",
            block_index,
            block_size
        ))]
        OffsetOverflow {
            block_index: i32,
            block_size: i32,
        },
    }
}
