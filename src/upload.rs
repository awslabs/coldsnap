// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Upload Amazon EBS snapshots.
*/

use bytes::BytesMut;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use rusoto_ebs::{CompleteSnapshotRequest, PutSnapshotBlockRequest, StartSnapshotRequest};
use rusoto_ebs::{Ebs, EbsClient};
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::cmp;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

const GIBIBYTE: i64 = 1024 * 1024 * 1024;
const SNAPSHOT_BLOCK_WORKERS: usize = 64;
// How long to wait between attempts; this number * attempt number, in seconds.
const SNAPSHOT_BLOCK_RETRY_SCALE: u64 = 2;
// 12 retries with scale 2 gives us 132 seconds, chosen because it got past "snapshot does not
// exist" errors in testing.
const SNAPSHOT_BLOCK_ATTEMPTS: u64 = 12;
const SNAPSHOT_TIMEOUT_MINUTES: i64 = 10;
const SHA256_ALGORITHM: &str = "SHA256";
const LINEAR_METHOD: &str = "LINEAR";

pub struct SnapshotUploader {
    ebs_client: EbsClient,
}

impl SnapshotUploader {
    pub fn new(ebs_client: EbsClient) -> Self {
        SnapshotUploader { ebs_client }
    }

    /// Upload a snapshot from the file at the specified path.
    /// * `path` is the source file for the snapshot.
    /// * `volume_size` is the desired size in GiB. If no size is provided (`None`), the source
    /// file's size will be rounded up to the nearest GiB and used instead.
    /// * `description` is the snapshot description. If no description is provided (`None`), the
    /// source file's name will be used instead.
    /// * `progress_bar` is optional, since output to the terminal may not be wanted.
    pub async fn upload_from_file<P: AsRef<Path>>(
        &self,
        path: P,
        volume_size: Option<i64>,
        description: Option<&str>,
        progress_bar: Option<ProgressBar>,
    ) -> Result<String> {
        let path = path.as_ref();
        let description = description.map(|s| s.to_string()).unwrap_or_else(|| {
            path.file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .to_string()
        });
        let file_size = self.file_size(path).await?;

        // EBS snapshots must be multiples of 1 GiB in size.
        let min_volume_size = cmp::max((file_size + GIBIBYTE - 1) / GIBIBYTE, 1);

        // If a volume size was provided, make sure it will be big enough.
        let volume_size = volume_size.unwrap_or(min_volume_size);
        ensure!(
            volume_size >= min_volume_size,
            error::BadVolumeSize {
                requested: volume_size,
                needed: min_volume_size,
            }
        );

        // Start the snapshot, which gives us the ID and block size we need.
        let (snapshot_id, block_size) = self.start_snapshot(volume_size, description).await?;
        let file_blocks = (file_size + block_size - 1) / block_size;

        // We skip sparse blocks, so we need to keep track of how many we send.
        let changed_blocks_count = Arc::new(AtomicI64::new(0));

        // Track the hashes of uploaded blocks for the final hash.
        let block_digests = Arc::new(Mutex::new(BTreeMap::new()));

        // Collect errors encountered while uploading blocks, since we can't
        // return a result directly through `for_each_concurrent`.
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));

        // We may have a progress bar to update.
        let progress_bar = match progress_bar {
            Some(pb) => {
                let pb_length = file_blocks;
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
        let mut remaining_data = file_size;
        for i in 0..file_blocks {
            // The file length may not be an exact multiple of the block size,
            // so we need to keep track of how many bytes are left for the call
            // to `read_exact` later.
            let data_length = cmp::min(block_size, remaining_data);
            let data_length =
                usize::try_from(data_length).with_context(|| error::ConvertNumber {
                    what: "data length",
                    number: data_length.to_string(),
                    target: "usize",
                })?;

            block_contexts.push(BlockContext {
                path: PathBuf::from(path),
                data_length,
                block_index: i,
                block_size,
                snapshot_id: snapshot_id.clone(),
                changed_blocks_count: Arc::clone(&changed_blocks_count),
                block_digests: Arc::clone(&block_digests),
                block_errors: Arc::clone(&block_errors),
                progress_bar: Arc::clone(&progress_bar),
                ebs_client: self.ebs_client.clone(),
            });

            remaining_data -= block_size;
        }

        // Distribute the work across a fixed number of concurrent workers.
        // New threads will be created by the runtime as needed, but we'll
        // only process this many blocks at once to limit resource usage.
        let upload = stream::iter(block_contexts).for_each_concurrent(
            SNAPSHOT_BLOCK_WORKERS,
            |context| async move {
                for attempt in 0..SNAPSHOT_BLOCK_ATTEMPTS {
                    // Increasing wait between attempts.  (No wait to start, on 0th attempt.)
                    time::sleep(Duration::from_secs(attempt * SNAPSHOT_BLOCK_RETRY_SCALE)).await;

                    let block_result = self.upload_block(&context).await;
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
        upload.await;

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
            error::PutSnapshotBlocks {
                error_count: block_errors_count,
                snapshot_id: snapshot_id.clone(),
                error_report,
            }
            .fail()?;
        }

        let changed_blocks_count = changed_blocks_count.load(AtomicOrdering::SeqCst);

        // Compute the "linear" hash - the hash of all hashes in block index order.
        let block_digests = Arc::try_unwrap(block_digests)
            .expect("referenced")
            .into_inner()
            .expect("poisoned");

        let mut full_digest = Sha256::new();
        for (_, hash_bytes) in block_digests {
            full_digest.update(&hash_bytes);
        }

        let full_hash = base64::encode(full_digest.finalize());

        self.complete_snapshot(&snapshot_id, changed_blocks_count, &full_hash)
            .await?;

        Ok(snapshot_id)
    }

    /// Find the size of a file.
    async fn file_size(&self, path: &Path) -> Result<i64> {
        let file_meta = fs::metadata(path)
            .await
            .context(error::ReadFileSize { path })?;
        let file_len = file_meta.len();
        let file_len = i64::try_from(file_len).with_context(|| error::ConvertNumber {
            what: "file length",
            number: file_len.to_string(),
            target: "i64",
        })?;
        Ok(file_len)
    }

    /// Start a new snapshot and return the ID and block size for subsequent puts.
    async fn start_snapshot(&self, volume_size: i64, description: String) -> Result<(String, i64)> {
        let start_request = StartSnapshotRequest {
            volume_size,
            description: Some(description),
            timeout: Some(SNAPSHOT_TIMEOUT_MINUTES),
            ..Default::default()
        };

        let start_response = self
            .ebs_client
            .start_snapshot(start_request)
            .await
            .context(error::StartSnapshot)?;

        let snapshot_id = start_response.snapshot_id.context(error::FindSnapshotId)?;
        let block_size = start_response
            .block_size
            .context(error::FindSnapshotBlockSize)?;

        Ok((snapshot_id, block_size))
    }

    /// Complete a snapshot.
    async fn complete_snapshot(
        &self,
        snapshot_id: &str,
        changed_blocks_count: i64,
        checksum: &str,
    ) -> Result<()> {
        let complete_request = CompleteSnapshotRequest {
            snapshot_id: snapshot_id.to_string(),
            changed_blocks_count,
            checksum: Some(checksum.to_string()),
            checksum_algorithm: Some(SHA256_ALGORITHM.to_string()),
            checksum_aggregation_method: Some(LINEAR_METHOD.to_string()),
        };

        self.ebs_client
            .complete_snapshot(complete_request)
            .await
            .context(error::CompleteSnapshot { snapshot_id })?;

        Ok(())
    }

    /// Read from the file in context and upload a single block to the snapshot.
    async fn upload_block(&self, context: &BlockContext) -> Result<()> {
        let path: &Path = context.path.as_ref();
        let mut f = File::open(path).await.context(error::OpenFile { path })?;

        let offset = context.block_index * context.block_size;
        let offset = u64::try_from(offset).with_context(|| error::ConvertNumber {
            what: "file offset",
            number: offset.to_string(),
            target: "u64",
        })?;

        f.seek(SeekFrom::Start(offset))
            .await
            .context(error::SeekFileOffset { path, offset })?;

        let block_size = context.block_size;
        let block_size = usize::try_from(block_size).with_context(|| error::ConvertNumber {
            what: "block size",
            number: block_size.to_string(),
            target: "usize",
        })?;

        let mut block = BytesMut::with_capacity(block_size);
        let count = context.data_length;
        block.resize(count, 0x0);

        f.read_exact(block.as_mut())
            .await
            .context(error::ReadFileBytes {
                path,
                count,
                offset,
            })?;

        // Blocks of all zeroes should be omitted from the snapshot.
        let sparse = block.iter().all(|&byte| byte == 0u8);
        if sparse {
            if let Some(ref progress_bar) = *context.progress_bar {
                progress_bar.inc(1);
            }
            return Ok(());
        }

        // Blocks must be padded to the expected block size.
        if block.len() < block_size {
            block.resize(block_size, 0x0);
        }

        let mut block_digest = Sha256::new();
        block_digest.update(&block);
        let hash_bytes = block_digest.finalize();
        let block_hash = base64::encode(&hash_bytes);

        let snapshot_id = &context.snapshot_id;
        let block_index = context.block_index;

        let data_length = block.len();
        let data_length = i64::try_from(data_length).with_context(|| error::ConvertNumber {
            what: "data length",
            number: data_length.to_string(),
            target: "i64",
        })?;

        let block_request = PutSnapshotBlockRequest {
            snapshot_id: snapshot_id.to_string(),
            block_index,
            block_data: block.freeze(),
            data_length,
            checksum: block_hash,
            checksum_algorithm: SHA256_ALGORITHM.to_string(),
            ..Default::default()
        };

        context
            .ebs_client
            .put_snapshot_block(block_request)
            .await
            .context(error::PutSnapshotBlock {
                snapshot_id,
                block_index,
            })?;

        let mut block_digests = context.block_digests.lock().expect("poisoned");
        block_digests.insert(block_index, hash_bytes.to_vec());

        let changed_blocks_count = &context.changed_blocks_count;
        changed_blocks_count.fetch_add(1, AtomicOrdering::SeqCst);

        if let Some(ref progress_bar) = *context.progress_bar {
            progress_bar.inc(1);
        }

        Ok(())
    }
}

/// Stores the context needed to upload a snapshot block.
struct BlockContext {
    path: PathBuf,
    data_length: usize,
    block_index: i64,
    block_size: i64,
    snapshot_id: String,
    changed_blocks_count: Arc<AtomicI64>,
    block_digests: Arc<Mutex<BTreeMap<i64, Vec<u8>>>>,
    block_errors: Arc<Mutex<BTreeMap<i64, Error>>>,
    progress_bar: Arc<Option<ProgressBar>>,
    ebs_client: EbsClient,
}

/// Potential errors while reading a local file and uploading a snapshot.
mod error {
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub(super)")]
    pub(super) enum Error {
        #[snafu(display("Failed to read size for '{}': {}", path.display(), source))]
        ReadFileSize {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display(
            "Bad volume size: requested {} GiB, needed at least {} GiB",
            requested,
            needed
        ))]
        BadVolumeSize { requested: i64, needed: i64 },

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

        #[snafu(display("Failed to read {} bytes at offset {} from '{}': {}", count, offset, path.display(), source))]
        ReadFileBytes {
            path: PathBuf,
            count: usize,
            offset: u64,
            source: std::io::Error,
        },

        #[snafu(display("Failed to start snapshot: {}", source))]
        StartSnapshot {
            source: rusoto_core::RusotoError<rusoto_ebs::StartSnapshotError>,
        },

        #[snafu(display(
            "Failed to put block {} for snapshot '{}': {}",
            block_index,
            snapshot_id,
            source
        ))]
        PutSnapshotBlock {
            snapshot_id: String,
            block_index: i64,
            source: rusoto_core::RusotoError<rusoto_ebs::PutSnapshotBlockError>,
        },

        #[snafu(display(
            "Failed to put {} blocks for snapshot '{}': {}",
            error_count,
            snapshot_id,
            error_report
        ))]
        PutSnapshotBlocks {
            error_count: usize,
            snapshot_id: String,
            error_report: String,
        },

        #[snafu(display("Failed to complete snapshot '{}': {}", snapshot_id, source))]
        CompleteSnapshot {
            snapshot_id: String,
            source: rusoto_core::RusotoError<rusoto_ebs::CompleteSnapshotError>,
        },

        #[snafu(display("Failed to find snapshot ID"))]
        FindSnapshotId {},

        #[snafu(display("Failed to find snapshot block size"))]
        FindSnapshotBlockSize {},

        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },
    }
}
