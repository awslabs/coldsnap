// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Upload Amazon EBS snapshots.
*/

use crate::block_device::get_block_device_size;
use aws_sdk_ebs::model::{ChecksumAggregationMethod, ChecksumAlgorithm};
use aws_sdk_ebs::types::ByteStream;
use aws_sdk_ebs::Client as EbsClient;
use bytes::BytesMut;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use log::debug;
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::cmp;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering as AtomicOrdering};
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
const SNAPSHOT_TIMEOUT_MINUTES: i32 = 10;
const SHA256_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::ChecksumAlgorithmSha256;
const LINEAR_METHOD: ChecksumAggregationMethod =
    ChecksumAggregationMethod::ChecksumAggregationLinear;

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

        let file_meta = fs::metadata(path)
            .await
            .context(error::ReadFileMetadataSnafu { path })?;

        let file_size = if file_meta.file_type().is_block_device() {
            get_block_device_size(path).context(error::GetBlockDeviceSizeSnafu)?
        } else {
            self.file_size(&file_meta).await?
        };

        // EBS snapshots must be multiples of 1 GiB in size.
        let min_volume_size = cmp::max((file_size + GIBIBYTE - 1) / GIBIBYTE, 1);

        // If a volume size was provided, make sure it will be big enough.
        let volume_size = volume_size.unwrap_or(min_volume_size);
        ensure!(
            volume_size >= min_volume_size,
            error::BadVolumeSizeSnafu {
                requested: volume_size,
                needed: min_volume_size,
            }
        );

        // Start the snapshot, which gives us the ID and block size we need.
        debug!("Uploading {}G to snapshot...", volume_size);
        let (snapshot_id, block_size) = self.start_snapshot(volume_size, description).await?;
        let file_blocks = (file_size + i64::from(block_size - 1)) / i64::from(block_size);
        let file_blocks =
            i32::try_from(file_blocks).with_context(|_| error::ConvertNumberSnafu {
                what: "calculate file blocks",
                number: file_blocks.to_string(),
                target: "i32",
            })?;

        // We skip sparse blocks, so we need to keep track of how many we send.
        let changed_blocks_count = Arc::new(AtomicI32::new(0));

        // Track the hashes of uploaded blocks for the final hash.
        let block_digests = Arc::new(Mutex::new(BTreeMap::new()));

        // Collect errors encountered while uploading blocks, since we can't
        // return a result directly through `for_each_concurrent`.
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));

        // We may have a progress bar to update.
        let progress_bar = match progress_bar {
            Some(pb) => {
                let pb_length = file_blocks;
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

        // Create a context for each block that can be moved to another thread.
        let mut block_contexts = Vec::new();
        let mut remaining_data = file_size;
        for i in 0..file_blocks {
            // The file length may not be an exact multiple of the block size,
            // so we need to keep track of how many bytes are left for the call
            // to `read_exact` later.
            let data_length = cmp::min(i64::from(block_size), remaining_data);
            let data_length =
                usize::try_from(data_length).with_context(|_| error::ConvertNumberSnafu {
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

            remaining_data -= i64::from(block_size);
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
                        debug!(
                            "Error uploading block, attempt {} of {}",
                            attempt + 1,
                            SNAPSHOT_BLOCK_ATTEMPTS
                        );
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
            let error_report: String = block_errors.values().map(|e| e.to_string()).collect();
            error::PutSnapshotBlocksSnafu {
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
    async fn file_size(&self, file_meta: &std::fs::Metadata) -> Result<i64> {
        let file_len = file_meta.len();
        let file_len = i64::try_from(file_len).with_context(|_| error::ConvertNumberSnafu {
            what: "file length",
            number: file_len.to_string(),
            target: "i64",
        })?;
        Ok(file_len)
    }

    /// Start a new snapshot and return the ID and block size for subsequent puts.
    async fn start_snapshot(&self, volume_size: i64, description: String) -> Result<(String, i32)> {
        let start_response = self
            .ebs_client
            .start_snapshot()
            .volume_size(volume_size)
            .set_description(Some(description))
            .set_timeout(Some(SNAPSHOT_TIMEOUT_MINUTES))
            .send()
            .await
            .context(error::StartSnapshotSnafu)?;

        let snapshot_id = start_response
            .snapshot_id
            .context(error::FindSnapshotIdSnafu)?;
        let block_size = start_response
            .block_size
            .context(error::FindSnapshotBlockSizeSnafu)?;

        Ok((snapshot_id, block_size))
    }

    /// Complete a snapshot.
    async fn complete_snapshot(
        &self,
        snapshot_id: &str,
        changed_blocks_count: i32,
        checksum: &str,
    ) -> Result<()> {
        self.ebs_client
            .complete_snapshot()
            .snapshot_id(snapshot_id)
            .changed_blocks_count(changed_blocks_count)
            .set_checksum(Some(checksum.to_string()))
            .set_checksum_algorithm(Some(SHA256_ALGORITHM))
            .set_checksum_aggregation_method(Some(LINEAR_METHOD))
            .send()
            .await
            .context(error::CompleteSnapshotSnafu { snapshot_id })?;

        Ok(())
    }

    /// Read from the file in context and upload a single block to the snapshot.
    async fn upload_block(&self, context: &BlockContext) -> Result<()> {
        let path: &Path = context.path.as_ref();
        let mut f = File::open(path)
            .await
            .context(error::OpenFileSnafu { path })?;

        let block_index_u64: u64 =
            u64::try_from(context.block_index).with_context(|_| error::ConvertNumberSnafu {
                what: "block_index",
                number: context.block_index.to_string(),
                target: "u64",
            })?;
        let block_size_u64: u64 =
            u64::try_from(context.block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block_size",
                number: context.block_size.to_string(),
                target: "u64",
            })?;

        let offset: u64 = block_index_u64
            .checked_mul(block_size_u64)
            .with_context(|| error::CheckedMultiplicationSnafu {
                right: "block_size",
                right_number: context.block_size.to_string(),
                left: "block_index",
                left_number: context.block_index.to_string(),
                target: "u64",
            })?;

        f.seek(SeekFrom::Start(offset))
            .await
            .context(error::SeekFileOffsetSnafu { path, offset })?;

        let block_size = context.block_size;
        let block_size =
            usize::try_from(block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block size",
                number: block_size.to_string(),
                target: "usize",
            })?;

        let mut block = BytesMut::with_capacity(block_size);
        let count = context.data_length;
        block.resize(count, 0x0);

        f.read_exact(block.as_mut())
            .await
            .context(error::ReadFileBytesSnafu {
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
        let block_hash = base64::encode(hash_bytes);

        let snapshot_id = &context.snapshot_id;
        let block_index = context.block_index;

        let data_length = block.len();
        let data_length =
            i32::try_from(data_length).with_context(|_| error::ConvertNumberSnafu {
                what: "data length",
                number: data_length.to_string(),
                target: "i32",
            })?;

        context
            .ebs_client
            .put_snapshot_block()
            .snapshot_id(snapshot_id.to_string())
            .block_index(block_index)
            .block_data(ByteStream::from(block.freeze()))
            .data_length(data_length)
            .checksum(block_hash)
            .checksum_algorithm(SHA256_ALGORITHM)
            .send()
            .await
            .context(error::PutSnapshotBlockSnafu {
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
    block_index: i32,
    block_size: i32,
    snapshot_id: String,
    changed_blocks_count: Arc<AtomicI32>,
    block_digests: Arc<Mutex<BTreeMap<i32, Vec<u8>>>>,
    block_errors: Arc<Mutex<BTreeMap<i32, Error>>>,
    progress_bar: Arc<Option<ProgressBar>>,
    ebs_client: EbsClient,
}

/// Potential errors while reading a local file and uploading a snapshot.
mod error {
    use aws_sdk_ebs::error::{CompleteSnapshotError, PutSnapshotBlockError, StartSnapshotError};
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
            source: aws_sdk_ebs::types::SdkError<StartSnapshotError>,
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
            source: aws_sdk_ebs::types::SdkError<PutSnapshotBlockError>,
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
            source: aws_sdk_ebs::types::SdkError<CompleteSnapshotError>,
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

        #[snafu(display(
            "Overflowed multiplying {} ({}) and {} ({}) inside a {}",
            left,
            left_number,
            right,
            right_number,
            target
        ))]
        CheckedMultiplication {
            left: String,
            left_number: String,
            right: String,
            right_number: String,
            target: String,
        },
    }
}
