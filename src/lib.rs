// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
A library that uses the Amazon EBS direct APIs to work with snapshots.

# Examples

Downloading a snapshot into a disk image:
```
use coldsnap::SnapshotDownloader;
use aws_sdk_ebs::Client as EbsClient;
use std::path::Path;

# async fn doc() {
let client = EbsClient::new(&aws_config::from_env().region("us-west-2").load().await);
let downloader = SnapshotDownloader::new(client);
let path = Path::new("./disk.img");

downloader.download_to_file("snap-1234", &path, None)
    .await
    .expect("failed to download snapshot");
# }
```

Uploading a disk image into a snapshot:
```
use coldsnap::SnapshotUploader;
use aws_sdk_ebs::Client as EbsClient;
use std::path::Path;

# async fn doc() {
let client = EbsClient::new(&aws_config::from_env().region("us-west-2").load().await);
let uploader = SnapshotUploader::new(client);
let path = Path::new("./disk.img");

let snapshot_id = uploader.upload_from_file(&path, None, None, None, None, None, None)
        .await
        .expect("failed to upload snapshot");
# }
```

Waiting for a snapshot to be completed:
```
use coldsnap::SnapshotWaiter;
use aws_sdk_ec2::Client as Ec2Client;

# async fn doc() {
let client = Ec2Client::new(&aws_config::from_env().region("us-west-2").load().await);
let waiter = SnapshotWaiter::new(client);

waiter.wait_for_completed("snap-1234")
        .await
        .expect("failed to wait for snapshot");
# }
```
*/

mod block_device;
mod download;
mod upload;
mod wait;

pub use download::Error as DownloadError;
pub use download::SnapshotDownloader;

pub use upload::Error as UploadError;
pub use upload::SnapshotUploader;
pub use upload::ZeroBlocks as UploadZeroBlocks;

pub use wait::Error as WaitError;
pub use wait::{SnapshotWaiter, WaitParams};

/// Errors from the AWS Rust SDK crate often swallow relevant information when they are printed
/// using `Display`. This leads to errors that do not have enough information for the user to know
/// what went wrong. This function `Display` prints an error and recursively adds up to n levels of
/// underlying errors to that printed message.
pub(crate) fn error_stack(e: &dyn std::error::Error, n: u16) -> String {
    let mut current_error = e;
    let mut s = format!("{e}");

    for _ in 0..n {
        current_error = match current_error.source() {
            None => return s,
            Some(next_error) => next_error,
        };
        s += &format!(": {current_error}")
    }
    s
}
