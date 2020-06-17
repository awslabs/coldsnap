// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
A library that uses the Amazon EBS direct APIs to work with snapshots.

# Examples

Uploading a disk image into a snapshot:
```
use coldsnap::SnapshotUploader;
use rusoto_ebs::EbsClient;
use std::path::Path;

# async fn doc() {
let client = EbsClient::new(rusoto_core::Region::UsWest2);
let uploader = SnapshotUploader::new(client);
let path = Path::new("./disk.img");

let snapshot_id = uploader.upload_from_file(&path, None, None, None)
        .await
        .expect("failed to upload snapshot");
# }
```
*/

mod upload;
pub use upload::Error as UploadError;
pub use upload::SnapshotUploader;
