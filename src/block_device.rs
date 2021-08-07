// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Block device helper functions.
*/

use snafu::{ensure, ResultExt, Snafu};
use std::convert::TryFrom;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::path::Path;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

/// Generates the blkgetsize64 function.
mod ioctl {
    use nix::ioctl_read;
    ioctl_read!(blkgetsize64, 0x12, 114, u64);
}

/// Find the size of a block device.
pub(crate) fn get_block_device_size(path: &Path) -> Result<i64> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .context(error::OpenFile { path })?;

    let mut block_device_size = 0;
    let result = unsafe { ioctl::blkgetsize64(file.as_raw_fd(), &mut block_device_size) }
        .context(error::GetBlockDeviceSize { path })?;
    ensure!(result == 0, error::InvalidBlockDeviceSize { result });

    let block_device_size =
        i64::try_from(block_device_size).with_context(|| error::ConvertNumber {
            what: "block device size",
            number: block_device_size.to_string(),
            target: "i64",
        })?;
    Ok(block_device_size)
}

/// Potential errors from block device helper functions.
mod error {
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub(super)")]
    pub(super) enum Error {
        #[snafu(display("Failed to open '{}': {}", path.display(), source))]
        OpenFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to get block device size for '{}': {}", path.display(), source))]
        GetBlockDeviceSize { path: PathBuf, source: nix::Error },

        #[snafu(display("Invalid block device size: {}", result))]
        InvalidBlockDeviceSize { result: i32 },

        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },
    }
}
