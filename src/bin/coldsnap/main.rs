// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
`coldsnap` is a command-line interface that uses the Amazon EBS direct APIs to upload and download
snapshots.
*/

#[macro_use]
mod client;

use argh::FromArgs;
use coldsnap::{SnapshotDownloader, SnapshotUploader, SnapshotWaiter, WaitParams};
use indicatif::{ProgressBar, ProgressStyle};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::{ChainProvider, ProfileProvider};
use rusoto_ebs::EbsClient;
use rusoto_ec2::Ec2Client;
use snafu::{ensure, ResultExt};
use std::path::PathBuf;
use std::time::Duration;

type Result<T> = std::result::Result<T, error::Error>;

#[tokio::main]
// Returning a Result from main makes it print a Debug representation of the error, but with Snafu
// we have nice Display representations of the error, so we wrap "main" (run) and print any error.
// https://github.com/shepmaster/snafu/issues/110
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args: Args = argh::from_env();
    match args.subcommand {
        SubCommand::Download(download_args) => {
            let client = build_client!(EbsClient, args.region, args.endpoint, args.profile)?;
            let downloader = SnapshotDownloader::new(client);
            ensure!(
                download_args.file.file_name().is_some(),
                error::ValidateFilenameSnafu {
                    path: download_args.file
                }
            );
            ensure!(
                download_args.force || !download_args.file.exists(),
                error::FileExistsSnafu {
                    path: download_args.file
                }
            );

            let progress_bar = build_progress_bar(download_args.no_progress, "Downloading");
            downloader
                .download_to_file(
                    &download_args.snapshot_id,
                    &download_args.file,
                    progress_bar,
                )
                .await
                .context(error::DownloadSnapshotSnafu)?;
        }

        SubCommand::Upload(upload_args) => {
            let client = build_client!(
                EbsClient,
                args.region.clone(),
                args.endpoint.clone(),
                args.profile.clone()
            )?;
            let uploader = SnapshotUploader::new(client);
            let progress_bar = build_progress_bar(upload_args.no_progress, "Uploading");
            let snapshot_id = uploader
                .upload_from_file(
                    &upload_args.file,
                    upload_args.volume_size,
                    upload_args.description.as_deref(),
                    progress_bar,
                )
                .await
                .context(error::UploadSnapshotSnafu)?;
            println!("{}", snapshot_id);
            if upload_args.wait {
                let client = build_client!(Ec2Client, args.region, args.endpoint, args.profile)?;
                let waiter = SnapshotWaiter::new(client);
                waiter
                    .wait_for_completed(&snapshot_id)
                    .await
                    .context(error::WaitSnapshotSnafu)?;
            }
        }

        SubCommand::Wait(wait_args) => {
            let client = build_client!(Ec2Client, args.region, args.endpoint, args.profile)?;
            let waiter = SnapshotWaiter::new(client);
            let wait_params = WaitParams::new(
                wait_args.desired_status,
                wait_args.successes_required,
                wait_args.max_attempts,
                wait_args.seconds_between_attempts,
            );
            waiter
                .wait(wait_args.snapshot_id, wait_params)
                .await
                .context(error::WaitSnapshotSnafu)?;
        }
    }
    Ok(())
}

/// Create a progress bar to show status of snapshot blocks, if wanted.
fn build_progress_bar(no_progress: bool, verb: &str) -> Option<ProgressBar> {
    if no_progress {
        return None;
    }
    let progress_bar = ProgressBar::new(0);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(&["  ", verb, "  [{bar:50.white/black}] {pos}/{len} ({eta})"].concat())
            .progress_chars("=> "),
    );
    Some(progress_bar)
}

#[derive(FromArgs, PartialEq, Debug)]
/// Work with snapshots through the Amazon EBS direct APIs.
struct Args {
    #[argh(subcommand)]
    subcommand: SubCommand,

    #[argh(option)]
    /// the region to use
    region: Option<String>,

    #[argh(option)]
    /// the endpoint to use
    endpoint: Option<String>,

    #[argh(option)]
    /// the profile to use
    profile: Option<String>,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCommand {
    Download(DownloadArgs),
    Upload(UploadArgs),
    Wait(WaitArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "download")]
/// Download an EBS snapshot into a local file.
struct DownloadArgs {
    #[argh(positional)]
    snapshot_id: String,

    #[argh(positional)]
    file: PathBuf,

    #[argh(switch)]
    /// overwrite an existing file
    force: bool,

    #[argh(switch)]
    /// disable the progress bar
    no_progress: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "upload")]
/// Upload a local file into an EBS snapshot.
struct UploadArgs {
    #[argh(positional)]
    file: PathBuf,

    #[argh(option)]
    /// the size of the volume
    volume_size: Option<i64>,

    #[argh(option)]
    /// the description for the snapshot
    description: Option<String>,

    #[argh(switch)]
    /// disable the progress bar
    no_progress: bool,

    #[argh(switch)]
    /// wait for snapshot to be in "completed" state
    wait: bool,
}

/// Turn a user-specified duration in seconds into a Duration object, for argh parsing.
fn seconds_from_str(input: &str) -> std::result::Result<Duration, String> {
    let seconds: u64 = input
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    Ok(Duration::from_secs(seconds))
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "wait")]
/// Wait for an EBS snapshot to be in a desired state.
struct WaitArgs {
    #[argh(positional)]
    /// the ID of the snapshot to wait for
    snapshot_id: String,

    #[argh(option)]
    /// the desired status to wait for, like "completed"
    desired_status: Option<String>,

    #[argh(option)]
    /// the number of successful checks in a row to consider the wait completed
    successes_required: Option<u8>,

    #[argh(option)]
    /// check at most this many times before giving up
    max_attempts: Option<u8>,

    #[argh(option, from_str_fn(seconds_from_str))]
    /// wait this many seconds between queries to check snapshot status
    seconds_between_attempts: Option<Duration>,
}

/// Potential errors during `coldsnap` execution.
mod error {
    use snafu::Snafu;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub(super) enum Error {
        #[snafu(display("Failed to create HTTP client: {}", source))]
        CreateHttpClient {
            source: rusoto_core::request::TlsError,
        },

        #[snafu(display("Failed to create profile provider: {}", source))]
        CreateProfileProvider {
            source: rusoto_credential::CredentialsError,
        },

        #[snafu(display("Failed to download snapshot: {}", source))]
        DownloadSnapshot { source: coldsnap::DownloadError },

        #[snafu(display("Refusing to overwrite existing file '{}' without --force", path.display()))]
        FileExists { path: std::path::PathBuf },

        #[snafu(display("Failed to parse region '{}': {}", region, source))]
        ParseRegion {
            region: String,
            source: rusoto_signature::region::ParseRegionError,
        },

        #[snafu(display("Failed to upload snapshot: {}", source))]
        UploadSnapshot { source: coldsnap::UploadError },

        #[snafu(display("Failed to validate filename '{}'", path.display()))]
        ValidateFilename { path: std::path::PathBuf },

        #[snafu(display("Failed to wait for snapshot: {}", source))]
        WaitSnapshot { source: coldsnap::WaitError },
    }
}
