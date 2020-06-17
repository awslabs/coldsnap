// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
/*!
`coldsnap` is a command-line interface that uses the Amazon EBS direct APIs to upload snapshots.
*/

use argh::FromArgs;
use coldsnap::SnapshotUploader;
use indicatif::{ProgressBar, ProgressStyle};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::{ChainProvider, ProfileProvider};
use rusoto_ebs::EbsClient;
use snafu::ResultExt;
use std::path::PathBuf;

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
    let client = build_client(args.region, args.endpoint, args.profile)?;
    match args.subcommand {
        SubCommand::Upload(args) => {
            let uploader = SnapshotUploader::new(client);
            let progress_bar = build_progress_bar(args.no_progress, "Uploading");
            let snapshot_id = uploader
                .upload_from_file(
                    &args.filename,
                    args.volume_size,
                    args.description.as_deref(),
                    progress_bar,
                )
                .await
                .context(error::UploadSnapshot)?;
            println!("{}", snapshot_id);
        }
    }
    Ok(())
}

/// Create an EBS client using the default region, endpoint, and credentials.
/// The behavior can be overriden by command line options. We don't expose the
/// full range of configuration options but this should cover most scenarios.
fn build_client(
    region: Option<String>,
    endpoint: Option<String>,
    profile: Option<String>,
) -> Result<EbsClient> {
    let http_client = HttpClient::new().context(error::CreateHttpClient)?;
    let profile_provider = match profile {
        Some(profile) => {
            let mut p = ProfileProvider::new().context(error::CreateProfileProvider)?;
            p.set_profile(profile);
            Some(p)
        }
        None => None,
    };

    let profile_region = profile_provider
        .as_ref()
        .and_then(|p| p.region_from_profile().ok().flatten());

    let region = parse_region(region)?.or(parse_region(profile_region)?);

    let region = match (region, endpoint) {
        (Some(region), Some(endpoint)) => Region::Custom {
            name: region.name().to_string(),
            endpoint,
        },
        (Some(region), None) => region,
        (None, Some(endpoint)) => Region::Custom {
            name: Region::default().name().to_string(),
            endpoint,
        },
        (None, None) => Region::default(),
    };

    match profile_provider {
        Some(provider) => Ok(EbsClient::new_with(http_client, provider, region)),
        None => Ok(EbsClient::new_with(
            http_client,
            ChainProvider::new(),
            region,
        )),
    }
}

/// Parse an optional string into a known region.
fn parse_region(region: Option<String>) -> Result<Option<Region>> {
    region
        .map(|r| r.parse().context(error::ParseRegion { region: r }))
        .transpose()
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
    Upload(UploadArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "upload")]
/// Upload a local file into an EBS snapshot.
struct UploadArgs {
    #[argh(positional)]
    filename: PathBuf,

    #[argh(option)]
    /// the size of the volume
    volume_size: Option<i64>,

    #[argh(option)]
    /// the description for the snapshot
    description: Option<String>,

    #[argh(switch)]
    /// disable the progress bar
    no_progress: bool,
}

/// Potential errors during `coldsnap` execution.
mod error {
    use snafu::Snafu;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub(super)")]
    pub(super) enum Error {
        #[snafu(display("Failed to create HTTP client: {}", source))]
        CreateHttpClient {
            source: rusoto_core::request::TlsError,
        },

        #[snafu(display("Failed to create profile provider: {}", source))]
        CreateProfileProvider {
            source: rusoto_credential::CredentialsError,
        },

        #[snafu(display("Failed to parse region '{}': {}", region, source))]
        ParseRegion {
            region: String,
            source: rusoto_signature::region::ParseRegionError,
        },

        #[snafu(display("Failed to upload snapshot: {}", source))]
        UploadSnapshot { source: coldsnap::UploadError },
    }
}
