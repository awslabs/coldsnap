// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Wait for Amazon EBS snapshots to be in the desired state.
*/

use aws_sdk_ec2::types::SnapshotState;
use aws_sdk_ec2::Client as Ec2Client;
use snafu::{ensure, ResultExt, Snafu};
use std::thread::sleep;
use std::time::Duration;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct WaitParams {
    pub state: String,
    pub successes_required: u8,
    pub max_attempts: u8,
    pub duration_between_attempts: Duration,
}

impl Default for WaitParams {
    fn default() -> Self {
        Self {
            state: "completed".to_string(),
            successes_required: 3,
            max_attempts: 90,
            duration_between_attempts: Duration::from_secs(2),
        }
    }
}

impl WaitParams {
    pub fn new(
        desired_status: Option<String>,
        successes_required: Option<u8>,
        max_attempts: Option<u8>,
        duration_between_attempts: Option<Duration>,
    ) -> Self {
        let mut wait_params = Self::default();

        if let Some(desired_status) = desired_status {
            wait_params.state = desired_status;
        }
        if let Some(successes_required) = successes_required {
            wait_params.successes_required = successes_required;
        }
        if let Some(max_attempts) = max_attempts {
            wait_params.max_attempts = max_attempts;
        }
        if let Some(duration_between_attempts) = duration_between_attempts {
            wait_params.duration_between_attempts = duration_between_attempts;
        }

        wait_params
    }
}

/// Allows you to wait for snapshots to come to a desired state in the region associated with the
/// given Ec2Client.
pub struct SnapshotWaiter {
    ec2_client: Ec2Client,
}

impl SnapshotWaiter {
    pub fn new(ec2_client: Ec2Client) -> Self {
        Self { ec2_client }
    }

    /// Waits for the given snapshot ID to be completed.
    pub async fn wait_for_completed<S>(&self, snapshot_id: S) -> Result<()>
    where
        S: AsRef<str>,
    {
        self.wait(snapshot_id, Default::default()).await
    }

    /// Waits for the given snapshot ID to move to the given state, with configurable number of
    /// attempts and number of successful checks in a row.
    pub async fn wait<S>(&self, snapshot_id: S, wait_params: WaitParams) -> Result<()>
    where
        S: AsRef<str>,
    {
        let WaitParams {
            state,
            successes_required,
            max_attempts,
            duration_between_attempts,
        } = wait_params;
        let mut successes = 0;
        let mut attempts = 0;

        loop {
            attempts += 1;
            // Stop if we're over max, unless we're on a success streak, then give it some wiggle room.
            ensure!(
                (attempts - successes) <= max_attempts,
                error::MaxAttemptsSnafu { max_attempts }
            );

            let describe_response = self
                .ec2_client
                .describe_snapshots()
                .set_snapshot_ids(Some(vec![snapshot_id.as_ref().to_string()]))
                .send()
                .await
                .context(error::DescribeSnapshotsSnafu)?;

            // The response contains an Option<Vec<Snapshot>>, so we have to check that we got a
            // list at all, and then that the list contains the ID in question.
            if let Some(snapshots) = describe_response.snapshots {
                let mut saw_it = false;
                for snapshot in snapshots {
                    if let Some(ref found_id) = snapshot.snapshot_id {
                        if let Some(found_state) = snapshot.state {
                            if snapshot_id.as_ref() == found_id && state == found_state.as_str() {
                                // Success; check if we have enough to declare victory.
                                saw_it = true;
                                successes += 1;
                                if successes >= successes_required {
                                    return Ok(());
                                }
                                break;
                            }
                            // If the state was error, we know we'll never hit their desired state.
                            // (Unless they desired "error", which will be caught above.)
                            ensure!(found_state != SnapshotState::Error, error::StateSnafu);
                        }
                    }
                }
                if !saw_it {
                    // Did not find snapshot in list; reset success count and try again (if we have spare attempts)
                    successes = 0;
                }
            } else {
                // Did not receive list; reset success count and try again (if we have spare attempts)
                successes = 0;
            };
            sleep(duration_between_attempts);
        }
    }
}

/// Potential errors while waiting for the snapshot.
mod error {
    use aws_sdk_ec2::operation::describe_snapshots::DescribeSnapshotsError;
    use snafu::Snafu;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub(super) enum Error {
        #[snafu(display("Failed to describe snapshots: {}", source))]
        DescribeSnapshots {
            // Clippy gets upset if this isn't a box
            // The size difference between this and the other enums is too much
            #[snafu(source(from(aws_sdk_ec2::error::SdkError<DescribeSnapshotsError>, Box::new)))]
            source: Box<aws_sdk_ec2::error::SdkError<DescribeSnapshotsError>>,
        },

        #[snafu(display("Snapshot went to 'error' state"))]
        State,

        #[snafu(display("Failed to reach desired state within {} attempts", max_attempts))]
        MaxAttempts { max_attempts: u8 },
    }
}
