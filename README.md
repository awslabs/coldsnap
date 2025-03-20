coldsnap
--------

`coldsnap` is a command-line interface that uses the Amazon EBS direct APIs to upload and download snapshots.

It does not need to launch an EC2 instance or manage EBS volume attachments.
It can be used to simplify snapshot handling in an automated pipeline.

## Usage

### Credentials

Coldsnap uses the same credential mechanisms as the `aws cli`.
For example, if you have credentials in `~/.aws/credentials`, these will be used.
You can specify the name of the profile to be used by adding `--profile profile-name`.

You can also define environment variables, for example:

```
$ export AWS_ACCESS_KEY_ID=EXAMPLEAKIAIOSFODNN7
$ export AWS_SECRET_ACCESS_KEY=EXAMPLEKEYwJalrXUtnFEMI/K7MDENG/bPxRfiCY
$ export AWS_DEFAULT_REGION=us-west-2
```

If the name of a profile is provided, then it will be used.
If not, then the default behavior of the AWS Rust SDK credential provider will be used.
[Here] is the description of the default behavior.

[Here]: https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html

### Upload

Upload a local file into an EBS snapshot:

```
$ coldsnap upload disk.img
```

If you want to wait for the uploaded snapshot to be in "available" state, add `--wait`:

```
$ coldsnap upload --wait disk.img
```

Alternately, you can use `coldsnap wait`, which offers more flexibility in terms of wait duration and behavior.

```
$ coldsnap wait snap-1234
```

If you want to add tags to the uploaded snapshot, add `--tag Key=k,Value=v` for each desired tag:

```
$ coldsnap upload snap-1234 --tag "Key=MyKeyName,Value=MyKeyValue" --tag "Key=MyOtherKeyName,Value=MyOtherKeyValue"
```


### Download

Download an EBS snapshot into a local file:

```
$ coldsnap download snap-1234 disk.img
```

Run `coldsnap --help` to see more options.

## Installation

`coldsnap` can be installed using [`cargo`](https://rustup.rs/).

```
$ cargo install --locked coldsnap
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

