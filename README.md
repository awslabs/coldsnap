coldsnap
--------

`coldsnap` is a command-line interface that uses the Amazon EBS direct APIs to upload and download snapshots.

It does not need to launch an EC2 instance or manage EBS volume attachments.
It can be used to simplify snapshot handling in an automated pipeline.

## Usage

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

