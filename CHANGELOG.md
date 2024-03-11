# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2024-03-15
### Changed
- Dependency updates
- Improve performance of an atomic lock (thanks @wang384670111) [#309]

[#309]: https://github.com/awslabs/coldsnap/pull/309

## [0.6.0] - 2023-08-22
### Security Fix
- Bump openssl from 0.10.48 to 0.10.55 [#257], [#271]
- Bump h2 from 0.3.15 to 0.3.20 [#257], [#271]
- Bump AWS SDK for Rust [#257]

### Changed
- Update other dependencies

## [0.5.1] - 2023-04-11
### Security Fix
- Bump openssl from 0.10.45 to 0.10.48 [#247]

### Changed
- Bump tokio from 1.25.0 to 1.26.0 [#239]

## [0.5.0] - 2023-03-08
### Changed
- Add debug logging to help with troubleshooting [#220]
- Remove minor/patch versions from Cargo.tomls [#237]
- Update dependencies

## ~~[0.4.3] - 2023-03-02~~
### ~~Changed~~
- ~~Add debug logging to help with troubleshooting [#220]~~
- ~~Remove minor/patch versions from Cargo.tomls [#237]~~
- ~~Update dependencies~~

⚠ This release was yanked and re-released as 0.5.0 due to breaking changes.

## [0.4.2] - 2022-10-03
### Changed
- Update dependencies [#197]

## [0.4.1] - 2022-08-12
### Changed
- Prevent integer overflows during offset calculations ([#186], thanks @okudajun!)
- Update dependencies

## [0.4.0] - 2022-07-26
### Changed
- Limited nix features ([#143], thanks @rtzoeller!)
- Removed Rusoto in favor of AWS SDK Rust [#145]
- Added support for files over 2^31 bytes ([#171], thanks @grahamc and @cole-c!)
- Update dependencies [#147], [#149], [#168], [#179]

## [0.3.3] - 2022-04-26
### Changed
- Add support for uploading from a block device.  [#92]
- Upgrade SNAFU.  ([#115], thanks, @shepmaster!)
- Unpin tokio.  [#129]
- Update dependencies.  [#91], [#94], [#97], [#98], [#99], [#102], [#103], [#105], [#106], [#109], [#111], [#112], [#114], [#115], [#116], [#117], [#118], [#119], [#123], [#124], [#127], [#130], [#131], [#132], [#134], [#135]

## [0.3.2] - 2021-07-30
### Changed
- Update dependencies.  [#61], [#63], [#64], [#66], [#67], [#73], [#77], [#82], [#87], [#88]
- Update docs to recommend installing with `--locked`.  [#79]
- Add license check to CI runner.  [#74]

## [0.3.1] - 2021-05-13
### Changed
- Add backoff-retry behavior to coldsnap uploads.  [#56]
- Update dependencies.  [#48], [#50], [#51], [#54], [#55], [#58], [#60]
- Fix clippy warnings for Rust 1.52.  [#57]

## [0.3.0] - 2021-02-25
### Breaking Changes
- Updated tokio to v1, this is a breaking change when using coldsnap as a library. [#39]

### Changed
- Fix an issue with download filepaths [#40]

## [0.2.0] - 2020-11-11
### Changed
- Added Cargo.toml features to switch between rusoto native-tls and rustls. [#18]

## [0.1.0] - 2020-08-05
### Added
- Everything!

[Unreleased]: https://github.com/awslabs/coldsnap/compare/v0.6.1...develop
[0.6.1]: https://github.com/awslabs/coldsnap/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/awslabs/coldsnap/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/awslabs/coldsnap/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/awslabs/coldsnap/compare/v0.4.2...v0.5.0
[0.4.3]: https://github.com/awslabs/coldsnap/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/awslabs/coldsnap/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/awslabs/coldsnap/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/awslabs/coldsnap/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/awslabs/coldsnap/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/awslabs/coldsnap/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/awslabs/coldsnap/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/awslabs/coldsnap/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/awslabs/coldsnap/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/awslabs/coldsnap/releases/tag/v0.1.0

[#18]: https://github.com/awslabs/coldsnap/pull/18
[#39]: https://github.com/awslabs/coldsnap/pull/39
[#40]: https://github.com/awslabs/coldsnap/pull/40
[#48]: https://github.com/awslabs/coldsnap/pull/48
[#50]: https://github.com/awslabs/coldsnap/pull/50
[#51]: https://github.com/awslabs/coldsnap/pull/51
[#54]: https://github.com/awslabs/coldsnap/pull/54
[#55]: https://github.com/awslabs/coldsnap/pull/55
[#56]: https://github.com/awslabs/coldsnap/pull/56
[#57]: https://github.com/awslabs/coldsnap/pull/57
[#58]: https://github.com/awslabs/coldsnap/pull/58
[#60]: https://github.com/awslabs/coldsnap/pull/60
[#61]: https://github.com/awslabs/coldsnap/pull/61
[#63]: https://github.com/awslabs/coldsnap/pull/63
[#64]: https://github.com/awslabs/coldsnap/pull/64
[#66]: https://github.com/awslabs/coldsnap/pull/66
[#67]: https://github.com/awslabs/coldsnap/pull/67
[#73]: https://github.com/awslabs/coldsnap/pull/73
[#74]: https://github.com/awslabs/coldsnap/pull/74
[#77]: https://github.com/awslabs/coldsnap/pull/77
[#79]: https://github.com/awslabs/coldsnap/pull/79
[#82]: https://github.com/awslabs/coldsnap/pull/82
[#87]: https://github.com/awslabs/coldsnap/pull/87
[#88]: https://github.com/awslabs/coldsnap/pull/88
[#91]: https://github.com/awslabs/coldsnap/pull/91
[#92]: https://github.com/awslabs/coldsnap/pull/92
[#94]: https://github.com/awslabs/coldsnap/pull/94
[#97]: https://github.com/awslabs/coldsnap/pull/97
[#98]: https://github.com/awslabs/coldsnap/pull/98
[#99]: https://github.com/awslabs/coldsnap/pull/99
[#102]: https://github.com/awslabs/coldsnap/pull/102
[#103]: https://github.com/awslabs/coldsnap/pull/103
[#105]: https://github.com/awslabs/coldsnap/pull/105
[#106]: https://github.com/awslabs/coldsnap/pull/106
[#109]: https://github.com/awslabs/coldsnap/pull/109
[#111]: https://github.com/awslabs/coldsnap/pull/111
[#112]: https://github.com/awslabs/coldsnap/pull/112
[#114]: https://github.com/awslabs/coldsnap/pull/114
[#115]: https://github.com/awslabs/coldsnap/pull/115
[#116]: https://github.com/awslabs/coldsnap/pull/116
[#117]: https://github.com/awslabs/coldsnap/pull/117
[#118]: https://github.com/awslabs/coldsnap/pull/118
[#119]: https://github.com/awslabs/coldsnap/pull/119
[#123]: https://github.com/awslabs/coldsnap/pull/123
[#124]: https://github.com/awslabs/coldsnap/pull/124
[#127]: https://github.com/awslabs/coldsnap/pull/127
[#129]: https://github.com/awslabs/coldsnap/pull/129
[#130]: https://github.com/awslabs/coldsnap/pull/130
[#131]: https://github.com/awslabs/coldsnap/pull/131
[#132]: https://github.com/awslabs/coldsnap/pull/132
[#134]: https://github.com/awslabs/coldsnap/pull/134
[#135]: https://github.com/awslabs/coldsnap/pull/135
[#143]: https://github.com/awslabs/coldsnap/pull/143
[#145]: https://github.com/awslabs/coldsnap/pull/145
[#147]: https://github.com/awslabs/coldsnap/pull/147
[#149]: https://github.com/awslabs/coldsnap/pull/149
[#168]: https://github.com/awslabs/coldsnap/pull/168
[#171]: https://github.com/awslabs/coldsnap/pull/171
[#179]: https://github.com/awslabs/coldsnap/pull/179
[#186]: https://github.com/awslabs/coldsnap/pull/186
[#197]: https://github.com/awslabs/coldsnap/pull/197
[#220]: https://github.com/awslabs/coldsnap/pull/220
[#237]: https://github.com/awslabs/coldsnap/pull/237
[#239]: https://github.com/awslabs/coldsnap/pull/239
[#247]: https://github.com/awslabs/coldsnap/pull/247
[#257]: https://github.com/awslabs/coldsnap/pull/257
[#271]: https://github.com/awslabs/coldsnap/pull/271

