# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Deprecated

### Removed

### Fixed

- CLI backend: pass the job environment as `export` lines in the batch script
  instead of `sbatch --export=<vars>`. Slurm treats `--export=<vars>` as an implicit
  `--get-user-env`; where retrieving the login environment fails on the compute
  nodes (seen on Slurm 26.05.5), every job was requeued and held with
  "user env retrieval failed".

### Security

## 0.0.1

Released on February 23, 2023.
