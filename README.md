# Xmip repository template

This repository is the starter snapshot for a Rust-based Xmip component repository. It is not an Xmip runtime capability.

A repository generated from this template has independent history. Later template changes do not automatically rewrite generated repositories.

## Before implementation

Follow [TEMPLATE_SETUP.md](TEMPLATE_SETUP.md). The new repository must be classified and declared in the authoritative [Xmip architecture manifest](https://github.com/IlleNilsson/Xmip/blob/main/xmip-architecture.json) before its responsibility or dependencies are treated as accepted architecture.

## Shared governance

Repository-specific licensing remains explicit in [LICENSE](LICENSE). Contribution, security, support, issue and pull-request defaults are inherited from [IlleNilsson/.github](https://github.com/IlleNilsson/.github) when they are not overridden locally.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at `IlleNilsson/.github@v1`. It does not run on pushes, pull requests or a schedule.

The ordered stages are formatting, semantic analysis, linting, compilation and linking, and test execution. Packaging and publishing are not configured.
