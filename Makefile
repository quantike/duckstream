# Every target here is a task, not a file. Some (clean, configure) share a name
# with a directory in the tree, so .PHONY is required, not cosmetic.
.PHONY: configure debug release clean

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckstream

# duckdb-rs relies on the unstable C API, so this must be 1.
# NOTE: this pins the produced binary to exactly TARGET_DUCKDB_VERSION;
# there is no forward compatibility across DuckDB releases.
USE_UNSTABLE_C_API=1

# Target DuckDB version. The duckdb-rs crate pin in Cargo.toml (~1.10505.0)
# encodes this (1.1MMPP.R -> v1.MINOR.PATCH) and is the real build target.
# When bumping, keep these in sync by hand:
#   - Cargo.toml           duckdb pin              (~1.1MMPP.0)
#   - this file            TARGET_DUCKDB_VERSION   (v1.MINOR.PATCH)
#   - .github/workflows/   MainDistributionPipeline.yml duckdb_version
#   - .github/workflows/   integration.yml DUCKDB_VERSION (CLI for tests)
#   - extension-ci-tools   submodule branch (vX.Y-codename; only on minor bumps)
TARGET_DUCKDB_VERSION=v1.5.5

# Include makefiles from DuckDB's extension-ci-tools (Rust C-API extension flow)
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

clean: clean_build clean_rust
