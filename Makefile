.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckstream

# duckdb-rs relies on the unstable C API, so this must be 1.
# NOTE: this pins the produced binary to exactly TARGET_DUCKDB_VERSION;
# there is no forward compatibility across DuckDB releases.
USE_UNSTABLE_C_API=1

# Target DuckDB version. Must match the DuckDB version encoded by the duckdb
# crate (see Cargo.toml) and the extension-ci-tools submodule branch.
TARGET_DUCKDB_VERSION=v1.5.4

all: configure debug

# Include makefiles from DuckDB's extension-ci-tools (Rust C-API extension flow)
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
