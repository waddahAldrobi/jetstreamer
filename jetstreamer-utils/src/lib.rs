#![deny(missing_docs)]
//! Shared utilities for Jetstreamer components.
//!
//! Currently this crate provides helpers for spawning local ClickHouse
//! instances that were previously part of the deprecated Geyser plugin crate.

/// ClickHouse process management helpers.
pub mod clickhouse;

/// JSONL export utilities.
pub mod jsonl_export;

/// S3 export utilities.
pub mod s3_export;

/// Re-exported ClickHouse utility types and functions.
pub use clickhouse::{
    ClickhouseError, ClickhouseStartResult, start, start_client, stop, stop_sync,
};

/// Re-exported JSONL export functions.
pub use jsonl_export::{get_export_format, set_export_format, write_to_jsonl};

/// Re-exported S3 export functions.
pub use s3_export::{get_bucket, set_bucket, write_to_s3};
