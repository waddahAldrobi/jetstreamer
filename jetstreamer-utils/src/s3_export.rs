use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client as S3Client, error::ProvideErrorMetadata, primitives::ByteStream};
use once_cell::sync::OnceCell;
use serde::Serialize;
use tokio::sync::OnceCell as TokioOnceCell;
use uuid::Uuid;

static S3_BUCKET: OnceCell<String> = OnceCell::new();
static AWS_CONFIG: TokioOnceCell<aws_config::SdkConfig> = TokioOnceCell::const_new();
static RUN_UUID: OnceCell<String> = OnceCell::new();

/// Sets the S3 bucket name for S3 export.
/// This should be called once during initialization.
pub fn set_bucket(bucket: String) {
    let _ = S3_BUCKET.set(bucket);
}

/// Gets the current S3 bucket name, if set.
pub fn get_bucket() -> Option<&'static str> {
    S3_BUCKET.get().map(|s| s.as_str())
}

/// Gets or initializes the run UUID with datetime prefix.
/// The UUID is generated once per run and shared across all plugins.
/// Format: `YYYY-MM-DDTHH:MM:SSZ-{uuid}`
fn get_run_uuid() -> &'static str {
    RUN_UUID.get_or_init(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Format as ISO 8601: YYYY-MM-DDTHH:MM:SSZ
        // Convert Unix timestamp to UTC datetime
        let datetime_utc = chrono::DateTime::<chrono::Utc>::from_timestamp(now as i64, 0)
            .unwrap_or_else(|| chrono::Utc::now());
        let datetime_str = datetime_utc.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let uuid = Uuid::new_v4();
        format!("{}-{}", datetime_str, uuid)
    })
}

/// Gets or initializes the AWS config (lazy initialization, only loads once).
/// Credential logs are suppressed by reusing the config (only loads credentials once).
/// To completely suppress credential logs, set RUST_LOG environment variable before
/// starting the application: RUST_LOG="...,aws_config::profile::credentials=off"
async fn get_aws_config() -> &'static aws_config::SdkConfig {
    AWS_CONFIG
        .get_or_init(|| async {
            // Load AWS config - by reusing this config, we avoid repeated credential loading
            // which reduces the number of credential logs
            aws_config::load_defaults(BehaviorVersion::latest()).await
        })
        .await
}

/// Writes rows to a JSONL file in S3.
/// Files are written to `s3://<bucket>/<datetime-prefixed-uuid>/<table_name>/<timestamp>.jsonl`.
/// The datetime-prefixed UUID is generated once per run and shared across all plugins.
pub async fn write_to_s3<T: Serialize>(
    table_name: &str,
    rows: Vec<T>,
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }

    let bucket = get_bucket().ok_or("S3 bucket not configured. Use --bucket <bucket_name>")?;

    // Build JSONL content in memory
    let mut jsonl_content = Vec::new();
    for event in rows {
        let json_line = serde_json::to_string(&event)?;
        jsonl_content.extend_from_slice(json_line.as_bytes());
        jsonl_content.push(b'\n');
    }

    // Generate S3 key with run UUID prefix and timestamp
    let run_uuid = get_run_uuid();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let key = format!("{}/{}/{}.jsonl", run_uuid, table_name, timestamp);

    // Get or initialize AWS config (only loads once, preventing repeated credential logs)
    let config = get_aws_config().await;
    let initial_region = config
        .region()
        .map(|r| r.as_ref().to_string())
        .unwrap_or_else(|| "us-east-1".to_string());
    let client = S3Client::new(config);

    // Upload to S3
    let body = ByteStream::from(jsonl_content.clone());
    let result = client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(body)
        .send()
        .await;

    // Handle PermanentRedirect (region mismatch) by retrying with us-east-1
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            // Check if it's a PermanentRedirect error - this means wrong region
            if let Some(code) = e.code() {
                if code == "PermanentRedirect" {
                    log::debug!(
                        "Got PermanentRedirect error - bucket region mismatch. \
                         Bucket is in us-east-1, but SDK is using {}. Retrying with us-east-1...",
                        initial_region
                    );

                    // Retry with us-east-1 (null LocationConstraint means us-east-1)
                    // Reuse the existing config to avoid reloading credentials
                    let region = aws_sdk_s3::config::Region::new("us-east-1");
                    let retry_config = config.to_builder().region(Some(region)).build();
                    let retry_client = S3Client::new(&retry_config);
                    let retry_body = ByteStream::from(jsonl_content);

                    match retry_client
                        .put_object()
                        .bucket(bucket)
                        .key(&key)
                        .body(retry_body)
                        .send()
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(retry_err) => {
                            let error_msg = format!(
                                "S3 upload retry failed: code={:?}, message={:?}, error={}",
                                retry_err.code(),
                                retry_err.message(),
                                retry_err
                            );
                            log::error!("{}", error_msg);
                            Err(error_msg.into())
                        }
                    }
                } else {
                    let error_msg = format!(
                        "S3 upload failed: code={:?}, message={:?}",
                        e.code(),
                        e.message()
                    );
                    log::error!("{}", error_msg);
                    Err(error_msg.into())
                }
            } else {
                let error_msg = format!(
                    "S3 upload failed (no error code): message={:?}, error={}",
                    e.message(),
                    e
                );
                log::error!("{}", error_msg);
                Err(error_msg.into())
            }
        }
    }
}
