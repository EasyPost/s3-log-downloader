use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use rusoto_core::Region;
use rusoto_s3::S3;

use clap::Arg;
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, warn};
use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;

enum ContinuationToken {
    Initial,
    Finished,
    Some(String),
}

impl ContinuationToken {
    fn into_option(self) -> Option<String> {
        match self {
            ContinuationToken::Initial => None,
            ContinuationToken::Some(s) => Some(s),
            ContinuationToken::Finished => unreachable!(),
        }
    }

    fn from_option(s: Option<String>) -> Self {
        if let Some(s) = s {
            ContinuationToken::Some(s)
        } else {
            ContinuationToken::Finished
        }
    }
}

struct OutputManager {
    output_dir: PathBuf,
    inner: Mutex<HashMap<String, BufWriter<File>>>,
}

impl OutputManager {
    fn new<P: Into<PathBuf>>(output_dir: P) -> Self {
        OutputManager {
            output_dir: output_dir.into(),
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn write_response_for_key(&self, key: String, body: Vec<u8>) {
        lazy_static! {
            static ref RE: Regex = Regex::new(
                r"(?x)
                    (.*/)?
                    (?:[A-Z0-9]{14}\.)?
                    (
                        \d{4}
                        -
                        \d{2}
                        -
                        \d{2}
                    )
                    [^/]+
                    $
                "
            )
            .unwrap();
        }
        if let Some(capture) = RE.captures(&key) {
            let date_key = capture.get(2).unwrap().as_str().to_owned();
            let mut map = self.inner.lock().unwrap();
            let f = map.entry(date_key.clone()).or_insert_with(|| {
                let mut filename = self.output_dir.clone();
                filename.push(date_key);
                println!("opening {:?} for writing", filename);
                let f = File::create(filename).unwrap();
                BufWriter::new(f)
            });
            f.write_all(&body).unwrap();
        } else {
            warn!("key {} does not match output regex", key);
        }
    }
}

async fn retry_fetch(
    client: &rusoto_s3::S3Client,
    bucket: String,
    key: String,
    attempts: usize,
) -> Result<rusoto_s3::GetObjectOutput, rusoto_core::RusotoError<rusoto_s3::GetObjectError>> {
    debug!("starting fetch for {:?} {:?}", bucket, key);
    let mut last_err = None;
    for _ in 0..attempts {
        let req = rusoto_s3::GetObjectRequest {
            key: key.clone(),
            bucket: bucket.clone(),
            ..Default::default()
        };
        match client.get_object(req).await {
            Ok(val) => {
                debug!("fetch successful");
                return Ok(val);
            }
            Err(e) => {
                warn!("got error {:?}, retrying in a few seconds", e);
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
    error!("ran out of retries on {:?}: {:?}", key, last_err);
    Err(last_err.unwrap())
}

async fn fetch_object(
    client: Arc<rusoto_s3::S3Client>,
    semaphore: Arc<Semaphore>,
    bucket: String,
    output_manager: Arc<OutputManager>,
    key: String,
) -> anyhow::Result<()> {
    let permit = semaphore.acquire().await?;
    let is_gzip = key.ends_with(".gz");
    let resp = retry_fetch(&client, bucket.clone(), key.clone(), 3).await?;
    let mut body = Vec::new();
    if let Some(rbody) = resp.body {
        rbody.into_async_read().read_to_end(&mut body).await?;
    }
    let unzipped_body = if is_gzip {
        debug!("unzipping {} bytes", body.len());
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
            use std::io::Read;

            let mut unzipped = Vec::new();
            let mut gz = flate2::read::GzDecoder::new(body.as_slice());
            gz.read_to_end(&mut unzipped)?;
            debug!("unzipped {} bytes", unzipped.len());
            Ok(unzipped)
        })
        .await??
    } else {
        body
    };
    drop(permit);
    output_manager.write_response_for_key(key, unzipped_body);
    Ok(())
}

async fn fetch_key_chunk(
    client: &rusoto_s3::S3Client,
    bucket: String,
    prefix: String,
    continuation_token: Option<String>,
) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let req = rusoto_s3::ListObjectsV2Request {
        bucket,
        delimiter: Some("/".to_owned()),
        prefix: Some(prefix),
        continuation_token,
        ..Default::default()
    };
    let resp = client.list_objects_v2(req).await?;
    let next_ct = resp.next_continuation_token;
    let contents = resp
        .contents
        .map(|c| c.into_iter().filter_map(|key| key.key).collect::<Vec<_>>())
        .unwrap_or_else(Vec::new);
    Ok((contents, next_ct))
}

#[tokio::main]
async fn async_main(
    bucket: String,
    prefix: String,
    output_dir: String,
    max_concurrent_fetches: usize,
    region: Region,
) {
    let client = Arc::new(rusoto_s3::S3Client::new(region));
    let output_manager = Arc::new(OutputManager::new(output_dir));
    let latter_client = Arc::clone(&client);
    let latter_bucket = bucket.clone();

    let fetch_sem = Arc::new(Semaphore::new(max_concurrent_fetches));

    let count = futures::stream::unfold(ContinuationToken::Initial, |continuation_token| {
        let list_client = Arc::clone(&client);
        let list_bucket = bucket.clone();
        let prefix = prefix.clone();
        async move {
            if let ContinuationToken::Finished = continuation_token {
                None
            } else {
                let (c, ct) = match fetch_key_chunk(
                    &list_client,
                    list_bucket,
                    prefix,
                    continuation_token.into_option(),
                )
                .await
                {
                    Ok((c, ct)) => (c, ct),
                    Err(e) => panic!("error fetching keys: {:?}", e),
                };
                Some((c, ContinuationToken::from_option(ct)))
            }
        }
    })
    .then(move |chunk| {
        let client = Arc::clone(&latter_client);
        let fetch_sem = Arc::clone(&fetch_sem);
        let latter_bucket = latter_bucket.clone();
        let output_manager = Arc::clone(&output_manager);
        async move {
            log::info!("spawning for chunk of {:?} keys", chunk.len());
            let mut handles = vec![];
            for key in chunk {
                let key_client = Arc::clone(&client);
                handles.push(tokio::spawn(fetch_object(
                    key_client,
                    Arc::clone(&fetch_sem),
                    latter_bucket.clone(),
                    Arc::clone(&output_manager),
                    key,
                )));
            }
            let jobs = handles.len();
            log::info!("spawned {:?} jobs", jobs);
            futures::future::join_all(handles).await;
            jobs
        }
    })
    .fold(0usize, |acc, c| async move { acc.wrapping_add(c) })
    .await;
    println!("fetched {} keys", count);
}

fn cli() -> clap::Command<'static> {
    clap::Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author("James Brown <jbrown@easypost.com>")
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::new("bucket")
                .short('b')
                .long("bucket")
                .takes_value(true)
                .required(true)
                .help("Bucket name to read from"),
        )
        .arg(
            Arg::new("prefix")
                .short('p')
                .long("prefix")
                .takes_value(true)
                .required(true)
                .help("Prefix to read"),
        )
        .arg(
            Arg::new("output_dir")
                .short('o')
                .long("output-dir")
                .takes_value(true)
                .required(true)
                .help("Output directory"),
        )
        .arg(
            Arg::new("concurrent_fetches")
                .short('C')
                .long("concurrent-fetches")
                .takes_value(true)
                .default_value("500")
                .help("Maximum concurrent fetches"),
        )
        .arg(
            Arg::new("aws-region")
                .short('r')
                .long("aws-region")
                .takes_value(true)
                .default_value("us-west-2"),
        )
}

fn main() {
    let matches = cli().get_matches();

    env_logger::init();

    let bucket = matches.value_of_t_or_exit("bucket");
    let prefix = matches.value_of_t_or_exit("prefix");
    let output_dir = matches.value_of_t_or_exit("output_dir");
    let max_concurrent_fetches = matches.value_of_t_or_exit("concurrent_fetches");
    let region = matches.value_of_t_or_exit("region");
    async_main(bucket, prefix, output_dir, max_concurrent_fetches, region);
}

#[cfg(test)]
mod tests {
    use super::cli;

    #[test]
    fn cli_debug_assert() {
        cli().debug_assert()
    }
}
