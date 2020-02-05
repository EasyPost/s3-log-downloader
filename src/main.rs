use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rusoto_core::Region;
use rusoto_s3::S3;

use clap::Arg;
use futures::stream::Stream;
use futures::Future;
use lazy_static::lazy_static;
use regex::Regex;
use tokio;
use tokio_sync::semaphore::Semaphore;
use log::{debug, info, warn, error};
use env_logger;

mod sem;

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
            static ref RE: Regex =
                Regex::new("(.*/)?(\\d{4}-\\d{2}-\\d{2})-\\d{2}-\\d{2}-\\d{2}-[^-]+$").unwrap();
        }
        if let Some(capture) = RE.captures(&key) {
            let date_key = capture.get(2).unwrap().as_str().to_owned();
            let mut map = self.inner.lock().unwrap();
            let f = map.entry(date_key.clone()).or_insert_with(|| {
                let mut filename = self.output_dir.clone();
                filename.push(date_key);
                debug!("opening {:?} for writing", filename);
                let f = File::create(filename).unwrap();
                BufWriter::new(f)
            });
            f.write_all(&body).unwrap();
        }
    }
}

fn fetch_object(
    client: Arc<rusoto_s3::S3Client>,
    semaphore: Arc<Semaphore>,
    bucket: Arc<String>,
    output_manager: Arc<OutputManager>,
    key: &str,
) -> impl Future<Item = (), Error = ()> {
    let key = key.to_owned();
    let mut req = rusoto_s3::GetObjectRequest::default();
    req.key = key.clone();
    req.bucket = bucket.to_string();
    let error_key = key.clone();
    let fetch_future = client.get_object(req);
    let releaser = Arc::clone(&semaphore);
    sem::SemaphoreWaiter::new(semaphore)
        .map_err(|e| error!("error acquiring semaphore: {:?}", e))
        .and_then(|mut permit| {
            fetch_future
                .map_err(move |e| {
                    error!(
                        "got error {:?} while fetching metadata for key {}",
                        e, error_key
                    )
                })
                .and_then(|resp| {
                    tokio::io::read_to_end(resp.body.unwrap().into_async_read(), Vec::new())
                        .map(|(_, body)| body)
                        .map_err(|e| error!("error reading body: {:?}", e))
                        .then(move |res| {
                            permit.release(&releaser);
                            res
                        })
                })
                .map(move |body| {
                    output_manager.write_response_for_key(key, body);
                })
        })
}


fn fetch_key_chunk(
    client: &rusoto_s3::S3Client,
    bucket: String,
    prefix: String,
    continuation_token: Option<String>,
) -> impl Future<Item = (Vec<String>, Option<String>), Error = ()> {
    let mut req = rusoto_s3::ListObjectsV2Request::default();
    req.bucket = bucket;
    req.delimiter = Some("/".to_owned());
    req.prefix = Some(prefix);
    req.continuation_token = continuation_token;
    client
        .list_objects_v2(req)
        .map(|resp| {
            let next_ct = resp.next_continuation_token;
            let contents = resp
                .contents
                .map(|c| c.into_iter().filter_map(|key| key.key).collect::<Vec<_>>())
                .unwrap_or_else(Vec::new);
            (contents, next_ct)
        })
        .map_err(|e| error!("error listing: {:?}", e))
}

struct CappedRetry {
    retries_remaining: usize
}

impl CappedRetry {
    fn new(retries: usize) -> Self {
        CappedRetry { retries_remaining: retries }
    }
}

impl<InError> futures_retry::ErrorHandler<InError> for CappedRetry {
    type OutError = InError;

    fn handle(&mut self, e: InError) -> futures_retry::RetryPolicy<Self::OutError> {
        if self.retries_remaining == 0 {
            warn!("retries exhausted!");
            futures_retry::RetryPolicy::ForwardError(e)
        } else {
            debug!("retrying");
            self.retries_remaining -= 1;
            futures_retry::RetryPolicy::WaitRetry(Duration::from_millis(50))
        }
    }
}

fn async_main(
    bucket: String,
    prefix: String,
    output_dir: String,
    max_concurrent_fetches: usize,
) -> impl Future<Item = (), Error = ()> {
    let client = Arc::new(rusoto_s3::S3Client::new(Region::UsWest2));
    let key_client = Arc::clone(&client);
    let bucket_client = Arc::new(bucket.clone());
    let output_manager = Arc::new(OutputManager::new(output_dir));

    let fetch_sem = Arc::new(Semaphore::new(max_concurrent_fetches));

    let keys = futures::stream::unfold(ContinuationToken::Initial, move |continuation_token| {
        if let ContinuationToken::Finished = continuation_token {
            None
        } else {
            Some(
                fetch_key_chunk(
                    &client,
                    bucket.clone(),
                    prefix.clone(),
                    continuation_token.into_option(),
                )
                .map(|(c, ct)| (c, ContinuationToken::from_option(ct))),
            )
        }
    });
    keys.map(move |chunk| {
        let mut count = 0;
        for key in chunk.into_iter() {
            let our_key_client = Arc::clone(&key_client);
            let our_fetch_sem = Arc::clone(&fetch_sem);
            let our_output_manager = Arc::clone(&output_manager);
            let our_bucket_client = Arc::clone(&bucket_client);
            tokio::spawn(futures_retry::FutureRetry::new(
                move || {
                    let key = key.clone();
                    fetch_object( Arc::clone(&our_key_client), Arc::clone(&our_fetch_sem), Arc::clone(&our_bucket_client), Arc::clone(&our_output_manager), &key)
                },
                CappedRetry::new(3)
            ));
            count += 1;
        }
        count
    })
    .collect()
    .map(|counts| info!("fetched {} keys", counts.iter().sum::<u64>()))
}

fn main() {
    let matches = clap::App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author("James Brown <jbrown@easypost.com>")
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("bucket")
                .short("b")
                .long("bucket")
                .takes_value(true)
                .required(true)
                .help("Bucket name to read from"),
        )
        .arg(
            Arg::with_name("prefix")
                .short("p")
                .long("prefix")
                .takes_value(true)
                .required(true)
                .help("Prefix to read"),
        )
        .arg(
            Arg::with_name("output_dir")
                .short("o")
                .long("output-dir")
                .takes_value(true)
                .required(true)
                .help("Output directory"),
        )
        .arg(
            Arg::with_name("concurrent_fetches")
                .short("C")
                .long("concurrent-fetches")
                .takes_value(true)
                .default_value("500")
                .help("Maximum concurrent fetches"),
        )
        .get_matches();

    let bucket = matches.value_of("bucket").unwrap().to_owned();
    let prefix = matches.value_of("prefix").unwrap().to_owned();
    let output_dir = matches.value_of("output_dir").unwrap().to_owned();
    let max_concurrent_fetches = matches
        .value_of("concurrent_fetches")
        .unwrap()
        .parse()
        .expect("--concurrent-fetches must be a usize");

    env_logger::init();
    log_panics::init();

    tokio::run(async_main(
        bucket,
        prefix,
        output_dir,
        max_concurrent_fetches,
    ));
}
