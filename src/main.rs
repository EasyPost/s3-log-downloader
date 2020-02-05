use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::Arc;
use std::io::{BufWriter, Write};
use std::fs::File;
use std::path::PathBuf;

use rusoto_core::Region;
use rusoto_s3::S3;

use futures::stream::Stream;
use futures::Future;
use tokio;
use lazy_static::lazy_static;
use regex::Regex;
use clap::Arg;

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
    inner: Mutex<HashMap<String, BufWriter<File>>>
}


impl OutputManager {
    fn new<P: Into<PathBuf>>(output_dir: P) -> Self {
        OutputManager {
            output_dir: output_dir.into(),
            inner: Mutex::new(HashMap::new())
        }
    }

    fn write_response_for_key(&self, key: String, body: Vec<u8>) {
        lazy_static! {
            static ref RE: Regex = Regex::new("(.*/)?(\\d{4}-\\d{2}-\\d{2})-\\d{2}-\\d{2}-\\d{2}-[^-]+$").unwrap();
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
        }
    }
}

fn fetch_object(client: &rusoto_s3::S3Client, bucket: String, output_manager: Arc<OutputManager>, key: String) -> impl Future<Item = (), Error = ()> {
    let mut req = rusoto_s3::GetObjectRequest::default();
    req.key = key.clone();
    req.bucket = bucket;
    let error_key = key.clone();
    client
        .get_object(req)
        .map_err(move |e| 
            eprintln!("got error {:?} while fetching metadata for key {}", e, error_key)
        )
        .and_then(|resp| {
            tokio::io::read_to_end(resp.body.unwrap().into_async_read(), Vec::new()).map(|(_, body)| body).map_err(|e| eprintln!("error reading body: {:?}", e))
        })
        .map(move |body| {
            output_manager.write_response_for_key(key, body);
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
        .map_err(|e| eprintln!("error listing: {:?}", e))
}

fn async_main(bucket: String, prefix: String, output_dir: String) -> impl Future<Item = (), Error = ()> {
    let client = Arc::new(rusoto_s3::S3Client::new(Region::UsWest2));
    let key_client = Arc::clone(&client);
    let bucket_client = bucket.clone();
    let output_manager = Arc::new(OutputManager::new(output_dir));

    let keys = futures::stream::unfold(ContinuationToken::Initial, move |continuation_token| {
        if let ContinuationToken::Finished = continuation_token {
            None
        } else {
            Some(
                fetch_key_chunk(&client, bucket.clone(), prefix.clone(), continuation_token.into_option())
                    .map(|(c, ct)| {
                        (c, ContinuationToken::from_option(ct))
                    })
            )
        }
    });
    keys.map(move |chunk| {
        let mut count = 0;
        for key in chunk {
            tokio::spawn(fetch_object(&key_client, bucket_client.clone(), Arc::clone(&output_manager), key));
            count += 1;
        }
        count
    })
    .collect()
    .map(|counts| println!("fetched {} keys", counts.iter().sum::<u64>()))
}

fn main() {
    let matches = clap::App::new(env!("CARGO_PKG_NAME"))
                            .version(env!("CARGO_PKG_VERSION"))
                            .author("James Brown <jbrown@easypost.com>")
                            .about(env!("CARGO_PKG_DESCRIPTION"))
                            .arg(Arg::with_name("bucket")
                                     .short("b")
                                     .long("bucket")
                                     .takes_value(true)
                                     .required(true)
                                     .help("Bucket name to read from"))
                            .arg(Arg::with_name("prefix")
                                     .short("p")
                                     .long("prefix")
                                     .takes_value(true)
                                     .required(true)
                                     .help("Prefix to read"))
                            .arg(Arg::with_name("output_dir")
                                     .short("o")
                                     .long("output-dir")
                                     .takes_value(true)
                                     .required(true)
                                     .help("Output directory"))
                            .get_matches();

    let bucket = matches.value_of("bucket").unwrap().to_owned();
    let prefix = matches.value_of("prefix").unwrap().to_owned();
    let output_dir = matches.value_of("output_dir").unwrap().to_owned();
    tokio::run(async_main(bucket, prefix, output_dir));
}
