This project implements a script to download S3 access logs quickly.

AWS S3 writes access logs into many, many small chunks (several per second), all of which are placed in a single S3 prefix in a different bucket than the bucket being logged. Most S3 utilities have a lot of trouble handling millions of small files.

This utility takes as arguments the bucket in which logs are stored and a prefix, and downloads all files matching the S3 log naming convention under that prefix. It then merges all segments on a per-day basis and writes out files.  No intra-day sorting is performed.

For example, if invoked as `s3-log-downloader --bucket our-logs --prefix s3-logs/otherbucket/2020-01 --output-dir output/`, you should expect to see files named `output/2020-01-01`, `output/2020-01-02`, and so on, each of which will contain that days' logs.

At this time, no retries are performed and we don't limit how many concurrenct connections are initiated in Tokio, so this can quickly overwhelm S3. Future work!

This project is made available under the terms of the ISC License, a copy of which can be found at [LICENSE.txt](LICENSE.txt)
