"""
Crawler Lambda
--------------
Triggered by SQS (the crawl-queue the discovery Lambda fills, one message
per URL). Fetches the page, hashes its content, and only writes to the
raw S3 bucket and updates the DynamoDB registry if the content actually
changed since the last crawl. Unchanged pages are a cheap no-op -- no S3
write, no downstream reprocessing, no re-ingestion cost.

Uses only boto3 + the standard library (urllib), so it pastes straight
into the console code editor with no layer needed.

Required environment variables:
  RAW_BUCKET          - S3 bucket for raw crawled HTML
  REGISTRY_TABLE_NAME - DynamoDB table (partition key: urlHash)

Configure the SQS trigger in the console with:
  - Batch size: 1-5 (each item does a network fetch; keep batches small
    so one slow page doesn't hold up the others)
  - Report batch item failures: ON, function response type
    "ReportBatchItemFailures" -- this handler relies on that so a single
    bad URL retries by itself instead of retrying the whole batch

Also set Reserved concurrency on this function under Configuration ->
Concurrency, to cap how many requests hit the target site at once (e.g.
10) -- this is what keeps the crawl polite rather than a de facto DDoS.
"""

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

RAW_BUCKET = os.environ["RAW_BUCKET"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)

USER_AGENT = "internal-kb-crawler/1.0"
FETCH_TIMEOUT_SECONDS = 15


def fetch_url(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            content_type = resp.headers.get("Content-Type", "")
            body = resp.read()
            status = resp.status
            final_url = resp.geturl()
        return status, content_type, body, final_url
    except urllib.error.HTTPError as e:
        return e.code, "", b"", url


def get_registry_item(url_hash):
    resp = registry_table.get_item(Key={"urlHash": url_hash})
    return resp.get("Item")


def touch_unchanged(url_hash, batch_id, generation):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET lastCheckedAt = :now, lastBatchId = :batch, crawlGeneration = :gen"
        ),
        ExpressionAttributeValues={
            ":now": int(time.time()),
            ":batch": batch_id,
            ":gen": generation,
        },
    )


def record_http_error(url_hash, url, status_code, batch_id, generation):
    """
    Records permanent HTTP client errors (404 Not Found, 410 Gone) in DynamoDB
    without failing the SQS batch, avoiding repeated DLQ redrives.
    """
    now = int(time.time())
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #status = :status, httpStatusCode = :code, "
            "lastCheckedAt = :now, lastBatchId = :batch, crawlGeneration = :gen"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": f"FAILED_HTTP_{status_code}",
            ":code": status_code,
            ":now": now,
            ":batch": batch_id,
            ":gen": generation,
        },
    )


def record_crawled(url_hash, url, content_hash, raw_key, batch_id, generation):
    now = int(time.time())
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #url = :url, contentHash = :content_hash, rawS3Key = :raw_key, "
            "#status = :status, lastCrawledAt = :now, lastCheckedAt = :now, "
            "lastBatchId = :batch, crawlGeneration = :gen"
        ),
        ExpressionAttributeNames={"#url": "url", "#status": "status"},
        ExpressionAttributeValues={
            ":url": url,
            ":content_hash": content_hash,
            ":raw_key": raw_key,
            ":status": "CRAWLED",
            ":now": now,
            ":batch": batch_id,
            ":gen": generation,
        },
    )


def process_message(body):
    url = body["url"]
    batch_id = body.get("batchId", "unknown")
    generation = body.get("generation", 0)
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()

    t0 = time.time()
    status, content_type, content, final_url = fetch_url(url)
    duration_ms = int((time.time() - t0) * 1000)

    if status in (404, 410):
        logger.warning(
            "Permanent HTTP error %d (%dms): url=%s urlHash=%s (recorded in registry, skipping SQS retry)",
            status, duration_ms, url, url_hash
        )
        record_http_error(url_hash, url, status, batch_id, generation)
        return

    if status != 200:
        logger.error("HTTP error %d (%dms): url=%s urlHash=%s", status, duration_ms, url, url_hash)
        raise RuntimeError(f"Unexpected status {status} for {url}")

    if final_url != url:
        logger.info("Redirect followed: %s -> %s (urlHash=%s)", url, final_url, url_hash)

    if content_type and "text/html" not in content_type.lower():
        logger.info("Skipping non-HTML content (%s): url=%s urlHash=%s", content_type, url, url_hash)
        return

    content_hash = hashlib.sha256(content).hexdigest()
    existing = get_registry_item(url_hash)

    if existing and existing.get("contentHash") == content_hash:
        touch_unchanged(url_hash, batch_id, generation)
        logger.debug("Unchanged content (%dms): url=%s urlHash=%s (skipped S3 write)", duration_ms, url, url_hash)
        return

    metadata = {"source-url": url}
    if final_url != url:
        metadata["final-url"] = final_url

    raw_key = f"raw/{time.strftime('%Y-%m-%d')}/{url_hash}.html"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=raw_key,
        Body=content,
        ContentType=content_type or "text/html",
        Metadata=metadata,
    )
    record_crawled(url_hash, url, content_hash, raw_key, batch_id, generation)
    logger.info(
        "Crawled and stored (%dms): url=%s urlHash=%s -> s3://%s/%s (bytes=%d)",
        duration_ms, url, url_hash, RAW_BUCKET, raw_key, len(content)
    )


def handler(event, context):
    batch_item_failures = []

    for record in event["Records"]:
        message_id = record["messageId"]
        failed_url = "unknown"
        try:
            body = json.loads(record["body"])
            failed_url = body.get("url", "unknown")
            process_message(body)
        except Exception as e:
            logger.error(
                "Failed processing SQS messageId=%s url=%s: %s",
                message_id, failed_url, e, exc_info=True
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    # Only the failed message IDs go back for retry/DLQ; everything else
    # in the batch is left alone since it already succeeded.
    return {"batchItemFailures": batch_item_failures}
