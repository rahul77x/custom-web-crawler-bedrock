"""
Curation Lambda
----------------
Triggered by SQS (a `curation-queue` fed by an S3 Event Notification on
the staging bucket, filtered to suffix ".metadata.json" -- deliberately
NOT ".txt", since Processing writes the text file first and the
metadata sidecar second; triggering on the second file guarantees both
already exist by the time this runs).

This is the approval gate: the only thing that actually writes into the
kb-source bucket, which is what Bedrock's knowledge base data source
reads from. Nothing reaches the KB without passing through here.

Two gates, checked in order:
  1. ingestionEnabled == "true" on the registry item -- the manual,
     per-URL suppression lever a human can set in the DynamoDB console.
     Still-crawled, still-processed content that's deliberately kept
     out of the KB lands here as status SUPPRESSED.
  2. CURATION_EXCLUDE_PATTERNS -- an optional regex denylist against
     the source URL, for business/content rules ("never put the
     careers section in the KB") distinct from per-URL manual control.
     A match lands as status REJECTED.
Anything passing both is copied as-is (both files, same key) into
kb-source and marked APPROVED.

Required environment variables:
  STAGING_BUCKET       - where Processing writes clean text + metadata
  KB_SOURCE_BUCKET      - the bucket Bedrock's data source points at
  REGISTRY_TABLE_NAME  - the content registry
Optional:
  BLOCKLIST_TABLE_NAME      - optional DynamoDB table (PK: url String) holding exact
                              full URLs to block from curation.
  CURATION_ALLOW_PATTERNS   - comma-separated regex patterns defining allowed
                              banking sections, e.g. "/personal-banking,
                              /loans,/cards,/deposits,/rates,/faqs". If set,
                              URLs not matching are REJECTED.
  CURATION_EXCLUDE_PATTERNS - comma-separated regex patterns checked
                              against the source URL, e.g. "/careers,
                              /press-releases"

Configure the SQS trigger in the console with:
  - Batch size: 1-5
  - Report batch item failures: ON, function response type
    "ReportBatchItemFailures"

IMPORTANT: once this Lambda is live, add curation-queue's URL to the
ingestion-trigger Lambda's MONITORED_QUEUE_URLS (alongside crawl-queue
and processing-queue, if that second one was added already -- confirm
it was, since it's easy to have only done one of the two). Without
this, the ingestion-trigger could sync while curation is still running.
"""

import json
import logging
import os
import re
import time
import urllib.parse

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

STAGING_BUCKET = os.environ["STAGING_BUCKET"]
KB_SOURCE_BUCKET = os.environ["KB_SOURCE_BUCKET"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
BLOCKLIST_TABLE_NAME = os.environ.get("BLOCKLIST_TABLE_NAME")

registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
blocklist_table = dynamodb.Table(BLOCKLIST_TABLE_NAME) if BLOCKLIST_TABLE_NAME else None

CURATION_ALLOW_PATTERNS = [
    re.compile(p.strip())
    for p in os.environ.get("CURATION_ALLOW_PATTERNS", "").split(",")
    if p.strip()
]

EXCLUDE_PATTERNS = [
    re.compile(p.strip())
    for p in os.environ.get("CURATION_EXCLUDE_PATTERNS", "").split(",")
    if p.strip()
]

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}


def normalize_url(url):
    parts = urllib.parse.urlsplit(url.strip())
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = sorted(
        (k, v) for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    )
    query = urllib.parse.urlencode(query_pairs)
    return urllib.parse.urlunsplit(("https", netloc, path, query, ""))


def is_in_blocklist_table(url):
    """
    Checks if the URL is listed in the optional BLOCKLIST_TABLE_NAME DynamoDB table.
    Checks both the raw URL and normalized form to guard against minor formatting differences.
    """
    if not blocklist_table:
        return False
    candidates = [url]
    norm = normalize_url(url)
    if norm != url:
        candidates.append(norm)
    for u in candidates:
        try:
            resp = blocklist_table.get_item(Key={"url": u})
            if "Item" in resp:
                return True
        except Exception as e:
            logger.warning("Failed to check blocklist table for %s: %s", u, e, exc_info=True)
    return False


def is_allowlisted(url):
    if not CURATION_ALLOW_PATTERNS:
        return True
    return any(rx.search(url) for rx in CURATION_ALLOW_PATTERNS)


def is_denied(url):
    return any(rx.search(url) for rx in EXCLUDE_PATTERNS)


def get_registry_item(url_hash):
    resp = registry_table.get_item(Key={"urlHash": url_hash})
    return resp.get("Item")


def mark_approved(url_hash, curated_key):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #status = :approved, curatedS3Key = :key, approvedAt = :now"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":approved": "APPROVED",
            ":key": curated_key,
            ":now": int(time.time()),
        },
    )


def mark_skipped(url_hash, status, reason):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression="SET #status = :status, curationNotes = :reason",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": status, ":reason": reason},
    )


def process_metadata_object(bucket, metadata_key):
    if not metadata_key.endswith(".metadata.json"):
        # Defensive -- shouldn't happen if the S3 event notification
        # suffix filter is configured correctly, but don't silently
        # mis-handle it if it does.
        logger.warning("Skipping non-metadata key: s3://%s/%s", bucket, metadata_key)
        return

    content_key = metadata_key[: -len(".metadata.json")]

    metadata_obj = s3.get_object(Bucket=bucket, Key=metadata_key)
    metadata_body = json.loads(metadata_obj["Body"].read())
    attrs = metadata_body.get("metadataAttributes", {})
    url_hash = attrs.get("urlHash")
    source_url = attrs.get("sourceUrl")

    if not url_hash or not source_url:
        raise RuntimeError(f"Missing urlHash/sourceUrl in s3://{bucket}/{metadata_key}")

    item = get_registry_item(url_hash)
    status = item.get("status") if item else None

    # If the item has not reached PROCESSED yet (e.g., in-flight processing or DynamoDB replication lag),
    # raise so SQS retries via visibility timeout instead of permanently dropping the message.
    if status in (None, "DISCOVERED", "CRAWLED"):
        raise RuntimeError(
            f"Item for {source_url} (hash {url_hash}) is in status '{status}'; not ready for curation yet. Retrying."
        )

    # Terminal states where curation should not promote
    if status in ("QUARANTINED", "SUPPRESSED", "REJECTED", "DELETED"):
        logger.info("Skipping %s (hash=%s): registry status is terminal (%s)", source_url, url_hash, status)
        return

    # Only PROCESSED (and idempotent APPROVED re-runs) proceed to gating and promotion
    if status not in ("PROCESSED", "APPROVED"):
        logger.warning("Skipping %s (hash=%s): unexpected registry status %s", source_url, url_hash, status)
        return

    if item.get("ingestionEnabled", "true") != "true":
        mark_skipped(url_hash, "SUPPRESSED", "ingestionEnabled is false")
        logger.info("Suppressed (ingestionEnabled=false): url=%s urlHash=%s", source_url, url_hash)
        return

    if is_in_blocklist_table(source_url):
        mark_skipped(url_hash, "REJECTED", "found in DynamoDB url-blocklist table")
        logger.info("Rejected (DynamoDB blocklist table): url=%s urlHash=%s", source_url, url_hash)
        return

    if not is_allowlisted(source_url):
        mark_skipped(url_hash, "REJECTED", "does not match CURATION_ALLOW_PATTERNS")
        logger.info("Rejected (not in CURATION_ALLOW_PATTERNS): url=%s urlHash=%s", source_url, url_hash)
        return

    if is_denied(source_url):
        mark_skipped(url_hash, "REJECTED", "matched CURATION_EXCLUDE_PATTERNS")
        logger.info("Rejected (CURATION_EXCLUDE_PATTERNS match): url=%s urlHash=%s", source_url, url_hash)
        return

    # Copy both files as-is -- same key, different bucket. CopyObject
    # needs s3:GetObject on the staging bucket and s3:PutObject on
    # kb-source in this Lambda's role.
    s3.copy_object(
        Bucket=KB_SOURCE_BUCKET, Key=content_key,
        CopySource={"Bucket": bucket, "Key": content_key},
    )
    s3.copy_object(
        Bucket=KB_SOURCE_BUCKET, Key=metadata_key,
        CopySource={"Bucket": bucket, "Key": metadata_key},
    )

    mark_approved(url_hash, content_key)
    logger.info("Approved: url=%s urlHash=%s -> s3://%s/%s", source_url, url_hash, KB_SOURCE_BUCKET, content_key)


def handler(event, context):
    batch_item_failures = []

    for record in event["Records"]:
        message_id = record["messageId"]
        target_key = "unknown"
        try:
            # SQS message body here is S3's own event notification JSON.
            s3_event = json.loads(record["body"])
            for s3_record in s3_event.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                target_key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])
                process_metadata_object(bucket, target_key)
        except Exception as e:
            logger.error(
                "Failed processing SQS messageId=%s key=%s: %s",
                message_id, target_key, e, exc_info=True
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
