"""
Ingestion-trigger Lambda
------------------------
Runs on a short EventBridge Scheduler interval (e.g. every 10-15 minutes).
Checks whether every monitored SQS queue in the crawl/process/curate
pipeline is empty; if it's been empty for two consecutive checks in a row,
and no ingestion job is already running, it starts a Bedrock Knowledge
Base ingestion job.

State (the consecutive-empty-check counter and the last-ingestion
timestamp) lives in a single-item DynamoDB table so concurrent or
overlapping invocations can't double-fire.

Required environment variables:
  MONITORED_QUEUE_URLS       - comma-separated list of SQS queue URLs
  STATE_TABLE_NAME           - DynamoDB table name (partition key: pipelineId)
  REGISTRY_TABLE_NAME        - the content registry the crawler Lambda writes to
  KB_SOURCE_BUCKET           - the S3 bucket Bedrock's data source points at
  KNOWLEDGE_BASE_ID          - Bedrock knowledge base ID
  DATA_SOURCE_ID             - Bedrock knowledge base data source ID
Optional:
  REQUIRED_EMPTY_CHECKS      - default 2
  MIN_HOURS_BETWEEN_INGESTIONS - default 6 (guards against re-firing all day)
"""

import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

sqs = boto3.client("sqs")
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock_agent = boto3.client("bedrock-agent")

QUEUE_URLS = [u.strip() for u in os.environ["MONITORED_QUEUE_URLS"].split(",")]
STATE_TABLE = os.environ["STATE_TABLE_NAME"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
KB_SOURCE_BUCKET = os.environ["KB_SOURCE_BUCKET"]
KB_ID = os.environ["KNOWLEDGE_BASE_ID"]
DATA_SOURCE_ID = os.environ["DATA_SOURCE_ID"]
REQUIRED_EMPTY_CHECKS = int(os.environ.get("REQUIRED_EMPTY_CHECKS", "2"))
MIN_HOURS_BETWEEN_INGESTIONS = float(
    os.environ.get("MIN_HOURS_BETWEEN_INGESTIONS", "6")
)

state_table = dynamodb.Table(STATE_TABLE)
registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
STATE_KEY = {"pipelineId": "kb-sync"}


def get_queue_depths():
    """
    Returns breakdown per queue and total pending message count.
    Surfaces exactly which queue is active or backed up during on-call troubleshooting.
    """
    breakdown = {}
    total = 0
    for url in QUEUE_URLS:
        queue_name = url.split("/")[-1]
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"]
            visible = int(attrs["ApproximateNumberOfMessages"])
            in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
            queue_total = visible + in_flight
            breakdown[queue_name] = f"{visible} visible, {in_flight} in-flight"
            total += queue_total
        except Exception as e:
            logger.error("Failed to query queue attributes for %s: %s", url, e, exc_info=True)
            breakdown[queue_name] = "ERROR"
    return breakdown, total


def reset_empty_checks():
    state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression="SET consecutiveEmptyChecks = :zero",
        ExpressionAttributeValues={":zero": 0},
    )


def increment_empty_checks():
    resp = state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression="ADD consecutiveEmptyChecks :one",
        ExpressionAttributeValues={":one": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(resp["Attributes"]["consecutiveEmptyChecks"])


def ingestion_job_in_progress():
    resp = bedrock_agent.list_ingestion_jobs(
        knowledgeBaseId=KB_ID,
        dataSourceId=DATA_SOURCE_ID,
        filters=[{"attribute": "STATUS", "operator": "EQ", "values": ["IN_PROGRESS"]}],
    )
    return len(resp.get("ingestionJobSummaries", [])) > 0


def get_current_generation():
    resp = state_table.get_item(Key=STATE_KEY)
    return int(resp.get("Item", {}).get("crawlGeneration", 0))


def mark_deleted(item):
    """
    Removes the item's file from the KB-source bucket (if it was ever
    approved into the KB -- assumes a curation step sets curatedS3Key)
    and flips the registry status to DELETED so it's excluded from
    future reconciliation scans.
    """
    url_hash = item["urlHash"]
    curated_key = item.get("curatedS3Key")
    if curated_key:
        metadata_key = f"{curated_key}.metadata.json"
        for key_to_del in (curated_key, metadata_key):
            try:
                s3.delete_object(Bucket=KB_SOURCE_BUCKET, Key=key_to_del)
                logger.info("Deleted stale S3 object from kb-source: %s", key_to_del)
            except ClientError as e:
                logger.error("Could not delete %s from KB-source bucket: %s", key_to_del, e, exc_info=True)

    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression="SET #status = :deleted, deletedAt = :now REMOVE curatedS3Key",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":deleted": "DELETED", ":now": int(time.time())},
    )


def reconcile_deleted_urls(current_generation):
    """
    Anything in the registry that was NOT stamped with tonight's
    lastSeenGeneration (and isn't already DELETED) wasn't found in this
    run's sitemap -- it's no longer on the site.

    Deliberately filters on lastSeenGeneration (set by Discovery on
    every URL it sees, regardless of crawlEnabled) rather than
    crawlGeneration (set by the Crawler only when a URL is actually
    fetched). A crawlEnabled="false" URL is still on the site -- using
    crawlGeneration here would wrongly mark every manually-paused URL
    as DELETED the very next time this runs.

    Uses a full table scan, which is fine for a registry in the
    thousands-to-low-tens-of-thousands of items; for a much larger
    site, add a GSI on lastSeenGeneration and query instead.
    """
    deleted_count = 0
    scan_kwargs = {
        "FilterExpression": (
            "(attribute_not_exists(lastSeenGeneration) OR lastSeenGeneration < :gen) "
            "AND #status <> :deleted"
        ),
        "ExpressionAttributeNames": {"#status": "status"},
        "ExpressionAttributeValues": {":gen": current_generation, ":deleted": "DELETED"},
    }
    while True:
        resp = registry_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            mark_deleted(item)
            deleted_count += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    logger.info("Reconciliation pass: marked %d stale item(s) as DELETED", deleted_count)
    return deleted_count


def get_pipeline_state():
    resp = state_table.get_item(Key=STATE_KEY)
    return resp.get("Item", {})


def check_recent_job_status():
    """Surfaces any failure in previous Bedrock ingestion jobs so they never fail silently."""
    try:
        resp = bedrock_agent.list_ingestion_jobs(
            knowledgeBaseId=KB_ID,
            dataSourceId=DATA_SOURCE_ID,
            maxResults=5,
        )
        summaries = resp.get("ingestionJobSummaries", [])
        if summaries:
            latest = summaries[0]
            status = latest.get("status")
            job_id = latest.get("ingestionJobId")
            if status == "FAILED":
                reasons = latest.get("failureReasons", [])
                logger.error("Previous Bedrock ingestion job %s FAILED! Reasons: %s", job_id, reasons)
            elif status == "COMPLETE":
                stats = latest.get("statistics", {})
                logger.debug("Previous Bedrock ingestion job %s COMPLETED. Statistics: %s", job_id, stats)
    except Exception as e:
        logger.warning("Could not check recent Bedrock job status: %s", e, exc_info=True)


def try_claim_ingestion_slot():
    """
    Conditional write: succeeds for at most one invocation, and only if we
    haven't started an ingestion job within the cooldown window. This is
    what stops the trigger from re-firing every 15 minutes all day once
    the queues are empty after last night's run.
    """
    now = int(time.time())
    cutoff = now - int(MIN_HOURS_BETWEEN_INGESTIONS * 3600)
    try:
        state_table.update_item(
            Key=STATE_KEY,
            UpdateExpression=(
                "SET lastIngestionAt = :now, "
                "consecutiveEmptyChecks = :zero, "
                "pipelineStatus = :ingesting"
            ),
            ConditionExpression=(
                "attribute_not_exists(lastIngestionAt) OR lastIngestionAt < :cutoff"
            ),
            ExpressionAttributeValues={
                ":now": now,
                ":zero": 0,
                ":cutoff": cutoff,
                ":ingesting": "INGESTING",
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def mark_pipeline_idle():
    state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression="SET pipelineStatus = :idle",
        ExpressionAttributeValues={":idle": "IDLE"},
    )


def handler(event, context):
    check_recent_job_status()

    state = get_pipeline_state()
    pipeline_status = state.get("pipelineStatus", "IDLE")

    # If Discovery or Queueing is currently in flight, do not trigger ingestion.
    # Queues are naturally empty before Queueing pushes messages to crawl-queue.
    if pipeline_status in ("DISCOVERING", "QUEUEING"):
        started_at = state.get("discoveryStartedAt", 0)
        # Safety: check if it's been stuck for over 3 hours
        if time.time() - started_at < 3 * 3600:
            logger.info("Pipeline is currently %s; waiting for crawl dispatch.", pipeline_status)
            return {"triggered": False, "reason": f"pipeline_{pipeline_status.lower()}"}
        logger.warning("Pipeline status %s appears stale (started %ds ago).", pipeline_status, int(time.time() - started_at))

    breakdown, depth = get_queue_depths()
    if pipeline_status == "IDLE" and depth == 0:
        logger.debug("Pipeline check: pipelineStatus=%s, totalPending=%d, breakdown=%s", pipeline_status, depth, breakdown)
    else:
        logger.info("Pipeline check: pipelineStatus=%s, totalPending=%d, breakdown=%s", pipeline_status, depth, breakdown)

    if depth > 0:
        reset_empty_checks()
        logger.info("Queues have work in flight (%d total pending) -- resetting empty checks.", depth)
        return {"triggered": False, "reason": "queues_not_empty", "depth": depth, "breakdown": breakdown}

    # If pipeline is IDLE and queues are empty, nothing is pending to ingest
    if pipeline_status == "IDLE":
        logger.debug("Pipeline is IDLE and queues are empty; nothing to ingest.")
        return {"triggered": False, "reason": "pipeline_idle"}

    empty_checks = increment_empty_checks()
    logger.info("Consecutive empty checks: %d / %d required", empty_checks, REQUIRED_EMPTY_CHECKS)

    if empty_checks < REQUIRED_EMPTY_CHECKS:
        return {
            "triggered": False,
            "reason": "waiting_for_confirmation",
            "emptyChecks": empty_checks,
        }

    if ingestion_job_in_progress():
        logger.warning("A Bedrock ingestion job is already IN_PROGRESS -- skipping this cycle.")
        return {"triggered": False, "reason": "job_already_running"}

    if not try_claim_ingestion_slot():
        logger.info("Cooldown active or slot claimed by another concurrent invocation.")
        return {"triggered": False, "reason": "already_claimed_or_too_recent"}

    # Reconcile deleted URLs against lastDiscoveredGeneration (only stamped
    # when Discovery completed its full pass). This completely prevents wiping
    # live URLs if Discovery was partial or failed.
    reconcile_gen = int(state.get("lastDiscoveredGeneration", 0))
    if reconcile_gen > 0:
        logger.info("Starting deletion reconciliation against generation %d...", reconcile_gen)
        deleted_count = reconcile_deleted_urls(reconcile_gen)
    else:
        logger.warning("No lastDiscoveredGeneration found; skipping deletion reconciliation.")
        deleted_count = 0

    resp = bedrock_agent.start_ingestion_job(
        knowledgeBaseId=KB_ID,
        dataSourceId=DATA_SOURCE_ID,
    )
    job_id = resp["ingestionJob"]["ingestionJobId"]
    logger.info("Successfully started Bedrock ingestion job: %s (deleted %d stale items)", job_id, deleted_count)

    mark_pipeline_idle()

    return {"triggered": True, "ingestionJobId": job_id, "deletedCount": deleted_count}
