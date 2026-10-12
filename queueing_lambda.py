"""
Queueing Lambda
----------------
Invoked asynchronously by the Discovery Lambda once it has finished
upserting this run's URLs into the DynamoDB registry. Queries the
registry's crawl-enabled-index GSI for every item where
crawlEnabled="true" and pushes one SQS message per URL onto
crawl-queue, in the same shape the Crawler Lambda already expects:
{"url": ..., "batchId": ..., "generation": ...}.

This is the step that actually makes crawlEnabled a real manual
control -- a URL sitting in the registry with crawlEnabled="false"
simply never gets queried by this Lambda, so it never gets re-crawled,
with zero changes needed anywhere else in the pipeline.

Required environment variables:
  REGISTRY_TABLE_NAME      - DynamoDB content-registry table
  CRAWL_ENABLED_INDEX_NAME - name of the GSI on crawlEnabled (String)
  CRAWL_QUEUE_URL           - SQS queue the Crawler Lambda listens on

Invoked with payload {"generation": <int>} (sent by Discovery Lambda).
Can also be run manually from the console Test tab with the same
payload shape -- e.g. to re-queue everything without waiting for a
fresh Discovery run, after flipping some crawlEnabled flags by hand.
"""

import json
import logging
import os
import time

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
CRAWL_ENABLED_INDEX_NAME = os.environ["CRAWL_ENABLED_INDEX_NAME"]
CRAWL_QUEUE_URL = os.environ["CRAWL_QUEUE_URL"]
STATE_TABLE_NAME = os.environ.get("STATE_TABLE_NAME")

registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME) if STATE_TABLE_NAME else None


def send_batch(chunk, batch_id, generation):
    """Sends a batch of up to 10 entries to SQS crawl-queue."""
    entries = [
        {
            "Id": str(idx),
            "MessageBody": json.dumps(
                {"url": item["url"], "batchId": batch_id, "generation": generation}
            ),
        }
        for idx, item in enumerate(chunk)
    ]
    try:
        resp = sqs.send_message_batch(QueueUrl=CRAWL_QUEUE_URL, Entries=entries)
        failed = resp.get("Failed", [])
        if failed:
            logger.error("SQS send_message_batch partial failure: %d URL(s) failed: %s", len(failed), failed)
        return len(resp.get("Successful", []))
    except Exception as e:
        logger.error("Failed to send SQS batch to %s: %s", CRAWL_QUEUE_URL, e, exc_info=True)
        raise


def stream_and_enqueue_urls(batch_id, generation):
    """
    Pages through registry items with crawlEnabled='true' via the GSI.
    Streams items directly to SQS in chunks of 10 without accumulating
    the entire table in memory, supporting large sites without memory exhaustion.
    """
    query_kwargs = {
        "IndexName": CRAWL_ENABLED_INDEX_NAME,
        "KeyConditionExpression": "crawlEnabled = :enabled",
        "ExpressionAttributeValues": {":enabled": "true"},
        "ProjectionExpression": "#url, urlHash",
        "ExpressionAttributeNames": {"#url": "url"},
    }
    candidate_count = 0
    enqueued_count = 0
    buffer = []

    while True:
        resp = registry_table.query(**query_kwargs)
        items = resp.get("Items", [])
        candidate_count += len(items)

        for item in items:
            buffer.append(item)
            if len(buffer) == 10:
                enqueued_count += send_batch(buffer, batch_id, generation)
                buffer = []

        if "LastEvaluatedKey" not in resp:
            break
        query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if buffer:
        enqueued_count += send_batch(buffer, batch_id, generation)

    return candidate_count, enqueued_count


def record_queueing_complete(generation, enqueued_count):
    """
    Transitions pipelineStatus to 'CRAWLING' (or 'IDLE' if nothing enqueued)
    and resets empty checks. This signals Ingestion-trigger that the batch is
    in flight and ready to be monitored across all queues.
    """
    if not state_table:
        return
    status = "CRAWLING" if enqueued_count > 0 else "IDLE"
    now = int(time.time())
    state_table.update_item(
        Key={"pipelineId": "kb-sync"},
        UpdateExpression=(
            "SET pipelineStatus = :status, "
            "queuedGeneration = :gen, "
            "queueingCompletedAt = :now, "
            "consecutiveEmptyChecks = :zero"
        ),
        ExpressionAttributeValues={
            ":status": status,
            ":gen": generation,
            ":now": now,
            ":zero": 0,
        },
    )


def handler(event, context):
    generation = event.get("generation", 0)
    batch_id = context.aws_request_id
    logger.info("Starting Queueing run: batchId=%s generation=%d", batch_id, generation)

    candidate_count, enqueued_count = stream_and_enqueue_urls(batch_id, generation)
    record_queueing_complete(generation, enqueued_count)

    logger.info(
        "Queueing complete: batchId=%s generation=%d crawlEnabledCount=%d enqueued=%d",
        batch_id, generation, candidate_count, enqueued_count
    )
    return {
        "batchId": batch_id,
        "generation": generation,
        "candidateCount": candidate_count,
        "urlsEnqueued": enqueued_count,
    }
