"""
Discovery Lambda
----------------
Runs on a nightly EventBridge Scheduler cron. Discovers URLs from a
sitemap (or a static seed list), normalizes and deduplicates them,
drops anything matching a hard exclusion pattern, and upserts each
surviving URL into the DynamoDB content registry.

This Lambda does NOT touch SQS. Manual control over which registry
URLs actually get crawled and ingested lives in two per-item flags --
crawlEnabled and ingestionEnabled -- that a human can flip directly in
the DynamoDB console without redeploying any code. A separate Queueing
Lambda (invoked at the end of this one) is responsible for reading the
registry for crawlEnabled="true" items and pushing those to
crawl-queue; it is not included here.

Required environment variables:
  STATE_TABLE_NAME     - DynamoDB pipeline-state table (PK: pipelineId)
  REGISTRY_TABLE_NAME  - DynamoDB content-registry table (PK: urlHash)
  QUEUEING_LAMBDA_NAME - name (or ARN) of the Queueing Lambda to invoke
                          once this run's registry writes are complete
Optional (pick one URL source):
  SITEMAP_URL       - e.g. https://example.com/sitemap.xml
  SEED_BUCKET       - S3 bucket holding a static newline-separated URL list
  SEED_KEY          - S3 key of that seed list
Optional (filtering, applied BEFORE a URL ever becomes a registry item --
distinct from crawlEnabled, which gates URLs already in the registry):
  ALLOW_URL_PATTERNS   - comma-separated regex patterns/prefixes defining
                          allowed banking sections, e.g.
                          "/personal-banking,/loans,/cards,/deposits,/rates,/faqs".
                          If set, only URLs matching at least one pattern become
                          registry items.
  EXCLUDE_URL_PATTERNS - comma-separated regex patterns, e.g.
                          "/login,/search,/cart,\\.pdf$"
  BLOCKLIST_TABLE_NAME - optional DynamoDB table (PK: url String) holding exact
                          full URLs to block preemptively.
Optional (tuning):
  UPSERT_CONCURRENCY - how many DynamoDB upserts to run in parallel
                        (default 20). DynamoDB's update_item has no
                        batch form that supports conditional
                        if_not_exists logic, so large sites are upserted
                        with a small thread pool rather than one call at
                        a time, to stay comfortably within the timeout.
"""

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import sha256
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from urllib.robotparser import RobotFileParser

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

SITEMAP_URL = os.environ.get("SITEMAP_URL")
SEED_BUCKET = os.environ.get("SEED_BUCKET")
SEED_KEY = os.environ.get("SEED_KEY")
STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
QUEUEING_LAMBDA_NAME = os.environ["QUEUEING_LAMBDA_NAME"]
BLOCKLIST_TABLE_NAME = os.environ.get("BLOCKLIST_TABLE_NAME")
UPSERT_CONCURRENCY = int(os.environ.get("UPSERT_CONCURRENCY", "20"))
RESPECT_ROBOTS_TXT = os.environ.get("RESPECT_ROBOTS_TXT", "true").lower() != "false"

state_table = dynamodb.Table(STATE_TABLE_NAME)
registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
STATE_KEY = {"pipelineId": "kb-sync"}

USER_AGENT = "internal-kb-crawler/1.0"
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

ALLOW_PATTERNS = [
    re.compile(p.strip())
    for p in os.environ.get("ALLOW_URL_PATTERNS", "").split(",")
    if p.strip()
]

EXCLUDE_PATTERNS = [
    re.compile(p.strip())
    for p in os.environ.get("EXCLUDE_URL_PATTERNS", "").split(",")
    if p.strip()
]

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}


def get_next_generation():
    """
    Strictly increasing counter, one new value per Discovery run. Stamped
    onto every registry item this run touches (as lastSeenGeneration) so
    the ingestion-trigger Lambda's reconciliation pass can tell which
    URLs were NOT found in the latest sitemap -- those have disappeared
    from the site and should eventually be marked DELETED. Note this is
    distinct from crawlGeneration (set by the Crawler Lambda, only when
    a URL is actually fetched) -- a crawlEnabled=false URL is still on
    the site and must not be mistaken for one that disappeared.

    Also updates pipelineStatus to 'DISCOVERING' and resets empty checks
    so the Ingestion-trigger Lambda knows a crawl discovery cycle is active
    and will not trigger premature reconciliation while queues are empty.
    """
    now = int(time.time())
    resp = state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression=(
            "ADD crawlGeneration :one "
            "SET pipelineStatus = :discovering, "
            "discoveryStartedAt = :now, "
            "consecutiveEmptyChecks = :zero"
        ),
        ExpressionAttributeValues={
            ":one": 1,
            ":discovering": "DISCOVERING",
            ":now": now,
            ":zero": 0,
        },
        ReturnValues="UPDATED_NEW",
    )
    return int(resp["Attributes"]["crawlGeneration"])


def mark_discovery_complete(generation):
    """
    Stamps lastDiscoveredGeneration and moves pipelineStatus to 'QUEUEING'.
    The Ingestion-trigger Lambda will only reconcile deleted URLs against a
    generation that has completed discovery, avoiding accidental wipes.
    """
    now = int(time.time())
    state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression=(
            "SET pipelineStatus = :queueing, "
            "lastDiscoveredGeneration = :gen, "
            "discoveryCompletedAt = :now"
        ),
        ExpressionAttributeValues={
            ":queueing": "QUEUEING",
            ":gen": generation,
            ":now": now,
        },
    )


def get_urls_from_sitemap(sitemap_url, _seen=None):
    """Handles both a flat <urlset> and a <sitemapindex> of child sitemaps."""
    if _seen is None:
        _seen = set()
    if sitemap_url in _seen:
        return []
    _seen.add(sitemap_url)

    req = urllib.request.Request(sitemap_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        xml_bytes = resp.read()
    root = ET.fromstring(xml_bytes)

    root_tag = root.tag.rsplit("}", 1)[-1]
    locs = [loc.text.strip() for loc in root.findall(".//sm:loc", SITEMAP_NS) if loc.text]

    if root_tag == "sitemapindex":
        logger.info("Sitemap index found: %s with %d child sitemap(s)", sitemap_url, len(locs))
        urls = []
        for child_sitemap_url in locs:
            urls.extend(get_urls_from_sitemap(child_sitemap_url, _seen))
        return urls

    return locs


def get_urls_from_s3_seed(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return [line.strip() for line in body.splitlines() if line.strip()]


def normalize_url(url):
    """Canonicalizes a URL so equivalent variants collapse to one registry item."""
    parts = urlsplit(url.strip())
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    )
    query = urlencode(query_pairs)
    return urlunsplit(("https", netloc, path, query, ""))


def load_blocked_urls():
    """
    Scans the optional BLOCKLIST_TABLE_NAME DynamoDB table (PK: url String)
    once at startup. Returns a set of normalized URLs for O(1) in-memory checks.
    Cost is negligible (~1 RCU for typical lists under 1000 items).
    """
    if not BLOCKLIST_TABLE_NAME:
        return set()
    blocked = set()
    try:
        table = dynamodb.Table(BLOCKLIST_TABLE_NAME)
        scan_kwargs = {
            "ProjectionExpression": "#url",
            "ExpressionAttributeNames": {"#url": "url"},
        }
        while True:
            resp = table.scan(**scan_kwargs)
            for item in resp.get("Items", []):
                raw = item.get("url", "").strip()
                if raw:
                    blocked.add(normalize_url(raw))
            if "LastEvaluatedKey" not in resp:
                break
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        logger.info("Loaded %d blocked URL(s) from DynamoDB %s", len(blocked), BLOCKLIST_TABLE_NAME)
    except Exception as e:
        logger.warning("Failed to load blocklist table %s: %s", BLOCKLIST_TABLE_NAME, e, exc_info=True)
    return blocked


def is_excluded(url):
    return any(rx.search(url) for rx in EXCLUDE_PATTERNS)


def is_allowlisted(url):
    """
    If ALLOW_URL_PATTERNS is configured, only URLs matching at least one
    pattern are allowed into the registry. If empty, allows everything.
    """
    if not ALLOW_PATTERNS:
        return True
    return any(rx.search(url) for rx in ALLOW_PATTERNS)


def load_robots_parser(sample_url):
    """
    robots.txt always lives at the site root, not wherever the sitemap
    happens to be -- so this derives the root from any one discovered
    URL (works whether URLs came from SITEMAP_URL or a seed list).
    Fetches robots.txt using USER_AGENT and explicit timeout so bank WAFs
    do not block Python-urllib's default user agent or hang indefinitely.
    Returns None if robots.txt cannot be fetched (network or 5xx error),
    failing open so a hiccup does not block the whole run.
    If robots.txt returns 404/410, returns an empty parser (allowing everything).
    """
    parts = urlsplit(sample_url)
    robots_url = urlunsplit(("https", parts.netloc, "/robots.txt", "", ""))
    rp = RobotFileParser()
    rp.set_url(robots_url)

    req = urllib.request.Request(robots_url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8", errors="replace")
            rp.parse(content.splitlines())
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            logger.info("%s returned HTTP %d; assuming no robots restrictions", robots_url, e.code)
            rp.parse([])
            return rp
        logger.warning("HTTP error %d fetching %s -- proceeding without robots rules", e.code, robots_url)
        return None
    except Exception as e:
        logger.warning("Could not read %s: %s -- proceeding without robots rules", robots_url, e, exc_info=True)
        return None

    # Crawl-delay isn't enforceable by this architecture today (reserved
    # concurrency caps how many parallel requests run, not the spacing
    # between them) -- surfacing it loudly rather than silently ignoring
    # a real directive the site owner set.
    delay = rp.crawl_delay(USER_AGENT) or rp.crawl_delay("*")
    if delay:
        logger.warning(
            "%s specifies Crawl-delay=%ss, which this pipeline does not currently enforce -- only concurrency is capped",
            robots_url, delay
        )
    return rp


def is_allowed_by_robots(rp, url):
    if rp is None:
        return True
    return rp.can_fetch(USER_AGENT, url)


def upsert_registry_item(url, generation):
    """
    Creates the item on first sight, refreshes url/lastSeenGeneration/
    lastSeenAt on every later run -- but crawlEnabled, ingestionEnabled,
    and status are only set via if_not_exists(), so a human's manual
    edit in the DynamoDB console is never clobbered by a future run.
    """
    url_hash = sha256(url.encode("utf-8")).hexdigest()
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #url = :url, "
            "lastSeenGeneration = :gen, "
            "lastSeenAt = :now, "
            "crawlEnabled = if_not_exists(crawlEnabled, :true_str), "
            "ingestionEnabled = if_not_exists(ingestionEnabled, :true_str), "
            "#status = if_not_exists(#status, :discovered)"
        ),
        ExpressionAttributeNames={"#url": "url", "#status": "status"},
        ExpressionAttributeValues={
            ":url": url,
            ":gen": generation,
            ":now": int(time.time()),
            ":true_str": "true",
            ":discovered": "DISCOVERED",
        },
    )
    return url_hash


def upsert_all(urls, generation):
    succeeded, failed = 0, []
    with ThreadPoolExecutor(max_workers=UPSERT_CONCURRENCY) as pool:
        futures = {pool.submit(upsert_registry_item, u, generation): u for u in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                future.result()
                succeeded += 1
            except Exception as e:
                logger.error("Failed to upsert registry item for %s: %s", url, e, exc_info=True)
                failed.append(url)
    return succeeded, failed


def trigger_queueing_lambda(generation):
    lambda_client.invoke(
        FunctionName=QUEUEING_LAMBDA_NAME,
        InvocationType="Event",  # async -- fire and forget, don't block on it
        Payload=json.dumps({"generation": generation}).encode("utf-8"),
    )


def handler(event, context):
    generation = get_next_generation()
    logger.info("Starting Discovery run: generation=%d sitemap=%s seedBucket=%s", generation, SITEMAP_URL, SEED_BUCKET)

    if SITEMAP_URL:
        raw_urls = get_urls_from_sitemap(SITEMAP_URL)
    elif SEED_BUCKET and SEED_KEY:
        raw_urls = get_urls_from_s3_seed(SEED_BUCKET, SEED_KEY)
    else:
        raise ValueError("Set either SITEMAP_URL or SEED_BUCKET/SEED_KEY")

    deduped = []
    seen = set()
    for u in raw_urls:
        norm = normalize_url(u)
        if norm not in seen:
            seen.add(norm)
            deduped.append(norm)

    robots_parser = None
    if RESPECT_ROBOTS_TXT and deduped:
        robots_parser = load_robots_parser(deduped[0])

    blocked_urls = load_blocked_urls()

    robots_blocked_count = 0
    blocklist_table_count = 0
    excluded_count = 0
    not_allowlisted_count = 0
    candidate_urls = []
    for u in deduped:
        if not is_allowed_by_robots(robots_parser, u):
            robots_blocked_count += 1
        elif u in blocked_urls:
            blocklist_table_count += 1
        elif is_excluded(u):
            excluded_count += 1
        elif not is_allowlisted(u):
            not_allowlisted_count += 1
        else:
            candidate_urls.append(u)

    succeeded, failed = upsert_all(candidate_urls, generation)

    logger.info(
        "Discovery run complete: generation=%d raw=%d afterDedup=%d robotsBlocked=%d "
        "blocklistTableBlocked=%d excluded=%d notAllowlisted=%d upserted=%d failed=%d",
        generation, len(raw_urls), len(deduped), robots_blocked_count,
        blocklist_table_count, excluded_count, not_allowlisted_count,
        succeeded, len(failed)
    )
    if failed:
        # A handful of failed upserts don't block the run -- they're fully
        # idempotent and will self-heal next generation -- but they
        # should never go unnoticed.
        logger.error("URLs that failed to upsert this run (generation=%d): %s", generation, failed)

    mark_discovery_complete(generation)
    trigger_queueing_lambda(generation)

    return {
        "generation": generation,
        "rawUrls": len(raw_urls),
        "afterDedup": len(deduped),
        "robotsBlocked": robots_blocked_count,
        "blocklistTableBlocked": blocklist_table_count,
        "excluded": excluded_count,
        "notAllowlisted": not_allowlisted_count,
        "upserted": succeeded,
        "failed": len(failed),
    }
