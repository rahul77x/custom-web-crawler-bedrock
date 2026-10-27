"""
Crawler Lambda
--------------
Triggered by SQS (the crawl-queue the Queueing Lambda fills, one message
per URL). Fetches the page, hashes its content, and only writes to the
raw S3 bucket and updates the DynamoDB registry if the content actually
changed since the last crawl. Unchanged pages are a cheap no-op -- no S3
write, no downstream reprocessing, no re-ingestion cost.

Safety checks done at FETCH time (defence in depth -- the queue is not
trusted to contain only approved URLs):
  * Host allow-list: the URL's host must be an enabled host in the
    crawl-sources table (aliases such as the apex domain are mapped to
    their target). Otherwise the message is skipped (no retry).
  * robots.txt: fetched per host and cached for an hour. A disallowed URL
    is skipped. 4xx on robots.txt means no restrictions; 5xx/429/network
    failure means "unreachable" and the URL is skipped this run (RFC 9309:
    don't crawl blind). Crawl-delay is logged, not enforced.
  * Redirects: EVERY hop must be https and land on an approved host.
    Anything else is recorded as REDIRECT_BLOCKED instead of being
    followed to a login/SSO host or off-site target. Redirect limits and
    loops are recorded, not retried into the DLQ.
  * Redirect-to-known-URL: if the final URL is a different registry item,
    the requested URL is treated as an alias and NOT ingested (the
    canonical URL gets ingested from its own message), so the same page
    is not stored twice.
  * Response size: bodies over MAX_HTML_BYTES are skipped.

Skips do NOT change `status`: they set crawlSkipReason / crawlSkippedAt on
the registry item (a successful crawl clears them), so a previously
APPROVED page keeps its lifecycle status.

Uses only boto3 + the standard library (urllib), so it pastes straight
into the console code editor with no layer needed.

Required environment variables:
  RAW_BUCKET           - S3 bucket for raw crawled HTML
  REGISTRY_TABLE_NAME  - DynamoDB content-registry table (PK: urlHash)
  SOURCES_TABLE_NAME   - DynamoDB crawl-sources table (PK: host); shared
                         with the Discovery Lambda
Optional:
  CRAWLER_USER_AGENT   - default "internal-kb-crawler/1.0" (keep identical to
                         Discovery's; robots.txt rules match on the text
                         before the first "/")
  RESPECT_ROBOTS_TXT   - default true
  MAX_HTML_BYTES       - default 5242880 (5 MB)
  FETCH_TIMEOUT_SECONDS - default 15

Configure the SQS trigger in the console with:
  - Batch size: 1-5 (each item does a network fetch; keep batches small
    so one slow page doesn't hold up the others)
  - Report batch item failures: ON, function response type
    "ReportBatchItemFailures" -- this handler relies on that so a single
    bad URL retries by itself instead of retrying the whole batch

Also set Reserved concurrency on this function under Configuration ->
Concurrency, to cap how many requests hit the target site at once (e.g.
10) -- this is what keeps the crawl polite rather than a de facto DDoS.

NOTE: normalize_url() below is a copy of the one in the Discovery Lambda
(needed to compare redirect targets with registry keys). Keep them in sync,
or move both into a shared Lambda layer.
"""

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

RAW_BUCKET = os.environ["RAW_BUCKET"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
SOURCES_TABLE_NAME = os.environ["SOURCES_TABLE_NAME"]
registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
sources_table = dynamodb.Table(SOURCES_TABLE_NAME)

USER_AGENT = os.environ.get("CRAWLER_USER_AGENT", "internal-kb-crawler/1.0")
RESPECT_ROBOTS_TXT = os.environ.get("RESPECT_ROBOTS_TXT", "true").lower() != "false"
FETCH_TIMEOUT_SECONDS = int(os.environ.get("FETCH_TIMEOUT_SECONDS", "15"))
MAX_HTML_BYTES = int(os.environ.get("MAX_HTML_BYTES", str(5 * 1024 * 1024)))

CONFIG_TTL_SECONDS = 300
ROBOTS_TTL_SECONDS = 3600
ROBOTS_FAILURE_TTL_SECONDS = 300

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}

# Warm-container caches (a Lambda container reuses module state between
# invocations, so this avoids a table scan / robots.txt fetch per message).
_config = {"expires": 0.0, "enabled": set(), "aliases": {}}
_robots = {}  # host -> (expires_at, parser_or_None, error_or_None)
_delay_warned = set()


class RedirectBlocked(Exception):
    """A redirect pointed outside the approved hosts (or off https)."""


class RobotsUnavailable(Exception):
    """robots.txt could not be fetched (5xx / 429 / network)."""


class ResponseTooLarge(Exception):
    pass


# --------------------------------------------------------------------------
# Host configuration (crawl-sources, cached)
# --------------------------------------------------------------------------

def get_crawl_config():
    now = time.time()
    if now < _config["expires"]:
        return _config
    try:
        enabled, aliases, kwargs = set(), {}, {}
        while True:
            resp = sources_table.scan(**kwargs)
            for item in resp.get("Items", []):
                host = str(item.get("host", "")).strip().lower()
                if not host:
                    continue
                alias_of = str(item.get("aliasOf", "")).strip().lower()
                if alias_of:
                    aliases[host] = alias_of
                elif str(item.get("enabled", "false")).lower() == "true":
                    enabled.add(host)
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        _config.update(expires=now + CONFIG_TTL_SECONDS, enabled=enabled, aliases=aliases)
    except Exception as e:
        if _config["enabled"]:
            logger.warning("Could not refresh crawl-sources (%s); using cached config", e)
            _config["expires"] = now + 60
        else:
            raise
    return _config


def host_of(url):
    try:
        return (urlsplit(url).hostname or "").lower().rstrip(".")
    except ValueError:
        return ""


def normalize_url(url, aliases):
    """Same canonicalization as the Discovery Lambda. Returns None if unusable."""
    try:
        parts = urlsplit(url.strip())
        host = (parts.hostname or "").lower().rstrip(".")
        port = parts.port
    except ValueError:
        return None
    if not host or parts.scheme not in ("http", "https"):
        return None
    host = aliases.get(host, host)
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    )
    return urlunsplit(("https", netloc, path, urlencode(query_pairs), ""))


class ScopedRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Validates EVERY redirect hop: https only, host must be approved."""

    max_redirections = 5

    def __init__(self, allowed_hosts, aliases):
        super().__init__()
        self._allowed = set(allowed_hosts)
        self._aliases = aliases

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parts = urlsplit(newurl)
        host = (parts.hostname or "").lower()
        host = self._aliases.get(host, host)
        if parts.scheme != "https" or host not in self._allowed:
            raise RedirectBlocked(f"{req.full_url} -> {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def build_opener(config):
    allowed = set(config["enabled"]) | set(config["aliases"])
    return urllib.request.build_opener(ScopedRedirectHandler(allowed, config["aliases"]))


# --------------------------------------------------------------------------
# robots.txt
# --------------------------------------------------------------------------

def read_limited(resp, limit):
    chunks, total = [], 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise ResponseTooLarge(f"response exceeds {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def _glob_match(segments, anchored, text):
    """
    Matches `text` against a robots.txt pattern pre-split on '*'. Supports
    only what RFC 9309 defines: '*' (any run of characters) and a trailing
    '$' (end of URL). Linear scan, no regex, so a hostile pattern cannot
    cause catastrophic backtracking.
    """
    if len(segments) == 1:
        return text == segments[0] if anchored else text.startswith(segments[0])
    first, last, middle = segments[0], segments[-1], segments[1:-1]
    if not text.startswith(first):
        return False
    pos = len(first)
    for seg in middle:
        idx = text.find(seg, pos)
        if idx < 0:
            return False
        pos = idx + len(seg)
    if anchored:
        return len(text) - pos >= len(last) and text.endswith(last)
    return text.find(last, pos) >= 0


class RobotsRules:
    """
    Small RFC 9309 robots.txt matcher, used instead of urllib.robotparser.

    The stdlib parser only does prefix matching: it ignores the '*' and '$'
    wildcards (so `Disallow: /*.pdf$` blocks nothing) and uses first-match
    instead of longest-match. This class implements:
      * groups with one or more User-agent lines; the group naming our
        product token wins, otherwise the '*' group; multiple groups for the
        same agent are merged
      * '*' wildcard and trailing '$' anchor in Allow/Disallow paths
      * longest matching rule wins; on a tie, Allow wins
      * empty `Disallow:` means no restriction
      * matching on path + query, case-sensitive, percent-decoded
      * Sitemap lines (global) and Crawl-delay (informational)
    Only the methods this pipeline uses are provided: parse(), can_fetch(),
    site_maps(), crawl_delay().
    NOTE: keep this class identical in the Discovery and Crawler Lambdas.
    """

    def __init__(self):
        self._groups = []      # {"agents": set, "rules": [(allow, segments, anchored, length)], "delay": float|None}
        self._sitemaps = []

    @staticmethod
    def _token(user_agent):
        parts = (user_agent or "").strip().split("/")[0].split()
        return parts[0].lower() if parts else "*"

    def parse(self, lines):
        group, in_agents = None, False
        for raw in lines:
            line = raw.split("#", 1)[0].strip().lstrip("\ufeff")
            if not line or ":" not in line:
                continue
            field, value = line.split(":", 1)
            field, value = field.strip().lower(), value.strip()
            if field == "user-agent":
                if group is None or not in_agents:
                    group = {"agents": set(), "rules": [], "delay": None}
                    self._groups.append(group)
                group["agents"].add(value.lower())
                in_agents = True
            elif field in ("allow", "disallow"):
                in_agents = False
                if group is None or not value:
                    continue  # rule outside a group, or empty value (= no restriction)
                pattern = unquote(value)
                anchored = pattern.endswith("$")
                if anchored:
                    pattern = pattern[:-1]
                group["rules"].append((field == "allow", pattern.split("*"), anchored, len(value)))
            elif field == "crawl-delay":
                in_agents = False
                if group is not None:
                    try:
                        group["delay"] = float(value)
                    except ValueError:
                        pass
            elif field == "sitemap":
                if value:
                    self._sitemaps.append(value)  # global; does not end a group

    def _selected_groups(self, user_agent):
        token = self._token(user_agent)
        specific = [g for g in self._groups if token in g["agents"]]
        return specific or [g for g in self._groups if "*" in g["agents"]]

    def can_fetch(self, user_agent, url):
        parts = urlsplit(url)
        path = parts.path or "/"
        if path == "/robots.txt":
            return True
        target = unquote(path + ("?" + parts.query if parts.query else ""))
        best = None  # (rule length, allow)
        for group in self._selected_groups(user_agent):
            for allow, segments, anchored, length in group["rules"]:
                if _glob_match(segments, anchored, target):
                    if best is None or length > best[0] or (length == best[0] and allow and not best[1]):
                        best = (length, allow)
        return True if best is None else best[1]

    def site_maps(self):
        return list(self._sitemaps) or None

    def crawl_delay(self, user_agent):
        for group in self._selected_groups(user_agent):
            if group["delay"] is not None:
                return group["delay"]
        return None


def fetch_robots(opener, host):
    robots_url = f"https://{host}/robots.txt"
    req = urllib.request.Request(robots_url, headers={"User-Agent": USER_AGENT})
    rp = RobotsRules()
    try:
        with opener.open(req, timeout=10) as resp:
            content = read_limited(resp, 1024 * 1024).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if 400 <= e.code < 500 and e.code != 429:
            rp.parse([])  # no robots.txt => no restrictions
            return rp
        raise RobotsUnavailable(f"HTTP {e.code} from {robots_url}")
    except RedirectBlocked as e:
        raise RobotsUnavailable(f"blocked redirect fetching robots.txt: {e}")
    except Exception as e:
        raise RobotsUnavailable(f"{robots_url}: {e}")
    rp.parse(content.splitlines())
    delay = rp.crawl_delay(USER_AGENT) or rp.crawl_delay("*")
    if delay and host not in _delay_warned:
        _delay_warned.add(host)
        logger.warning(
            "%s specifies Crawl-delay=%ss, which this pipeline does not enforce "
            "(only reserved concurrency limits the request rate)", robots_url, delay
        )
    return rp


def get_robots(opener, host):
    now = time.time()
    cached = _robots.get(host)
    if cached and cached[0] > now:
        if cached[2]:
            raise RobotsUnavailable(cached[2])
        return cached[1]
    try:
        rp = fetch_robots(opener, host)
    except RobotsUnavailable as e:
        _robots[host] = (now + ROBOTS_FAILURE_TTL_SECONDS, None, str(e))
        raise
    _robots[host] = (now + ROBOTS_TTL_SECONDS, rp, None)
    return rp


# --------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------

def fetch_url(opener, url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener.open(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            content_type = resp.headers.get("Content-Type", "")
            declared = resp.headers.get("Content-Length")
            if declared and declared.isdigit() and int(declared) > MAX_HTML_BYTES:
                raise ResponseTooLarge(f"Content-Length {declared} exceeds {MAX_HTML_BYTES}")
            body = read_limited(resp, MAX_HTML_BYTES)
            status = resp.status
            final_url = resp.geturl()
        return status, content_type, body, final_url
    except urllib.error.HTTPError as e:
        return e.code, "", b"", url


# --------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------

def get_registry_item(url_hash):
    resp = registry_table.get_item(Key={"urlHash": url_hash})
    return resp.get("Item")


def touch_unchanged(url_hash, batch_id, generation, final_url):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET lastCheckedAt = :now, lastBatchId = :batch, crawlGeneration = :gen, "
            "finalUrl = :final REMOVE crawlSkipReason, crawlSkippedAt, crawlSkipDetail"
        ),
        ExpressionAttributeValues={
            ":now": int(time.time()),
            ":batch": batch_id,
            ":gen": generation,
            ":final": final_url,
        },
    )


def record_skip(url_hash, reason, detail=None):
    """
    Records WHY a URL was not fetched/ingested without touching `status`, so a
    previously APPROVED page keeps its lifecycle state. Cleared by the next
    successful crawl.
    """
    now = int(time.time())
    expr = "SET crawlSkipReason = :r, crawlSkippedAt = :now, lastCheckedAt = :now"
    values = {":r": reason, ":now": now}
    if detail:
        expr += ", crawlSkipDetail = :d"
        values[":d"] = str(detail)[:500]
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=expr,
        ExpressionAttributeValues=values,
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


def record_crawled(url_hash, url, content_hash, raw_key, batch_id, generation, final_url):
    now = int(time.time())
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #url = :url, contentHash = :content_hash, rawS3Key = :raw_key, "
            "#status = :status, lastCrawledAt = :now, lastCheckedAt = :now, "
            "lastBatchId = :batch, crawlGeneration = :gen, finalUrl = :final "
            "REMOVE crawlSkipReason, crawlSkippedAt, crawlSkipDetail"
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
            ":final": final_url,
        },
    )


# --------------------------------------------------------------------------
# Message processing
# --------------------------------------------------------------------------

def process_message(body):
    url = body["url"]
    batch_id = body.get("batchId", "unknown")
    generation = body.get("generation", 0)
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()

    config = get_crawl_config()
    aliases = config["aliases"]
    host = host_of(url)
    host = aliases.get(host, host)

    # 1. Host allow-list
    if host not in config["enabled"]:
        logger.warning("Skipping %s (urlHash=%s): host %s is not an enabled source", url, url_hash, host)
        record_skip(url_hash, "HOST_NOT_ENABLED")
        return

    opener = build_opener(config)

    # 2. robots.txt
    if RESPECT_ROBOTS_TXT:
        try:
            rp = get_robots(opener, host)
        except RobotsUnavailable as e:
            logger.warning("Skipping %s (urlHash=%s): robots.txt unavailable (%s)", url, url_hash, e)
            record_skip(url_hash, "ROBOTS_UNAVAILABLE")
            return
        if not rp.can_fetch(USER_AGENT, url):
            logger.info("Skipping %s (urlHash=%s): disallowed by robots.txt", url, url_hash)
            record_skip(url_hash, "ROBOTS_DISALLOWED")
            return

    # 3. Fetch (every redirect hop validated)
    t0 = time.time()
    try:
        status, content_type, content, final_url = fetch_url(opener, url)
    except RedirectBlocked as e:
        logger.warning("Redirect blocked for %s (urlHash=%s): %s", url, url_hash, e)
        record_skip(url_hash, "REDIRECT_BLOCKED", detail=e)
        return
    except ResponseTooLarge as e:
        logger.warning("Skipping %s (urlHash=%s): %s", url, url_hash, e)
        record_skip(url_hash, "RESPONSE_TOO_LARGE")
        return
    duration_ms = int((time.time() - t0) * 1000)

    if 300 <= status < 400:
        # urllib raises HTTPError with the 3xx code when it gives up (redirect
        # limit or loop). Permanent from our point of view: do not retry.
        logger.warning("Redirect limit/loop (HTTP %d, %dms): url=%s urlHash=%s", status, duration_ms, url, url_hash)
        record_skip(url_hash, "REDIRECT_LIMIT")
        return

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

    # 4. Redirect handling: is the final URL the page we meant to crawl?
    requested_norm = normalize_url(url, aliases) or url
    final_norm = normalize_url(final_url, aliases) or final_url
    if final_norm != requested_norm:
        logger.info("Redirect followed: %s -> %s (urlHash=%s)", url, final_url, url_hash)
        final_hash = hashlib.sha256(final_norm.encode("utf-8")).hexdigest()
        if final_hash != url_hash and get_registry_item(final_hash):
            logger.info(
                "Skipping %s (urlHash=%s): redirects to %s, which is its own registry item "
                "and will be ingested from there", url, url_hash, final_norm
            )
            record_skip(url_hash, "REDIRECTS_TO_KNOWN_URL", detail=final_norm)
            return

    if content_type and "text/html" not in content_type.lower():
        logger.info("Skipping non-HTML content (%s): url=%s urlHash=%s", content_type, url, url_hash)
        return

    content_hash = hashlib.sha256(content).hexdigest()
    existing = get_registry_item(url_hash)

    if existing and existing.get("contentHash") == content_hash:
        touch_unchanged(url_hash, batch_id, generation, final_norm)
        logger.debug("Unchanged content (%dms): url=%s urlHash=%s (skipped S3 write)", duration_ms, url, url_hash)
        return

    metadata = {"source-url": url}
    if final_norm != requested_norm:
        metadata["final-url"] = final_url[:1000]

    raw_key = f"raw/{time.strftime('%Y-%m-%d')}/{url_hash}.html"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=raw_key,
        Body=content,
        ContentType=content_type or "text/html",
        Metadata=metadata,
    )
    record_crawled(url_hash, url, content_hash, raw_key, batch_id, generation, final_norm)
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
