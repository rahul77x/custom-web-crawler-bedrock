"""
Discovery Lambda
----------------
Runs on a nightly EventBridge Scheduler cron. Discovers URLs for every
APPROVED host listed in the crawl-sources table (www plus whichever
subdomains you have switched on), normalizes and deduplicates them,
applies the robots/blocklist/exclude/allow filters, and upserts each
surviving URL into the DynamoDB content registry.

How hosts are discovered
  * You give it ONE root URL (ROOT_URL, e.g. https://www.bankofireland.com/).
    That host is bootstrapped into crawl-sources as enabled.
  * Each run it scans the root page's links, and any sitemap URLs that
    fall outside the approved hosts, for other hosts under the same
    domain. New ones are written to crawl-sources as CANDIDATES with
    enabled="false". NOTHING is crawled from a candidate until a human
    flips enabled to "true" in the DynamoDB console.
  * For each enabled host it finds the sitemap(s): crawl-sources
    `sitemapUrls` override, else SITEMAP_URL (if it belongs to that
    host), else the `Sitemap:` lines in the host's own robots.txt, else
    /sitemap.xml, /sitemaps.xml, /sitemap_index.xml.
  * A host with no discoverable sitemap is reported as an ERROR rather
    than silently skipped. Give it a `sitemapUrls` override, or add its
    URLs to the S3 seed file.

crawl-sources table (PK: host, String)
  enabled            "true"/"false" (String) -- the manual switch
  origin             BOOTSTRAP | LINK_SCAN | SITEMAP_URL | BOOTSTRAP_ALIAS
  aliasOf            optional; marks the row as an alias (e.g. the apex
                     domain pointing at www). Alias rows are never crawled;
                     their URLs are rewritten to the target host.
  sitemapUrls        optional override (String comma-separated, or List)
  lastStatus / lastError / lastRunGeneration / lastRunAt   -- per-host health
  lastGoodUrlCount   URL count from the last good run; drives the sanity
                     guard. Edit it in the console to accept a legitimate
                     large drop.

Completeness guard
  If ANY enabled host fails (fetch error, no sitemap, robots.txt
  unreachable, URL count dropped by more than MAX_URL_DROP_PERCENT), or
  any registry upsert fails, the run is INCOMPLETE: pages found are still
  upserted and crawled, but lastDiscoveredGeneration is NOT stamped, so
  the ingestion-trigger's reconciliation pass cannot treat the missing
  pages as deleted. Look for "DISCOVERY_INCOMPLETE" in the logs (a good
  CloudWatch metric-filter target).

NOTE: disabling a host (or removing its URLs from the sitemap) means its
registry items stop getting a fresh lastSeenGeneration, so at the next
COMPLETE run they are reconciled to DELETED and removed from kb-source.

Revival: if a URL that was reconciled to DELETED shows up again (re-added to
the sitemap, host re-enabled, an exclude pattern removed, or it was deleted
by mistake), Discovery resets it to DISCOVERED and clears its contentHash.
Without that reset the Crawler would see the old hash, call the page
"unchanged", and it would never return to the knowledge base.

This Lambda does NOT touch SQS. A separate Queueing Lambda (invoked at the
end of this one) pushes crawlEnabled="true" registry items to crawl-queue.

Required environment variables:
  STATE_TABLE_NAME     - DynamoDB pipeline-state table (PK: pipelineId)
  REGISTRY_TABLE_NAME  - DynamoDB content-registry table (PK: urlHash)
  SOURCES_TABLE_NAME   - DynamoDB crawl-sources table (PK: host)
  QUEUEING_LAMBDA_NAME - name (or ARN) of the Queueing Lambda
  ROOT_URL             - e.g. https://www.bankofireland.com/ (bootstraps the
                         root host, and defines the domain used for candidate
                         detection). Optional only if crawl-sources is already
                         populated.
Optional:
  DOMAIN_SUFFIX        - domain whose subdomains count as candidates. Default:
                         ROOT_URL host without a leading "www.".
  SITEMAP_URL          - explicit sitemap for the host it belongs to
  SEED_BUCKET/SEED_KEY - S3 file of extra URLs (one per line); their hosts
                         must still be enabled in crawl-sources
  CRAWLER_USER_AGENT   - default "internal-kb-crawler/1.0". robots.txt rules
                         match on the text before the first "/". Use the same
                         value in the Crawler Lambda.
  RESPECT_ROBOTS_TXT   - default true
  ALLOW_URL_PATTERNS / EXCLUDE_URL_PATTERNS - comma-separated regexes, matched
                         against the full URL (so they apply to every host)
  BLOCKLIST_TABLE_NAME - DynamoDB table (PK: url) of exact URLs to block
  UPSERT_CONCURRENCY   - default 20
  MAX_SITEMAP_BYTES    - default 52428800 (the sitemap spec maximum)
  MAX_SITEMAP_FILES    - default 200 sitemap files fetched per host per run
  MAX_URL_DROP_PERCENT - default 30; set to 100 to disable the drop guard

Set the Lambda timeout generously (several minutes); hosts are fetched
one after another.
"""

import gzip
import io
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
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, unquote, urlencode

import boto3
from botocore.exceptions import ClientError

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

ROOT_URL = os.environ.get("ROOT_URL", "").strip()
SITEMAP_URL = os.environ.get("SITEMAP_URL", "").strip()
SEED_BUCKET = os.environ.get("SEED_BUCKET")
SEED_KEY = os.environ.get("SEED_KEY")
STATE_TABLE_NAME = os.environ["STATE_TABLE_NAME"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
SOURCES_TABLE_NAME = os.environ["SOURCES_TABLE_NAME"]
QUEUEING_LAMBDA_NAME = os.environ["QUEUEING_LAMBDA_NAME"]
BLOCKLIST_TABLE_NAME = os.environ.get("BLOCKLIST_TABLE_NAME")
UPSERT_CONCURRENCY = int(os.environ.get("UPSERT_CONCURRENCY", "20"))
RESPECT_ROBOTS_TXT = os.environ.get("RESPECT_ROBOTS_TXT", "true").lower() != "false"
USER_AGENT = os.environ.get("CRAWLER_USER_AGENT", "internal-kb-crawler/1.0")
MAX_SITEMAP_BYTES = int(os.environ.get("MAX_SITEMAP_BYTES", str(50 * 1024 * 1024)))
MAX_SITEMAP_FILES = int(os.environ.get("MAX_SITEMAP_FILES", "200"))
MAX_SITEMAP_DEPTH = 5
MAX_URL_DROP_PERCENT = int(os.environ.get("MAX_URL_DROP_PERCENT", "30"))

state_table = dynamodb.Table(STATE_TABLE_NAME)
registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)
sources_table = dynamodb.Table(SOURCES_TABLE_NAME)
STATE_KEY = {"pipelineId": "kb-sync"}

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
COMMON_SITEMAP_PATHS = ("/sitemap.xml", "/sitemaps.xml", "/sitemap_index.xml")

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


def host_of(url):
    try:
        return (urlsplit(url).hostname or "").lower().rstrip(".")
    except ValueError:
        return ""


ROOT_HOST = host_of(ROOT_URL) if ROOT_URL else ""
DOMAIN_SUFFIX = (
    os.environ.get("DOMAIN_SUFFIX", "").strip().lower()
    or (ROOT_HOST[4:] if ROOT_HOST.startswith("www.") else ROOT_HOST)
)

# alias host -> canonical host. Filled from crawl-sources rows that carry an
# `aliasOf` attribute (see load_sources). The single source of truth, shared
# with the Crawler Lambda, so the two can never disagree about hosts.
HOST_ALIASES = {}


def canonical_host(host):
    return HOST_ALIASES.get(host, host)


def under_domain(host):
    return bool(DOMAIN_SUFFIX) and (host == DOMAIN_SUFFIX or host.endswith("." + DOMAIN_SUFFIX))


class RedirectBlocked(Exception):
    """A redirect pointed outside the approved hosts (or off https)."""


class RobotsUnavailable(Exception):
    """robots.txt could not be fetched (5xx / 429 / network)."""


class ScopedRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Validates EVERY redirect hop: https only, and the target host must be an
    approved host (or an alias of one). Anything else raises RedirectBlocked
    instead of silently following it to a login/SSO host, an off-site open
    redirect, or an error page.
    """

    max_redirections = 5

    def __init__(self, allowed_hosts):
        super().__init__()
        self._allowed = set(allowed_hosts)

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parts = urlsplit(newurl)
        host = canonical_host((parts.hostname or "").lower())
        if parts.scheme != "https" or host not in self._allowed:
            raise RedirectBlocked(f"{req.full_url} -> {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def get_next_generation():
    """
    Strictly increasing counter, one new value per Discovery run. Stamped
    onto every registry item this run touches (as lastSeenGeneration) so
    the ingestion-trigger Lambda's reconciliation pass can tell which
    URLs were NOT found in the latest discovery -- those have disappeared
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


def mark_discovery_complete(generation, complete):
    """
    Moves pipelineStatus to 'QUEUEING'. lastDiscoveredGeneration is stamped
    ONLY when the run was complete -- the Ingestion-trigger Lambda reconciles
    deleted URLs against that value, so an incomplete run must never advance
    it (that would delete every page belonging to a host that failed).
    """
    now = int(time.time())
    expr = "SET pipelineStatus = :queueing, discoveryCompletedAt = :now, discoveryComplete = :complete"
    values = {":queueing": "QUEUEING", ":now": now, ":complete": complete}
    if complete:
        expr += ", lastDiscoveredGeneration = :gen"
        values[":gen"] = generation
    state_table.update_item(
        Key=STATE_KEY,
        UpdateExpression=expr,
        ExpressionAttributeValues=values,
    )


def release_pipeline():
    """
    Called when Discovery crashes before handing over to Queueing, so the
    pipeline is not left in DISCOVERING (which blocks ingestion for hours).
    """
    try:
        state_table.update_item(
            Key=STATE_KEY,
            UpdateExpression="SET pipelineStatus = :idle",
            ExpressionAttributeValues={":idle": "IDLE"},
        )
    except Exception:
        logger.error("Could not reset pipelineStatus to IDLE", exc_info=True)


# --------------------------------------------------------------------------
# crawl-sources table
# --------------------------------------------------------------------------

def as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def load_sources():
    """Returns (sources, aliases). Alias rows are separated out."""
    sources, aliases = {}, {}
    kwargs = {}
    while True:
        resp = sources_table.scan(**kwargs)
        for item in resp.get("Items", []):
            host = str(item.get("host", "")).strip().lower()
            if not host:
                continue
            alias_of = str(item.get("aliasOf", "")).strip().lower()
            if alias_of:
                aliases[host] = alias_of
            else:
                sources[host] = item
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return sources, aliases


def upsert_source(host, enabled_default, origin, alias_of=None):
    """
    Creates the row on first sight. enabled/origin/firstSeenAt only use
    if_not_exists, so a human's edit (enabling a candidate, disabling www)
    is never overwritten by a later run.
    """
    now = int(time.time())
    names = {"#en": "enabled", "#or": "origin"}
    values = {":en": enabled_default, ":or": origin, ":now": now}
    expr = (
        "SET #en = if_not_exists(#en, :en), #or = if_not_exists(#or, :or), "
        "firstSeenAt = if_not_exists(firstSeenAt, :now), lastSeenAt = :now"
    )
    if alias_of:
        expr += ", aliasOf = :alias"
        values[":alias"] = alias_of
    sources_table.update_item(
        Key={"host": host},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def record_source_result(host, generation, error=None, url_count=None):
    now = int(time.time())
    names = {"#ls": "lastStatus", "#lg": "lastRunGeneration", "#lr": "lastRunAt", "#le": "lastError"}
    values = {":ls": "ERROR" if error else "OK", ":lg": generation, ":lr": now}
    if error:
        expr = "SET #ls = :ls, #lg = :lg, #lr = :lr, #le = :le"
        values[":le"] = str(error)[:500]
    else:
        names["#lc"] = "lastGoodUrlCount"
        expr = "SET #ls = :ls, #lg = :lg, #lr = :lr, #lc = :lc REMOVE #le"
        values[":lc"] = url_count
    sources_table.update_item(
        Key={"host": host},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def check_url_count(count, last_good):
    """Returns a problem description, or None if the count looks sane."""
    if count == 0:
        return "sitemap(s) yielded 0 URLs for this host"
    if last_good and MAX_URL_DROP_PERCENT < 100:
        floor = last_good * (100 - MAX_URL_DROP_PERCENT) / 100.0
        if count < floor:
            return (
                f"URL count dropped from {last_good} to {count} "
                f"(more than {MAX_URL_DROP_PERCENT}%); if this is expected, edit "
                f"lastGoodUrlCount for the host in crawl-sources"
            )
    return None


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

def read_limited(resp, limit):
    chunks, total = [], 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise ValueError(f"response exceeds {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def fetch_bytes(opener, url, timeout=20, max_bytes=None):
    """GET with a hard size cap; transparently un-gzips .gz sitemaps."""
    limit = max_bytes or MAX_SITEMAP_BYTES
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with opener.open(req, timeout=timeout) as resp:
        data = read_limited(resp, limit)
    if data[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            data = read_limited(gz, limit)
    return data


def parse_xml(data):
    # Sitemaps never need a DTD. Refusing them closes the entity-expansion
    # ("billion laughs") hole in the stdlib parser without a new dependency.
    if b"<!DOCTYPE" in data or b"<!ENTITY" in data:
        raise ValueError("sitemap contains a DOCTYPE/ENTITY declaration; refusing to parse")
    return ET.fromstring(data)


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


def load_robots(opener, host):
    """
    RFC 9309 semantics: 4xx (except 429) means no restrictions; 5xx, 429 or a
    network failure means unreachable, which we treat as a host failure
    instead of crawling blind.
    """
    robots_url = f"https://{host}/robots.txt"
    req = urllib.request.Request(robots_url, headers={"User-Agent": USER_AGENT})
    rp = RobotsRules()
    try:
        with opener.open(req, timeout=10) as resp:
            content = read_limited(resp, 1024 * 1024).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if 400 <= e.code < 500 and e.code != 429:
            if e.code in (401, 403):
                logger.warning(
                    "%s returned HTTP %d -- treated as 'no robots rules', but a 401/403 here "
                    "often means a WAF is blocking this crawler", robots_url, e.code
                )
            else:
                logger.info("%s returned HTTP %d; assuming no robots restrictions", robots_url, e.code)
            rp.parse([])
            return rp
        raise RobotsUnavailable(f"{robots_url}: HTTP {e.code}")
    except RedirectBlocked as e:
        raise RobotsUnavailable(f"{robots_url}: blocked redirect {e}")
    except Exception as e:
        raise RobotsUnavailable(f"{robots_url}: {e}")

    rp.parse(content.splitlines())
    # Crawl-delay isn't enforceable by this architecture (reserved concurrency
    # caps parallel requests, not the spacing between them) -- surface it.
    delay = rp.crawl_delay(USER_AGENT) or rp.crawl_delay("*")
    if delay:
        logger.warning(
            "%s specifies Crawl-delay=%ss, which this pipeline does not enforce -- only concurrency is capped",
            robots_url, delay
        )
    return rp


def is_allowed_by_robots(rp, url):
    if rp is None:
        return True
    return rp.can_fetch(USER_AGENT, url)


# --------------------------------------------------------------------------
# Sitemap resolution
# --------------------------------------------------------------------------

def parse_sitemap_override(value):
    if not value:
        return []
    parts = value.split(",") if isinstance(value, str) else list(value)
    return [str(p).strip() for p in parts if str(p).strip()]


def probe_common_sitemaps(opener, host):
    for path in COMMON_SITEMAP_PATHS:
        candidate = f"https://{host}{path}"
        try:
            parse_xml(fetch_bytes(opener, candidate, timeout=15))
            return [candidate]
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500:
                continue
            raise
        except ET.ParseError:
            continue  # an HTML "soft 404" page, not a sitemap
    return []


def resolve_sitemap_roots(opener, host, source, rp, enabled_hosts):
    override = parse_sitemap_override((source or {}).get("sitemapUrls"))
    if override:
        return override
    if SITEMAP_URL and canonical_host(host_of(SITEMAP_URL)) == host:
        return [SITEMAP_URL]
    if rp is not None:
        listed = []
        for u in rp.site_maps() or []:
            if canonical_host(host_of(u)) in enabled_hosts:
                listed.append(u)
            else:
                logger.info("Ignoring robots.txt Sitemap entry on non-enabled host: %s", u)
        if listed:
            return listed
    return probe_common_sitemaps(opener, host)


def collect_sitemap_urls(opener, sitemap_url, enabled_hosts, ctx, depth=0):
    """
    Handles a flat <urlset> and a <sitemapindex>. ctx = {"seen": set,
    "fetched": int, "foreign": set}. Child sitemaps on hosts that are not
    enabled are skipped (and remembered as candidate hosts), not followed.
    Any fetch/parse error propagates: a PARTIAL sitemap must fail the host,
    otherwise reconciliation would treat the missing pages as deleted.
    """
    if sitemap_url in ctx["seen"]:
        return []
    if depth > MAX_SITEMAP_DEPTH:
        raise ValueError(f"sitemap nesting deeper than {MAX_SITEMAP_DEPTH}: {sitemap_url}")
    if ctx["fetched"] >= MAX_SITEMAP_FILES:
        raise ValueError(f"more than {MAX_SITEMAP_FILES} sitemap files for one host")
    ctx["seen"].add(sitemap_url)
    ctx["fetched"] += 1

    root = parse_xml(fetch_bytes(opener, sitemap_url))
    root_tag = root.tag.rsplit("}", 1)[-1]
    locs = [loc.text.strip() for loc in root.findall(".//sm:loc", SITEMAP_NS) if loc.text]

    if root_tag == "sitemapindex":
        logger.info("Sitemap index %s lists %d child sitemap(s)", sitemap_url, len(locs))
        urls = []
        for child in locs:
            child_host = canonical_host(host_of(child))
            if child_host not in enabled_hosts:
                ctx["foreign"].add(child_host)
                logger.info("Skipping child sitemap on non-enabled host %s: %s", child_host, child)
                continue
            urls.extend(collect_sitemap_urls(opener, child, enabled_hosts, ctx, depth + 1))
        return urls
    return locs


def get_urls_from_s3_seed(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return [line.strip() for line in body.splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Candidate host detection (links on the root page)
# --------------------------------------------------------------------------

class LinkHostCollector(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self._base = base_url
        self.hosts = set()

    def handle_starttag(self, tag, attrs):
        if tag not in ("a", "area"):
            return
        for key, value in attrs:
            if key == "href" and value:
                host = host_of(urljoin(self._base, value))
                if host:
                    self.hosts.add(host)


def scan_for_candidate_hosts(opener, known_hosts):
    """
    Best effort: a failure here never fails the run. New hosts under the
    domain are recorded as CANDIDATES (enabled="false") for human review.
    """
    try:
        html = fetch_bytes(opener, ROOT_URL, timeout=15, max_bytes=5 * 1024 * 1024)
        collector = LinkHostCollector(ROOT_URL)
        collector.feed(html.decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Candidate host scan of %s failed: %s", ROOT_URL, e)
        return 0
    added = 0
    for raw_host in sorted(collector.hosts):
        host = canonical_host(raw_host)
        if host in known_hosts or not under_domain(host):
            continue
        upsert_source(host, "false", "LINK_SCAN")
        known_hosts.add(host)
        added += 1
        logger.info("New CANDIDATE host found on %s: %s (enabled=false until approved)", ROOT_URL, host)
    return added


# --------------------------------------------------------------------------
# URL handling and filtering
# --------------------------------------------------------------------------

def normalize_url(url):
    """
    Canonicalizes a URL so equivalent variants collapse to one registry item:
    https, lowercase canonical host (aliases such as the apex domain are
    rewritten to their target), no fragment, no trailing slash, no tracking
    params, sorted query. Returns None for URLs that are not usable http(s).
    """
    try:
        parts = urlsplit(url.strip())
        host = (parts.hostname or "").lower().rstrip(".")
        port = parts.port
    except ValueError:
        return None
    if not host or parts.scheme not in ("http", "https"):
        return None
    host = canonical_host(host)
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    )
    return urlunsplit(("https", netloc, path, urlencode(query_pairs), ""))


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
                norm = normalize_url(raw) if raw else None
                if norm:
                    blocked.add(norm)
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


# --------------------------------------------------------------------------
# Registry upserts
# --------------------------------------------------------------------------

def upsert_registry_item(url, generation):
    """
    Creates the item on first sight, refreshes url/host/lastSeenGeneration/
    lastSeenAt on every later run -- but crawlEnabled, ingestionEnabled,
    and status are only set via if_not_exists(), so a human's manual
    edit in the DynamoDB console is never clobbered by a future run.
    """
    url_hash = sha256(url.encode("utf-8")).hexdigest()
    resp = registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #url = :url, "
            "#host = :host, "
            "lastSeenGeneration = :gen, "
            "lastSeenAt = :now, "
            "crawlEnabled = if_not_exists(crawlEnabled, :true_str), "
            "ingestionEnabled = if_not_exists(ingestionEnabled, :true_str), "
            "#status = if_not_exists(#status, :discovered)"
        ),
        ExpressionAttributeNames={"#url": "url", "#host": "host", "#status": "status"},
        ExpressionAttributeValues={
            ":url": url,
            ":host": host_of(url),
            ":gen": generation,
            ":now": int(time.time()),
            ":true_str": "true",
            ":discovered": "DISCOVERED",
        },
        # ALL_OLD (not UPDATED_OLD): returns the whole previous item with no
        # extra read cost, so we can see a prior status of DELETED reliably.
        ReturnValues="ALL_OLD",
    )
    revived = False
    if resp.get("Attributes", {}).get("status") == "DELETED":
        revived = revive_deleted_item(url_hash)
        if revived:
            logger.info("Revived previously DELETED URL: %s (urlHash=%s)", url, url_hash)
    return url_hash, revived


def revive_deleted_item(url_hash):
    """
    A DELETED item that Discovery sees again goes back to DISCOVERED with its
    contentHash cleared, so the Crawler treats the next fetch as new content
    and the page flows through Processing and Curation back into the KB.
    Conditional on status still being DELETED, so it cannot clobber a status
    that changed in the meantime.
    """
    try:
        registry_table.update_item(
            Key={"urlHash": url_hash},
            UpdateExpression="SET #status = :discovered REMOVE contentHash, curatedS3Key, deletedAt",
            ConditionExpression="#status = :deleted",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":discovered": "DISCOVERED", ":deleted": "DELETED"},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def upsert_all(urls, generation):
    succeeded, failed, revived = 0, [], 0
    with ThreadPoolExecutor(max_workers=UPSERT_CONCURRENCY) as pool:
        futures = {pool.submit(upsert_registry_item, u, generation): u for u in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                _, was_revived = future.result()
                succeeded += 1
                revived += 1 if was_revived else 0
            except Exception as e:
                logger.error("Failed to upsert registry item for %s: %s", url, e, exc_info=True)
                failed.append(url)
    return succeeded, failed, revived


def trigger_queueing_lambda(generation):
    lambda_client.invoke(
        FunctionName=QUEUEING_LAMBDA_NAME,
        InvocationType="Event",  # async -- fire and forget, don't block on it
        Payload=json.dumps({"generation": generation}).encode("utf-8"),
    )


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_discovery(generation):
    logger.info("Starting Discovery run: generation=%d root=%s", generation, ROOT_URL or "-")

    sources, aliases = load_sources()

    # Bootstrap: the root host is enabled by default; the apex domain becomes
    # an alias of it. if_not_exists means a human can later disable www or
    # re-point an alias without the bootstrap undoing it.
    if ROOT_HOST:
        canon_root = aliases.get(ROOT_HOST, ROOT_HOST)
        if canon_root not in sources:
            upsert_source(canon_root, "true", "BOOTSTRAP")
            sources[canon_root] = {"host": canon_root, "enabled": "true"}
        if canon_root.startswith("www."):
            apex = canon_root[4:]
            if apex not in aliases and apex not in sources:
                upsert_source(apex, "false", "BOOTSTRAP_ALIAS", alias_of=canon_root)
                aliases[apex] = canon_root

    HOST_ALIASES.clear()
    HOST_ALIASES.update(aliases)

    enabled_hosts = sorted(
        h for h, item in sources.items() if str(item.get("enabled", "false")).lower() == "true"
    )
    if not enabled_hosts:
        raise ValueError("No enabled hosts in crawl-sources. Set ROOT_URL or enable a host.")
    enabled_set = set(enabled_hosts)
    logger.info("Enabled hosts (%d): %s", len(enabled_hosts), ", ".join(enabled_hosts))

    opener = urllib.request.build_opener(ScopedRedirectHandler(enabled_set | set(aliases)))

    if ROOT_URL:
        scan_for_candidate_hosts(opener, set(sources) | set(aliases))

    # --- per-host sitemap discovery -------------------------------------
    host_errors = {}
    robots = {}
    raw_urls = []
    foreign_hosts = set()

    for host in enabled_hosts:
        try:
            rp = load_robots(opener, host) if RESPECT_ROBOTS_TXT else None
            roots = resolve_sitemap_roots(opener, host, sources[host], rp, enabled_set)
            if not roots:
                raise ValueError(
                    "no sitemap found (tried override, SITEMAP_URL, robots.txt, common paths); "
                    "set sitemapUrls on the host in crawl-sources or use the seed file"
                )
            ctx = {"seen": set(), "fetched": 0, "foreign": set()}
            host_raw = []
            for root_url in roots:
                host_raw.extend(collect_sitemap_urls(opener, root_url, enabled_set, ctx))
            robots[host] = rp
            raw_urls.extend(host_raw)
            foreign_hosts |= ctx["foreign"]
        except Exception as e:
            host_errors[host] = f"{type(e).__name__}: {e}"
            logger.error("DISCOVERY_HOST_FAILED host=%s: %s", host, e, exc_info=True)

    seed_failed = False
    if SEED_BUCKET and SEED_KEY:
        try:
            raw_urls.extend(get_urls_from_s3_seed(SEED_BUCKET, SEED_KEY))
        except Exception as e:
            seed_failed = True
            logger.error("DISCOVERY_SEED_FAILED s3://%s/%s: %s", SEED_BUCKET, SEED_KEY, e, exc_info=True)

    # --- normalize, dedupe, scope to enabled hosts ----------------------
    by_host = {}
    seen = set()
    invalid = out_of_scope = 0
    for raw in raw_urls:
        norm = normalize_url(raw)
        if norm is None:
            invalid += 1
            continue
        if norm in seen:
            continue
        seen.add(norm)
        host = host_of(norm)
        if host not in enabled_set:
            out_of_scope += 1
            if under_domain(host):
                foreign_hosts.add(host)
            continue
        by_host.setdefault(host, []).append(norm)

    for host in sorted(foreign_hosts):
        if host and under_domain(host) and host not in sources and host not in aliases:
            try:
                upsert_source(host, "false", "SITEMAP_URL")
                logger.info("New CANDIDATE host referenced by a sitemap: %s (enabled=false until approved)", host)
            except Exception as e:
                logger.warning("Could not record candidate host %s: %s", host, e)

    # --- per-host health + sanity guard ---------------------------------
    ok_hosts = []
    host_summary = {}
    for host in enabled_hosts:
        count = len(by_host.get(host, []))
        problem = host_errors.get(host) or check_url_count(
            count, as_int(sources[host].get("lastGoodUrlCount"))
        )
        if problem:
            host_errors[host] = problem
            logger.error("DISCOVERY_HOST_FAILED host=%s urls=%d: %s", host, count, problem)
            record_source_result(host, generation, error=problem)
            host_summary[host] = {"status": "ERROR", "urls": count, "error": problem}
        else:
            ok_hosts.append(host)
            record_source_result(host, generation, url_count=count)
            host_summary[host] = {"status": "OK", "urls": count}

    # --- policy filters (only for hosts that came through healthy) ------
    blocked_urls = load_blocked_urls()
    robots_blocked = blocklisted = excluded = not_allowlisted = 0
    candidate_urls = []
    for host in ok_hosts:
        rp = robots.get(host)
        for u in by_host.get(host, []):
            if not is_allowed_by_robots(rp, u):
                robots_blocked += 1
            elif u in blocked_urls:
                blocklisted += 1
            elif is_excluded(u):
                excluded += 1
            elif not is_allowlisted(u):
                not_allowlisted += 1
            else:
                candidate_urls.append(u)

    succeeded, failed, revived = upsert_all(candidate_urls, generation)

    complete = not host_errors and not seed_failed and not failed
    logger.info(
        "Discovery run finished: generation=%d complete=%s rawUrls=%d invalid=%d outOfScope=%d "
        "robotsBlocked=%d blocklistTableBlocked=%d excluded=%d notAllowlisted=%d upserted=%d revived=%d failed=%d",
        generation, complete, len(raw_urls), invalid, out_of_scope, robots_blocked,
        blocklisted, excluded, not_allowlisted, succeeded, revived, len(failed)
    )
    if not complete:
        logger.error(
            "DISCOVERY_INCOMPLETE generation=%d failedHosts=%s seedFailed=%s upsertFailures=%d -- "
            "lastDiscoveredGeneration NOT advanced, so no deletions will be reconciled this cycle",
            generation, sorted(host_errors), seed_failed, len(failed)
        )
    if failed:
        logger.error("URLs that failed to upsert this run (generation=%d): %s", generation, failed)

    mark_discovery_complete(generation, complete)
    trigger_queueing_lambda(generation)

    return {
        "generation": generation,
        "complete": complete,
        "hosts": host_summary,
        "rawUrls": len(raw_urls),
        "invalid": invalid,
        "outOfScope": out_of_scope,
        "robotsBlocked": robots_blocked,
        "blocklistTableBlocked": blocklisted,
        "excluded": excluded,
        "notAllowlisted": not_allowlisted,
        "upserted": succeeded,
        "revived": revived,
        "failed": len(failed),
    }


def handler(event, context):
    generation = get_next_generation()
    try:
        return run_discovery(generation)
    except Exception:
        logger.error("DISCOVERY_FAILED generation=%d", generation, exc_info=True)
        release_pipeline()
        raise
