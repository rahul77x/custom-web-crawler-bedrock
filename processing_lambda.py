"""
Processing Lambda
------------------
Triggered by SQS (a `processing-queue` fed directly by an S3 Event
Notification on the raw bucket's ObjectCreated events -- no code needed
for that hookup, it's configured in the S3 console under Properties ->
Event notifications, targeting the processing-queue).

For each raw HTML object: strips script/style/nav/header/footer/aside
content down to plain text using only the standard library, validates
it's long enough to be useful, and -- if it passes -- writes one clean
text file plus a Bedrock metadata sidecar to the staging bucket.
Chunking is deliberately NOT done here; Bedrock's ingestion job applies
its own configured chunking strategy against these clean text files.

Required environment variables:
  STAGING_BUCKET       - S3 bucket for cleaned, validated text
  REGISTRY_TABLE_NAME  - the content registry the crawler Lambda writes to
Optional:
  MIN_TEXT_LENGTH      - minimum characters of extracted text to pass
                          validation (default 200)
  BOILERPLATE_PATTERNS - comma-separated regex patterns; any paragraph
                          matching one is dropped entirely. Manually
                          curated -- fill this in once you've seen real
                          recurring boilerplate (cookie banners, legal
                          disclaimers) in actual processed output, e.g.
                          "This page is for informational purposes.*"

Configure the SQS trigger in the console with:
  - Batch size: 1-5
  - Report batch item failures: ON, function response type
    "ReportBatchItemFailures"

IMPORTANT: once this Lambda is live, add processing-queue's URL to the
ingestion-trigger Lambda's MONITORED_QUEUE_URLS env var (comma-separated
alongside crawl-queue). Without that, the trigger only waits for
crawling to finish, not processing -- it could sync a batch while pages
are still being cleaned.
"""

import hashlib
import json
import logging
import os
import re
import time
import urllib.parse
from html.parser import HTMLParser

import boto3

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

STAGING_BUCKET = os.environ["STAGING_BUCKET"]
REGISTRY_TABLE_NAME = os.environ["REGISTRY_TABLE_NAME"]
MIN_TEXT_LENGTH = int(os.environ.get("MIN_TEXT_LENGTH", "200"))

registry_table = dynamodb.Table(REGISTRY_TABLE_NAME)

# Tags whose content should never end up in the extracted text -- this is
# a heuristic, not a full boilerplate-removal algorithm, but it covers
# the parts of a page that are reliably never article/page content.
SKIP_TAGS = {"script", "style", "nav", "header", "footer", "aside", "noscript", "svg", "form"}

# Block-level tags mark a paragraph/section boundary -- text is only
# joined with a blank line AT these boundaries. Everything else (a, b,
# span, em, etc.) is treated as inline: its text is joined with spaces
# to whatever came before it in the same block, so one sentence that
# happens to contain a link doesn't get fragmented across lines. This
# matters more for semantic chunking specifically than for fixed-size
# chunking, since semantic chunking groups sentences by meaning and a
# broken sentence boundary confuses that grouping before it even starts.
BLOCK_TAGS = {
    "p", "div", "section", "article", "li", "ul", "ol",
    "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre",
    "br", "hr",
    # nav/header/footer/aside/form are block-level too, even though their
    # CONTENT is dropped entirely via SKIP_TAGS above -- listing them
    # here just means whatever comes right after one of them starts a
    # fresh paragraph rather than running on from whatever preceded it.
    "nav", "header", "footer", "aside", "form",
}

HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}

# Regex patterns matched against each completed paragraph; any match drops
# that whole paragraph from the output. This is a manually-curated list,
# not automatic detection -- it only catches boilerplate you've actually
# identified from real pages, e.g. a recurring legal disclaimer or cookie
# banner line that shows up near-verbatim across many pages and would
# otherwise become a near-duplicate chunk under every single one of them.
# Genuinely automatic cross-page boilerplate detection (tracking which
# paragraphs recur across many distinct URLs) needs a frequency table this
# single-page Lambda doesn't have -- a legitimate future upgrade once
# you've seen which boilerplate is actually showing up, not before.
BOILERPLATE_PATTERNS = [
    re.compile(p.strip())
    for p in os.environ.get("BOILERPLATE_PATTERNS", "").split(",")
    if p.strip()
]


def is_boilerplate(paragraph):
    return any(rx.search(paragraph) for rx in BOILERPLATE_PATTERNS)


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._skip_depth = 0
        self._paragraphs = []   # completed paragraphs, block-boundary separated
        self._current = []      # inline fragments accumulating for the open block
        self._title_pieces = []
        self._in_title = False
        self._heading_level = None  # set while inside h1-h6, else None
        self.boilerplate_dropped = 0  # populated by get_text()

        # Table state preservation (converts HTML tables to Markdown tables
        # so Bedrock semantic chunking retains row-column relationships)
        self._in_table = False
        self._table_rows = []
        self._current_row = []
        self._current_cell = []
        self._in_cell = False

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self._in_title = True
        if tag in SKIP_TAGS:
            self._skip_depth += 1
        if tag in BLOCK_TAGS:
            self._flush_paragraph()
        if tag in HEADING_TAGS:
            self._heading_level = int(tag[1])
        if tag == "table":
            self._flush_paragraph()
            self._in_table = True
            self._table_rows = []
        elif tag == "tr" and self._in_table:
            self._current_row = []
        elif tag in ("td", "th") and self._in_table:
            self._in_cell = True
            self._current_cell = []

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        if tag in SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag in BLOCK_TAGS:
            # Flush BEFORE clearing heading_level, so the paragraph that
            # was actually collected while inside e.g. <h2> still gets
            # tagged as a heading when it's written out.
            self._flush_paragraph()
        if tag in HEADING_TAGS:
            self._heading_level = None
        if tag in ("td", "th") and self._in_table:
            self._in_cell = False
            cell_text = " ".join(self._current_cell).strip().replace("|", "\\|")
            self._current_row.append(cell_text)
            self._current_cell = []
        elif tag == "tr" and self._in_table:
            if self._current_row:
                row_str = "| " + " | ".join(self._current_row) + " |"
                self._table_rows.append(row_str)
                if len(self._table_rows) == 1:
                    sep = "| " + " | ".join(["---"] * len(self._current_row)) + " |"
                    self._table_rows.append(sep)
            self._current_row = []
        elif tag == "table" and self._in_table:
            self._flush_table()

    def handle_data(self, data):
        if self._in_title:
            self._title_pieces.append(data.strip())
            return
        if self._skip_depth == 0:
            text = " ".join(data.split())
            if text:
                if self._in_table and self._in_cell:
                    self._current_cell.append(text)
                elif not self._in_table:
                    self._current.append(text)

    def _flush_paragraph(self):
        if self._current:
            text = " ".join(self._current).strip()
            if self._heading_level:
                text = ("#" * min(self._heading_level, 6)) + " " + text
            self._paragraphs.append(text)
            self._current = []

    def _flush_table(self):
        if self._table_rows:
            table_md = "\n".join(self._table_rows)
            self._paragraphs.append(table_md)
            self._table_rows = []
        self._in_table = False

    def get_text(self):
        self._flush_paragraph()
        self._flush_table()
        kept = []
        dropped = 0
        for p in self._paragraphs:
            if not p:
                continue
            if is_boilerplate(p):
                dropped += 1
                continue
            kept.append(p)
        self.boilerplate_dropped = dropped
        # Blank-line separated paragraphs give the chunker clear block
        # boundaries without ever splitting a single sentence in two.
        return "\n\n".join(kept)

    def get_title(self):
        return " ".join(t for t in self._title_pieces if t).strip()


def extract_text(html_bytes):
    parser = TextExtractor()
    # errors="replace" -- a malformed byte in the page shouldn't fail the
    # whole extraction, just replace the offending character.
    parser.feed(html_bytes.decode("utf-8", errors="replace"))
    text = parser.get_text()  # must be called before boilerplate_dropped is valid
    return text, parser.get_title(), parser.boilerplate_dropped


def get_registry_item(url_hash):
    resp = registry_table.get_item(Key={"urlHash": url_hash})
    return resp.get("Item")


def mark_processed(url_hash, processed_key, char_count, boilerplate_dropped):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression=(
            "SET #status = :processed, processedS3Key = :key, "
            "processedCharCount = :count, validationNotes = :notes, "
            "boilerplateParagraphsDropped = :dropped, processedAt = :now"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":processed": "PROCESSED",
            ":key": processed_key,
            ":count": char_count,
            ":notes": "passed",
            ":dropped": boilerplate_dropped,
            ":now": int(time.time()),
        },
    )


def mark_quarantined(url_hash, reason):
    registry_table.update_item(
        Key={"urlHash": url_hash},
        UpdateExpression="SET #status = :quarantined, validationNotes = :notes",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":quarantined": "QUARANTINED", ":notes": reason},
    )


def process_s3_record(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    html_bytes = obj["Body"].read()
    source_url = obj.get("Metadata", {}).get("source-url")

    if not source_url:
        # The crawler always sets this metadata -- if it's missing, this
        # object either wasn't written by the crawler or something is
        # wrong upstream. Either way we can't map it to a registry item.
        raise RuntimeError(f"No source-url metadata on s3://{bucket}/{key}")

    url_hash = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    text, page_title, boilerplate_dropped = extract_text(html_bytes)

    if len(text) < MIN_TEXT_LENGTH:
        mark_quarantined(url_hash, f"extracted_text_too_short: {len(text)} chars")
        logger.warning(
            "Quarantined (too short: %d chars < %d): url=%s urlHash=%s s3Key=s3://%s/%s",
            len(text), MIN_TEXT_LENGTH, source_url, url_hash, bucket, key
        )
        return

    # Deliberately NOT date-prefixed, unlike the raw bucket. One stable
    # key per URL means a content change genuinely OVERWRITES the
    # previous version here (and, via Curation's copy, in kb-source too)
    # instead of accumulating a new object every time the page changes --
    # which would otherwise leave stale, superseded versions sitting in
    # the knowledge base alongside the current one forever, since nothing
    # would ever delete the old key. Staging bucket versioning already
    # gives you historical rollback if you ever need an older version,
    # without this correctness problem in the live/current namespace.
    processed_key = f"processed/{url_hash}.txt"
    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=processed_key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )

    # Update DynamoDB status to PROCESSED BEFORE putting the metadata sidecar to S3.
    # Because S3 ObjectCreated on metadata_key triggers Curation Lambda, setting
    # status first guarantees Curation sees status=PROCESSED rather than CRAWLED.
    mark_processed(url_hash, processed_key, len(text), boilerplate_dropped)

    # Bedrock metadata sidecar -- same key plus ".metadata.json", same
    # folder. Curation triggers on this file and copies both files as a pair.
    metadata_key = f"{processed_key}.metadata.json"
    metadata_body = {
        "metadataAttributes": {
            "sourceUrl": source_url,
            "urlHash": url_hash,
            "pageTitle": page_title or source_url,
        }
    }
    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata_body).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Processed: url=%s urlHash=%s -> s3://%s/%s (chars=%d, boilerplate_dropped=%d)",
        source_url, url_hash, STAGING_BUCKET, processed_key, len(text), boilerplate_dropped
    )


def handler(event, context):
    batch_item_failures = []

    for record in event["Records"]:
        message_id = record["messageId"]
        target_key = "unknown"
        try:
            # The SQS message body here is S3's own event notification
            # JSON, not our custom {"url": ...} shape -- S3 was
            # configured to send events straight to this queue.
            s3_event = json.loads(record["body"])
            for s3_record in s3_event.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                target_key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])
                process_s3_record(bucket, target_key)
        except Exception as e:
            logger.error(
                "Failed processing SQS messageId=%s s3Key=%s: %s",
                message_id, target_key, e, exc_info=True
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
