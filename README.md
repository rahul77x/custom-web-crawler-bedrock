# Custom Web Crawler for Amazon Bedrock Knowledge Bases

An enterprise-grade, event-driven, serverless web crawler and document processing pipeline designed to feed structured website content into Amazon Bedrock Knowledge Bases for generative AI and RAG applications.

---

## Architecture Overview

```
[EventBridge Cron]
        │
        ▼
[Discovery Lambda] ──(async invoke)──► [Queueing Lambda]
        │                                      │
   (DynamoDB                              (SQS crawl-queue)
    Registry)                                  │
                                               ▼
                                       [Crawler Lambda]
                                               │
                                         (S3 raw-bucket)
                                               │ (S3 ObjectCreated)
                                               ▼
                                      [Processing Lambda]
                                               │
                                      (S3 staging-bucket)
                                               │ (S3 ObjectCreated)
                                               ▼
                                      [Curation Lambda]
                                               │
                                      (S3 kb-source-bucket)
                                               ▲
                                               │ (Deletion Reconciliation)
[Ingestion-Trigger Lambda] ────────────────────┴──► [Bedrock Knowledge Base Sync]
 (Monitors 3 queues & quiescence)
```

### The 6 Lambda Functions

1. **Discovery Lambda (`discovery_lambda.py`)**
   - Scheduled via EventBridge (nightly cron).
   - Ingests XML sitemaps (including nested `<sitemapindex>`) or static S3 seed lists.
   - Normalizes URLs, respects `robots.txt`, and evaluates allowlist and exclusion regexes.
   - Upserts entries into DynamoDB `content-registry` using `if_not_exists` to preserve manual administrative flags.
   - Asynchronously triggers Queueing Lambda.

2. **Queueing Lambda (`queueing_lambda.py`)**
   - Queries DynamoDB GSI `crawl-enabled-index` for items where `crawlEnabled="true"`.
   - Streams messages in batches of 10 to `crawl-queue` without unbounded in-memory buffering.
   - Transitions `pipelineStatus` in `pipeline-state` to `CRAWLING`.

3. **Crawler Lambda (`crawler_lambda.py`)**
   - Triggered by SQS `crawl-queue` (with reserved concurrency for web server politeness).
   - Fetches HTML using custom user-agent with timeout protection.
   - Performs SHA-256 change detection: unchanged pages touch the registry and skip S3 rewrites.
   - Handles permanent HTTP errors (`404`, `410`) without causing SQS dead-letter storms.
   - Writes new/modified HTML to `raw-bucket` with source URL metadata.

4. **Processing Lambda (`processing_lambda.py`)**
   - Triggered by SQS (`processing-queue` fed by S3 ObjectCreated on raw HTML).
   - Drops boilerplate tags (`<script>`, `<style>`, `<nav>`, `<footer>`, `<aside>`, `<form>`).
   - Converts HTML financial/comparison tables to clean Markdown tables (`| Col1 | Col2 |`) to preserve row-column semantic relationships for Bedrock chunking.
   - Writes clean text and companion Bedrock metadata sidecar (`{key}.metadata.json`) to `staging-bucket`.
   - Updates DynamoDB status to `PROCESSED` before metadata upload to avoid race conditions.

5. **Curation Lambda (`curation_lambda.py`)**
   - Triggered by SQS (`curation-queue` on staging `.metadata.json`).
   - Two approval gates: manual toggle `ingestionEnabled == "true"` and compliance allow/exclusion filters.
   - Checks against optional DynamoDB `url-blocklist` table.
   - Copies approved document pairs (text + metadata) into `kb-source-bucket`.

6. **Ingestion-Trigger Lambda (`ingestion_trigger_lambda.py`)**
   - Runs periodically via EventBridge (every 10–15 min).
   - Monitors queue depth across all 3 queues (`crawl`, `processing`, `curation`).
   - Requires consecutive empty checks to confirm pipeline quiescence.
   - Enforces cooldown windows and guards against concurrent sync jobs.
   - Reconciles deletions against `lastDiscoveredGeneration`, safely deleting stale content and Bedrock companion `.metadata.json` files from `kb-source-bucket`.
   - Starts Bedrock Knowledge Base ingestion sync job.

---

## Unit Testing

The repository includes a comprehensive unit test suite with 40 test cases built using Python's standard library `unittest` and `unittest.mock` (zero external dependencies required):

```powershell
python -m unittest discover -s tests -v
```

### Coverage
- **Processing**: Table-to-Markdown conversion, boilerplate stripping, length validation, SQS batch partial failures.
- **Crawler**: Redirect inspection, 404/410 handling, SHA-256 change detection, SQS partial failures.
- **Curation**: In-flight status retries, blocklist table lookups, suppression gates, dual-file S3 promotion.
- **Queueing**: GSI pagination streaming, batch formatting, pipeline status state transitions.
- **Discovery**: URL normalization, allow/exclude filters, XML sitemap parsing, robots.txt resilience.
- **Ingestion-Trigger**: 3-queue depth breakdown, quiescence counter, cooldown locks, deletion reconciliation.

---

## Production Readiness

Refer to [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md) for environment variable configurations, CloudWatch alarm setups, and step-by-step smoke testing procedures.
