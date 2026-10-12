# Website Crawler to Bedrock Knowledge Base Pipeline: Production Checklist

## Pipeline Architecture Overview
- **Discovery Lambda**: EventBridge Scheduler (nightly cron). Sitemaps/seed discovery, URL normalization, robots.txt, `if_not_exists` registry upsert, async invokes Queueing.
- **Queueing Lambda**: Queries GSI `crawl-enabled-index` for `crawlEnabled="true"`. Streams batches of 10 to `crawl-queue`. Updates `pipelineStatus`.
- **Crawler Lambda**: SQS trigger (`crawl-queue`). urllib fetch (15s timeout, User-Agent `internal-kb-crawler/1.0`). SHA-256 change detection. Scoped `update_item` only. Writes raw HTML snapshot to S3.
- **Processing Lambda**: SQS trigger (`processing-queue` via S3 `ObjectCreated` on `raw/*.html`). Drops nav/footer/script/boilerplate. Converts HTML tables to Markdown tables. Generates Bedrock sidecar metadata. Writes to staging bucket.
- **Curation Lambda**: SQS trigger (`curation-queue` via S3 `ObjectCreated` on staging `processed/*.metadata.json`). Approval gates: `ingestionEnabled=="true"`, exclusion denylist. Copies pair to `kb-source`.
- **Ingestion-Trigger Lambda**: EventBridge Scheduler (every 10-15 min). Checks quiescence across all 3 queues. Protects against race conditions via `pipelineStatus`. Reconciles deletions against `lastDiscoveredGeneration`. Starts Bedrock Knowledge Base sync.

---

## 1. Code Fixes & Remediation Completed

- [x] **Fix 1: Curation Silent Drop Race Condition Eliminated**
  - **Files**: `processing_lambda.py`, `curation_lambda.py`
  - **Resolution**:
    - In `processing_lambda.py`, `mark_processed()` updates DynamoDB status to `PROCESSED` **before** uploading `.metadata.json` to S3.
    - In `curation_lambda.py`, if a record is still in an in-flight status (`DISCOVERED`, `CRAWLED`, `None`), it raises an error so SQS visibility timeout retries it instead of silently dropping the message.
  - **Status**: **VERIFIED**

- [x] **Fix 2: Deletion Reconciliation Orphaned Metadata in S3 Resolved**
  - **Files**: `ingestion_trigger_lambda.py`
  - **Resolution**: Updated `mark_deleted()` to delete both the content object (`curatedS3Key`) and its Bedrock companion sidecar (`{curatedS3Key}.metadata.json`) from `kb-source`.
  - **Status**: **VERIFIED**

- [x] **Fix 3: Pipeline Run State & Knowledge Base Wipeout Race Condition Prevented**
  - **Files**: `discovery_lambda.py`, `queueing_lambda.py`, `ingestion_trigger_lambda.py`
  - **Resolution**:
    - Lifecycle coordination via `pipelineStatus`: `DISCOVERING` -> `QUEUEING` -> `CRAWLING` -> `INGESTING` -> `IDLE`.
    - `discovery_lambda.py`: Stamped `lastDiscoveredGeneration` only after all URLs are discovered and upserted.
    - `ingestion_trigger_lambda.py`: Skips execution during `DISCOVERING` and `QUEUEING` (when queues are naturally empty before crawl dispatch). Reconciles deletions strictly against `lastDiscoveredGeneration`.
  - **Status**: **VERIFIED**

- [x] **Fix 4: WAF-Resilient Robots.txt Fetching**
  - **Files**: `discovery_lambda.py`
  - **Resolution**: Replaced `rp.read()` with explicit `urllib.request.urlopen` sending the crawler's configured `USER_AGENT = "internal-kb-crawler/1.0"`, 10-second timeout, and proper HTTP 404 vs 403 handling.
  - **Status**: **VERIFIED**

- [x] **Fix 5: Financial Table Markdown Preservation for Semantic Chunking**
  - **Files**: `processing_lambda.py`
  - **Resolution**: Removed `table`, `tr`, `td`, `th` from block tag paragraph-splitting. Implemented Markdown table conversion inside `TextExtractor` (`| Col1 | Col2 |`) so Bedrock semantic chunking retains row-column relationships and rate comparisons intact.
  - **Status**: **VERIFIED**

- [x] **Fix 6: Memory-Efficient Streaming in Queueing Lambda**
  - **Files**: `queueing_lambda.py`
  - **Resolution**: Refactored `stream_and_enqueue_urls()` to dispatch batches of 10 to SQS directly inside the GSI query pagination loop instead of accumulating the entire table in a single in-memory list.
  - **Status**: **VERIFIED**

- [x] **Fix 7: Crawler Redirect Inspection & HTTP 404 Dead-Letter Storms Handled**
  - **Files**: `crawler_lambda.py`
  - **Resolution**:
    - Caught permanent HTTP errors (`404 Not Found`, `410 Gone`) and marked `FAILED_HTTP_{code}` in DynamoDB without failing the SQS batch, eliminating repeated 5x retries into DLQ.
    - Inspected `resp.geturl()` to log redirects and attach `final-url` S3 metadata.
  - **Status**: **VERIFIED**

- [x] **Fix 8: Bedrock Ingestion Job Status Verification & Alerts**
  - **Files**: `ingestion_trigger_lambda.py`
  - **Resolution**: Added `check_recent_job_status()` to inspect the latest Bedrock sync job, logging completion statistics and alerting on any `FAILED` job status with failure reasons.
  - **Status**: **VERIFIED**

- [x] **Fix 9: Banking Section Allowlisting (Defense-in-Depth)**
  - **Files**: `discovery_lambda.py`, `curation_lambda.py`
  - **Resolution**:
    - `discovery_lambda.py`: Added `ALLOW_URL_PATTERNS`. Only URLs matching approved banking sections (e.g. `/personal-banking,/loans,/cards,/deposits,/rates,/faqs`) are discovered and upserted, preventing thousands of non-banking pages from polluting the registry or consuming crawl capacity.
    - `curation_lambda.py`: Added `CURATION_ALLOW_PATTERNS` as a secondary gate. Any processed page not matching the approved banking sections is marked `REJECTED` and blocked from `kb-source`.
  - **Status**: **VERIFIED**

- [x] **Fix 10: Dedicated DynamoDB URL Blocklist Table Support**
  - **Files**: `discovery_lambda.py`, `curation_lambda.py`
  - **Resolution**:
    - Added support for `BLOCKLIST_TABLE_NAME` (e.g., `url-blocklist`, Partition Key: `url` String).
    - `discovery_lambda.py`: Scans the blocklist table once at startup (< 1 RCU, ~15ms) into an in-memory set, dropping blocked URLs before they ever enter `content-registry`.
    - `curation_lambda.py`: Executes a point lookup (`get_item`) against `url-blocklist`. If found, marks `status = REJECTED` and halts promotion to `kb-source`.
    - Enables compliance officers to preemptively block exact full URLs in DynamoDB console with plain URLs (no SHA-256 calculation or code deployment needed).
  - **Status**: **VERIFIED**

- [x] **Fix 11: Production-Grade CloudWatch Logging Across All 6 Lambdas**
  - **Files**: All 6 Lambda functions (`discovery_lambda.py`, `queueing_lambda.py`, `crawler_lambda.py`, `processing_lambda.py`, `curation_lambda.py`, `ingestion_trigger_lambda.py`)
  - **Resolution**:
    - Replaced all ad-hoc `print()` statements with standard `logging.getLogger()` configured at `INFO` level.
    - Added contextual log level separation (`INFO`, `WARNING`, `ERROR`, `DEBUG`).
    - Dynamic runtime configuration via `LOG_LEVEL` environment variable (defaults to `INFO`, toggleable to `DEBUG` in console without code changes).
    - Fixed error swallowing in all SQS handlers: all exceptions now log full stack traces using `exc_info=True`.
    - Added high-cardinality correlation fields (`messageId`, `url`, `urlHash`, `s3Key`, `batchId`, `generation`) to every log line.
    - Added queue backlog breakdown (`visible` vs `in-flight` per queue) in `ingestion_trigger_lambda.py` and silenced 24/7 idle heartbeat to `DEBUG`.
    - Added fetch latency tracking (`duration_ms`) in `crawler_lambda.py` and moved unchanged page hits to `DEBUG` to eliminate nightly log flood.
  - **Status**: **VERIFIED**

---

## 2. AWS Console Configuration & Environment Checklist

### Discovery Lambda
- [ ] **Trigger**: EventBridge Scheduler (Nightly cron, e.g., `cron(0 1 * * ? *)`)
- [ ] **Runtime**: Python 3.12 (No layers)
- [ ] **Timeout**: 5 to 15 minutes (depending on sitemap size)
- [ ] **Memory**: 512 MB – 1024 MB
- [ ] **Environment Variables**:
  - `STATE_TABLE_NAME`: `pipeline-state`
  - `REGISTRY_TABLE_NAME`: `content-registry`
  - `QUEUEING_LAMBDA_NAME`: Name or ARN of Queueing Lambda
  - `SITEMAP_URL`: e.g. `https://<domain>/sitemap.xml` (or `SEED_BUCKET` / `SEED_KEY`)
  - `ALLOW_URL_PATTERNS`: Optional comma-separated regexes defining allowed banking sections (e.g. `/personal-banking,/loans,/cards,/deposits,/rates,/faqs`)
  - `EXCLUDE_URL_PATTERNS`: Optional comma-separated regexes
  - `BLOCKLIST_TABLE_NAME`: Optional DynamoDB table name (e.g. `url-blocklist`, PK: `url` String) holding exact URLs to block
  - `UPSERT_CONCURRENCY`: `20`
  - `RESPECT_ROBOTS_TXT`: `true`

### Queueing Lambda
- [ ] **Trigger**: Asynchronously invoked by Discovery (No schedule)
- [ ] **Runtime**: Python 3.12
- [ ] **Timeout**: 5 minutes
- [ ] **Memory**: 512 MB
- [ ] **Environment Variables**:
  - `REGISTRY_TABLE_NAME`: `content-registry`
  - `CRAWL_ENABLED_INDEX_NAME`: `crawl-enabled-index`
  - `CRAWL_QUEUE_URL`: URL of `crawl-queue`
  - `STATE_TABLE_NAME`: `pipeline-state`

### Crawler Lambda
- [ ] **Trigger**: SQS `crawl-queue`
  - **Batch size**: 1 – 5
  - **Report batch item failures**: **ENABLED** (`ReportBatchItemFailures`)
- [ ] **Timeout**: 60 seconds
- [ ] **SQS Visibility Timeout**: $\ge 360$ seconds (6x Lambda timeout rule)
- [ ] **Reserved Concurrency**: Cap to 5 – 10 (rate limit against bank web server)
- [ ] **Environment Variables**:
  - `RAW_BUCKET`: Name of raw S3 bucket
  - `REGISTRY_TABLE_NAME`: `content-registry`

### Processing Lambda
- [ ] **Trigger**: SQS `processing-queue` (Fed by S3 ObjectCreated on raw bucket: Prefix `raw/`, Suffix `.html`)
  - **Batch size**: 1 – 5
  - **Report batch item failures**: **ENABLED**
- [ ] **Timeout**: 60 seconds
- [ ] **SQS Visibility Timeout**: $\ge 360$ seconds
- [ ] **Environment Variables**:
  - `STAGING_BUCKET`: Name of staging S3 bucket
  - `REGISTRY_TABLE_NAME`: `content-registry`
  - `MIN_TEXT_LENGTH`: `200`
  - `BOILERPLATE_PATTERNS`: Optional comma-separated regexes

### Curation Lambda
- [ ] **Trigger**: SQS `curation-queue` (Fed by S3 ObjectCreated on staging bucket: Prefix `processed/`, Suffix `.metadata.json`)
  - **Batch size**: 1 – 5
  - **Report batch item failures**: **ENABLED**
- [ ] **Timeout**: 60 seconds
- [ ] **SQS Visibility Timeout**: $\ge 360$ seconds
- [ ] **Environment Variables**:
  - `STAGING_BUCKET`: Name of staging S3 bucket
  - `KB_SOURCE_BUCKET`: Name of kb-source S3 bucket
  - `REGISTRY_TABLE_NAME`: `content-registry`
  - `BLOCKLIST_TABLE_NAME`: Optional DynamoDB table name (e.g. `url-blocklist`, PK: `url` String) holding exact URLs to block
  - `CURATION_ALLOW_PATTERNS`: Optional comma-separated regexes defining allowed banking sections (e.g. `/personal-banking,/loans,/cards,/deposits,/rates,/faqs`)
  - `CURATION_EXCLUDE_PATTERNS`: Optional comma-separated regexes

### Ingestion-Trigger Lambda
- [ ] **Trigger**: EventBridge Scheduler (Rate rule: every 10–15 minutes)
- [ ] **Timeout**: 3 minutes
- [ ] **Environment Variables**:
  - `MONITORED_QUEUE_URLS`: Comma-separated list of all 3 queues:
    `<crawl-queue-url>,<processing-queue-url>,<curation-queue-url>`
  - `STATE_TABLE_NAME`: `pipeline-state`
  - `REGISTRY_TABLE_NAME`: `content-registry`
  - `KB_SOURCE_BUCKET`: Name of kb-source S3 bucket
  - `KNOWLEDGE_BASE_ID`: Bedrock Knowledge Base ID
  - `DATA_SOURCE_ID`: Bedrock Knowledge Base Data Source ID
  - `REQUIRED_EMPTY_CHECKS`: `2`
  - `MIN_HOURS_BETWEEN_INGESTIONS`: `6`

---

## 3. Automated Unit Test Suite
- [x] **Comprehensive Test Suite Implemented (`tests/`)**:
  - **Execution**: `python -m unittest discover -s tests -v` (40 tests across all 6 Lambdas, 0 external dependencies).
  - [`tests/test_processing_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_processing_lambda.py): Table-to-Markdown conversion, boilerplate tag/regex stripping, min-length validation, S3 sidecar creation ordering, SQS partial batch failures.
  - [`tests/test_crawler_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_crawler_lambda.py): Fetching, redirect logging, 404/410 handling without DLQ storm, SHA-256 change detection, 5xx SQS batch item failures.
  - [`tests/test_curation_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_curation_lambda.py): In-flight status retry protection (`DISCOVERED`/`CRAWLED`), terminal state skipping, `ingestionEnabled` suppression, DynamoDB blocklist table rejection, banking pattern allowlist/exclusion, dual-file copy to `kb-source`.
  - [`tests/test_queueing_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_queueing_lambda.py): GSI query pagination, batch slicing in chunks of 10, SQS batch dispatch, pipelineStatus transition to `CRAWLING`/`IDLE`.
  - [`tests/test_discovery_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_discovery_lambda.py): URL normalization, allow/exclude regexes, XML sitemaps and sitemap indexes, robots.txt resilience (404 fallback), `if_not_exists` registry upsert, async Queueing trigger.
  - [`tests/test_ingestion_trigger_lambda.py`](file:///c:/Users/rahul/OneDrive/Documents/boi-web-crwaler-solution/tests/test_ingestion_trigger_lambda.py): 3-queue depth breakdown, consecutive empty checks counter, cooldown & concurrency slot claim, Bedrock job running check, stale page & companion sidecar S3 deletion reconciliation.
  - **Status**: **VERIFIED (40/40 Passing)**

---

## 4. Pre-Flight Verification & Smoke Testing
- [ ] **Dry Run with 2-3 Seed URLs**: Run a test with `SEED_BUCKET`/`SEED_KEY` containing 2-3 sample pages.
- [ ] **S3 Verification**:
  - Verify raw bucket has `raw/{YYYY-MM-DD}/{urlHash}.html` with `source-url` metadata.
  - Verify staging bucket has `processed/{urlHash}.txt` and companion `processed/{urlHash}.txt.metadata.json`.
  - Verify `kb-source` has both files copied by Curation.
- [ ] **DynamoDB Verification**:
  - Verify `content-registry` items reach status `APPROVED`.
  - Verify `pipeline-state` updates `pipelineStatus` through `DISCOVERING` -> `QUEUEING` -> `CRAWLING` -> `INGESTING` -> `IDLE`.
- [ ] **Bedrock Knowledge Base Sync**:
  - Trigger `ingestion_trigger_lambda` manually.
  - Confirm Bedrock ingestion job reaches status `COMPLETE`.
- [ ] **Amazon Connect AI Agent Test**:
  - Query the Amazon Connect chatbot for questions answered by the test pages.
  - Verify answers and citation links.

---

## 5. Security, Compliance & Observability
- [ ] **Bank WAF Allowlist**: Confirm bank SOC/WAF has allowlisted User-Agent `internal-kb-crawler/1.0`. (If required, deploy Crawler into VPC with static NAT Gateway Elastic IPs).
- [ ] **Bedrock Guardrails**: Attach Amazon Bedrock Guardrails to the Knowledge Base to prevent indirect prompt injection from public web content.
- [ ] **CloudWatch Alarms**:
  - Alert if `ApproximateNumberOfMessagesVisible > 0` on `crawl-dlq`, `processing-dlq`, or `curation-dlq`.
  - Metric Filter on `[ERROR]` level across the 6 Lambda log groups.
  - EventBridge rule on Bedrock Ingestion Job `FAILED` status targeting SNS alert topic.

---

## 6. Production Troubleshooting with CloudWatch Log Insights

### Query 1: Find all failed SQS records and root causes
```sql
fields @timestamp, @message
| filter @message like /ERROR/ or @message like /Failed/
| sort @timestamp desc
| limit 50
```

### Query 2: Track crawler latencies and bank server response codes
```sql
fields @timestamp, @message
| filter @message like /HTTP/ or @message like /Crawled and stored/
| parse @message /.*HTTP (error|status) (?<statusCode>\d+).*/
| stats count(*) by statusCode
```

### Query 3: Trace a specific URL or hash across the entire pipeline
```sql
fields @timestamp, @logStream, @message
| filter @message like /<insert-url-or-hash-here>/
| sort @timestamp asc
| limit 100
```

