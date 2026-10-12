import os

# Set dummy AWS credentials and default region for tests
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

# Environment variables for Lambda functions
os.environ.setdefault("STATE_TABLE_NAME", "test-pipeline-state")
os.environ.setdefault("REGISTRY_TABLE_NAME", "test-content-registry")
os.environ.setdefault("QUEUEING_LAMBDA_NAME", "test-queueing-lambda")
os.environ.setdefault("BLOCKLIST_TABLE_NAME", "test-url-blocklist")
os.environ.setdefault("CRAWL_ENABLED_INDEX_NAME", "test-crawl-enabled-index")
os.environ.setdefault("CRAWL_QUEUE_URL", "https://sqs.ap-south-1.amazonaws.com/123456789012/test-crawl-queue")
os.environ.setdefault("RAW_BUCKET", "test-raw-bucket")
os.environ.setdefault("STAGING_BUCKET", "test-staging-bucket")
os.environ.setdefault("KB_SOURCE_BUCKET", "test-kb-source-bucket")
os.environ.setdefault("MIN_TEXT_LENGTH", "50")
os.environ.setdefault("MONITORED_QUEUE_URLS", "https://sqs.ap-south-1.amazonaws.com/123456789012/q1,https://sqs.ap-south-1.amazonaws.com/123456789012/q2,https://sqs.ap-south-1.amazonaws.com/123456789012/q3")
os.environ.setdefault("KNOWLEDGE_BASE_ID", "test-kb-id")
os.environ.setdefault("DATA_SOURCE_ID", "test-ds-id")
os.environ.setdefault("REQUIRED_EMPTY_CHECKS", "2")
os.environ.setdefault("MIN_HOURS_BETWEEN_INGESTIONS", "6")

