import hashlib
import json
import os
import sys
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test init to set env vars before lambda import
import tests  # noqa: F401

import crawler_lambda


class TestCrawlerLambda(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = MagicMock()
        self.mock_registry_table = MagicMock()

        crawler_lambda.s3 = self.mock_s3
        crawler_lambda.registry_table = self.mock_registry_table

    def test_fetch_url_success_and_redirect(self):
        """Verify fetch_url returns status, content_type, body, and final_url on redirect."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        mock_resp.read.return_value = b"<html><body>Bank Homepage</body></html>"
        mock_resp.geturl.return_value = "https://bank.com/en/home"
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            status, ct, body, final_url = crawler_lambda.fetch_url("http://bank.com")

            self.assertEqual(status, 200)
            self.assertEqual(ct, "text/html; charset=utf-8")
            self.assertEqual(body, b"<html><body>Bank Homepage</body></html>")
            self.assertEqual(final_url, "https://bank.com/en/home")

    def test_fetch_url_http_error(self):
        """Verify fetch_url catches HTTPError and returns code with empty body."""
        http_err = urllib.error.HTTPError("https://bank.com/missing", 404, "Not Found", {}, None)
        with patch("urllib.request.urlopen", side_effect=http_err):
            status, ct, body, final_url = crawler_lambda.fetch_url("https://bank.com/missing")

            self.assertEqual(status, 404)
            self.assertEqual(body, b"")
            self.assertEqual(final_url, "https://bank.com/missing")

    def test_process_message_permanent_404_recorded_without_raising(self):
        """Verify permanent 404 is recorded in DynamoDB without raising or putting to S3."""
        with patch.object(
            crawler_lambda,
            "fetch_url",
            return_value=(404, "", b"", "https://bank.com/deleted-loan")
        ):
            crawler_lambda.process_message({
                "url": "https://bank.com/deleted-loan",
                "batchId": "b-123",
                "generation": 5,
            })

            # S3 put_object should not be called
            self.mock_s3.put_object.assert_not_called()
            # DynamoDB should be updated with FAILED_HTTP_404
            self.mock_registry_table.update_item.assert_called_once()
            call_kwargs = self.mock_registry_table.update_item.call_args[1]
            self.assertEqual(call_kwargs["ExpressionAttributeValues"][":status"], "FAILED_HTTP_404")
            self.assertEqual(call_kwargs["ExpressionAttributeValues"][":code"], 404)

    def test_process_message_unchanged_content_skips_s3(self):
        """Verify unchanged content (matching SHA-256) touches registry without rewriting S3."""
        body_content = b"<html><body>Consistent content</body></html>"
        expected_hash = hashlib.sha256(body_content).hexdigest()

        with patch.object(
            crawler_lambda,
            "fetch_url",
            return_value=(200, "text/html", body_content, "https://bank.com/rates")
        ), patch.object(
            crawler_lambda,
            "get_registry_item",
            return_value={"contentHash": expected_hash}
        ):
            crawler_lambda.process_message({
                "url": "https://bank.com/rates",
                "batchId": "b-456",
                "generation": 10,
            })

            # S3 put_object should not be called
            self.mock_s3.put_object.assert_not_called()
            # DynamoDB should be touched (touch_unchanged)
            self.mock_registry_table.update_item.assert_called_once()
            call_kwargs = self.mock_registry_table.update_item.call_args[1]
            self.assertIn("lastCheckedAt = :now", call_kwargs["UpdateExpression"])

    def test_process_message_new_content_writes_s3_and_registry(self):
        """Verify newly fetched/changed content writes to raw S3 bucket and sets status to CRAWLED."""
        body_content = b"<html><body>Brand new content</body></html>"
        content_hash = hashlib.sha256(body_content).hexdigest()

        with patch.object(
            crawler_lambda,
            "fetch_url",
            return_value=(200, "text/html", body_content, "https://bank.com/new-promo")
        ), patch.object(
            crawler_lambda,
            "get_registry_item",
            return_value=None  # First time crawled
        ):
            crawler_lambda.process_message({
                "url": "https://bank.com/new-promo",
                "batchId": "b-789",
                "generation": 12,
            })

            # Verify S3 upload
            self.mock_s3.put_object.assert_called_once()
            s3_call = self.mock_s3.put_object.call_args[1]
            self.assertEqual(s3_call["Bucket"], "test-raw-bucket")
            self.assertEqual(s3_call["Body"], body_content)
            self.assertEqual(s3_call["Metadata"]["source-url"], "https://bank.com/new-promo")

            # Verify DynamoDB status update
            self.mock_registry_table.update_item.assert_called_once()
            db_call = self.mock_registry_table.update_item.call_args[1]
            self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "CRAWLED")
            self.assertEqual(db_call["ExpressionAttributeValues"][":content_hash"], content_hash)

    def test_handler_records_batch_failures_on_5xx_errors(self):
        """Verify 5xx server error triggers batch item failure for SQS retry."""
        event = {
            "Records": [
                {
                    "messageId": "msg-ok",
                    "body": json.dumps({"url": "https://bank.com/ok", "batchId": "b-1"}),
                },
                {
                    "messageId": "msg-error",
                    "body": json.dumps({"url": "https://bank.com/500-error", "batchId": "b-2"}),
                },
            ]
        }

        def fake_fetch(url):
            if "500-error" in url:
                return 500, "", b"", url
            return 200, "text/html", b"<html><body>OK</body></html>", url

        with patch.object(crawler_lambda, "fetch_url", side_effect=fake_fetch):
            result = crawler_lambda.handler(event, None)

            self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "msg-error"}]})


if __name__ == "__main__":
    unittest.main()

