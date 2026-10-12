import json
import os
import re
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test init to set env vars before lambda import
import tests  # noqa: F401

import processing_lambda


class TestProcessingLambda(unittest.TestCase):
    def setUp(self):
        # Mock AWS clients
        self.mock_s3 = MagicMock()
        self.mock_dynamodb = MagicMock()
        self.mock_registry_table = MagicMock()

        processing_lambda.s3 = self.mock_s3
        processing_lambda.registry_table = self.mock_registry_table

    def test_text_extractor_financial_table_to_markdown(self):
        """Verify HTML tables are converted to clean Markdown tables for Bedrock chunking."""
        html = """
        <html>
            <body>
                <h1>Personal Loan Interest Rates</h1>
                <table>
                    <tr>
                        <th>Tenure</th>
                        <th>Interest Rate</th>
                        <th>Processing Fee</th>
                    </tr>
                    <tr>
                        <td>1 Year</td>
                        <td>10.50%</td>
                        <td>1% | Min $50</td>
                    </tr>
                    <tr>
                        <td>3 Years</td>
                        <td>11.25%</td>
                        <td>1%</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        text, title, dropped = processing_lambda.extract_text(html.encode("utf-8"))

        self.assertIn("# Personal Loan Interest Rates", text)
        self.assertIn("| Tenure | Interest Rate | Processing Fee |", text)
        self.assertIn("| --- | --- | --- |", text)
        self.assertIn("| 1 Year | 10.50% | 1% \\| Min $50 |", text)  # Pipe in content escaped
        self.assertIn("| 3 Years | 11.25% | 1% |", text)
        self.assertEqual(dropped, 0)

    def test_text_extractor_strips_boilerplate_tags(self):
        """Verify script, style, nav, header, footer, and form tags are omitted."""
        html = """
        <html>
            <head><title>Sample Bank Page</title></head>
            <body>
                <header><p>Top Header Navigation</p></header>
                <nav><a href="/home">Home</a><a href="/login">Login</a></nav>
                <script>alert('malicious or tracking');</script>
                <style>body { color: red; }</style>
                <main>
                    <h1>Savings Account Overview</h1>
                    <p>Earn competitive interest on daily balances with zero monthly maintenance fees.</p>
                </main>
                <footer><p>Copyright 2026 Bank of India. All rights reserved.</p></footer>
            </body>
        </html>
        """
        text, title, dropped = processing_lambda.extract_text(html.encode("utf-8"))

        self.assertEqual(title, "Sample Bank Page")
        self.assertIn("# Savings Account Overview", text)
        self.assertIn("Earn competitive interest on daily balances with zero monthly maintenance fees.", text)
        self.assertNotIn("Top Header Navigation", text)
        self.assertNotIn("Home", text)
        self.assertNotIn("alert", text)
        self.assertNotIn("color: red", text)
        self.assertNotIn("Copyright 2026", text)

    def test_text_extractor_drops_boilerplate_patterns(self):
        """Verify paragraphs matching configured boilerplate regexes are dropped."""
        with patch.object(
            processing_lambda,
            "BOILERPLATE_PATTERNS",
            [re.compile("This page is for informational purposes only")]
        ):
            html = """
            <html>
                <body>
                    <p>Valid product content that should be indexed by Bedrock.</p>
                    <p>This page is for informational purposes only and does not constitute financial advice.</p>
                </body>
            </html>
            """
            text, _, dropped = processing_lambda.extract_text(html.encode("utf-8"))

            self.assertIn("Valid product content that should be indexed by Bedrock.", text)
            self.assertNotIn("informational purposes only", text)
            self.assertEqual(dropped, 1)

    def test_process_s3_record_quarantines_short_text(self):
        """Verify pages with extracted text shorter than MIN_TEXT_LENGTH are quarantined."""
        short_html = b"<html><body><p>Too short.</p></body></html>"
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=short_html)),
            "Metadata": {"source-url": "https://bank.com/terms"},
        }

        with patch.object(processing_lambda, "MIN_TEXT_LENGTH", 200):
            processing_lambda.process_s3_record("test-raw-bucket", "raw/2026-10-10/abc.html")

            # Check that quarantined status was recorded
            self.mock_registry_table.update_item.assert_called_once()
            call_kwargs = self.mock_registry_table.update_item.call_args[1]
            self.assertEqual(call_kwargs["ExpressionAttributeValues"][":quarantined"], "QUARANTINED")
            # Should NOT put processed text or metadata to staging bucket
            self.mock_s3.put_object.assert_not_called()

    def test_process_s3_record_success_and_ordering(self):
        """Verify processed text and Bedrock metadata sidecar are written, with status set first."""
        full_html = (
            b"<html><head><title>Fixed Deposit Rates</title></head><body>"
            b"<h1>Fixed Deposit Interest Rates</h1>"
            b"<p>Bank of India offers attractive fixed deposit interest rates for all tenures. "
            b"Senior citizens receive an additional 0.50% interest rate across all tenures.</p>"
            b"</body></html>"
        )
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=full_html)),
            "Metadata": {"source-url": "https://bank.com/rates/fd"},
        }

        call_order = []
        self.mock_registry_table.update_item.side_effect = lambda **kwargs: call_order.append("mark_processed")
        self.mock_s3.put_object.side_effect = lambda **kwargs: call_order.append(f"put_object_{kwargs['Key']}")

        processing_lambda.process_s3_record("test-raw-bucket", "raw/2026-10-10/fd.html")

        # Verify DynamoDB mark_processed occurred before metadata.json was uploaded to S3
        self.assertIn("mark_processed", call_order)
        text_put = [c for c in call_order if c.endswith(".txt")][0]
        meta_put = [c for c in call_order if c.endswith(".metadata.json")][0]

        # Order must be: put .txt -> mark_processed in DynamoDB -> put .metadata.json
        self.assertLess(call_order.index(text_put), call_order.index("mark_processed"))
        self.assertLess(call_order.index("mark_processed"), call_order.index(meta_put))

    def test_handler_reports_batch_item_failures(self):
        """Verify partial failure reporting in SQS handler."""
        event = {
            "Records": [
                {
                    "messageId": "msg-success",
                    "body": json.dumps({
                        "Records": [{
                            "s3": {
                                "bucket": {"name": "test-raw-bucket"},
                                "object": {"key": "raw/2026-10-10/ok.html"},
                            }
                        }]
                    }),
                },
                {
                    "messageId": "msg-fail",
                    "body": json.dumps({
                        "Records": [{
                            "s3": {
                                "bucket": {"name": "test-raw-bucket"},
                                "object": {"key": "raw/2026-10-10/missing-url.html"},
                            }
                        }]
                    }),
                },
            ]
        }

        def mock_process(bucket, key):
            if "missing-url" in key:
                raise RuntimeError("No source-url metadata on s3 object")

        with patch.object(processing_lambda, "process_s3_record", side_effect=mock_process):
            result = processing_lambda.handler(event, None)

            self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "msg-fail"}]})


if __name__ == "__main__":
    unittest.main()
