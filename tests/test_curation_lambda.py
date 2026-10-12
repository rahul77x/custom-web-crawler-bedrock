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

import curation_lambda


class TestCurationLambda(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = MagicMock()
        self.mock_registry_table = MagicMock()
        self.mock_blocklist_table = MagicMock()

        curation_lambda.s3 = self.mock_s3
        curation_lambda.registry_table = self.mock_registry_table
        curation_lambda.blocklist_table = self.mock_blocklist_table

    def test_normalize_url(self):
        """Verify tracking parameters are stripped and trailing slashes normalized."""
        url = "https://bank.com/personal-banking/?utm_source=google&utm_campaign=diwali&gclid=123"
        normalized = curation_lambda.normalize_url(url)
        self.assertEqual(normalized, "https://bank.com/personal-banking")

    def test_process_metadata_in_flight_status_raises_for_retry(self):
        """Verify items in DISCOVERED or CRAWLED status raise an error to trigger SQS retry."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/personal-banking/savings",
                "urlHash": "hash123",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }

        # Case 1: Status is CRAWLED
        self.mock_registry_table.get_item.return_value = {"Item": {"status": "CRAWLED"}}

        with self.assertRaises(RuntimeError) as ctx:
            curation_lambda.process_metadata_object(
                "test-staging-bucket", "processed/hash123.txt.metadata.json"
            )
        self.assertIn("not ready for curation yet. Retrying.", str(ctx.exception))

    def test_process_metadata_terminal_status_skipped(self):
        """Verify items in terminal statuses (QUARANTINED, REJECTED, etc.) are skipped silently."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/careers",
                "urlHash": "hash456",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }
        self.mock_registry_table.get_item.return_value = {"Item": {"status": "QUARANTINED"}}

        curation_lambda.process_metadata_object(
            "test-staging-bucket", "processed/hash456.txt.metadata.json"
        )

        # Neither S3 copy nor DynamoDB update should happen
        self.mock_s3.copy_object.assert_not_called()
        self.mock_registry_table.update_item.assert_not_called()

    def test_process_metadata_suppressed_when_ingestion_disabled(self):
        """Verify items with ingestionEnabled='false' are marked SUPPRESSED."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/personal-banking/internal-policy",
                "urlHash": "hash789",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }
        self.mock_registry_table.get_item.return_value = {
            "Item": {"status": "PROCESSED", "ingestionEnabled": "false"}
        }

        curation_lambda.process_metadata_object(
            "test-staging-bucket", "processed/hash789.txt.metadata.json"
        )

        self.mock_s3.copy_object.assert_not_called()
        self.mock_registry_table.update_item.assert_called_once()
        db_call = self.mock_registry_table.update_item.call_args[1]
        self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "SUPPRESSED")

    def test_process_metadata_rejected_by_blocklist_table(self):
        """Verify URLs found in DynamoDB url-blocklist table are marked REJECTED."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/secret-rates",
                "urlHash": "hash-blocked",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }
        self.mock_registry_table.get_item.return_value = {
            "Item": {"status": "PROCESSED", "ingestionEnabled": "true"}
        }
        self.mock_blocklist_table.get_item.return_value = {
            "Item": {"url": "https://bank.com/secret-rates"}
        }

        curation_lambda.process_metadata_object(
            "test-staging-bucket", "processed/hash-blocked.txt.metadata.json"
        )

        self.mock_s3.copy_object.assert_not_called()
        db_call = self.mock_registry_table.update_item.call_args[1]
        self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "REJECTED")
        self.assertIn("url-blocklist", db_call["ExpressionAttributeValues"][":reason"])

    def test_process_metadata_rejected_by_exclusion_pattern(self):
        """Verify URLs matching CURATION_EXCLUDE_PATTERNS are marked REJECTED."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/careers/jobs",
                "urlHash": "hash-careers",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }
        self.mock_registry_table.get_item.return_value = {
            "Item": {"status": "PROCESSED", "ingestionEnabled": "true"}
        }
        self.mock_blocklist_table.get_item.return_value = {}

        with patch.object(curation_lambda, "EXCLUDE_PATTERNS", [re.compile("/careers")]):
            curation_lambda.process_metadata_object(
                "test-staging-bucket", "processed/hash-careers.txt.metadata.json"
            )

            self.mock_s3.copy_object.assert_not_called()
            db_call = self.mock_registry_table.update_item.call_args[1]
            self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "REJECTED")

    def test_process_metadata_approved_copies_both_files(self):
        """Verify passed item copies text and metadata to kb-source and marks APPROVED."""
        metadata = {
            "metadataAttributes": {
                "sourceUrl": "https://bank.com/personal-banking/loans",
                "urlHash": "hash-approved",
            }
        }
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(metadata).encode("utf-8")))
        }
        self.mock_registry_table.get_item.return_value = {
            "Item": {"status": "PROCESSED", "ingestionEnabled": "true"}
        }
        self.mock_blocklist_table.get_item.return_value = {}

        with patch.object(
            curation_lambda,
            "CURATION_ALLOW_PATTERNS",
            [re.compile("/personal-banking")]
        ), patch.object(curation_lambda, "EXCLUDE_PATTERNS", []):
            curation_lambda.process_metadata_object(
                "test-staging-bucket", "processed/hash-approved.txt.metadata.json"
            )

            # S3 should copy both the text file and the metadata.json sidecar
            self.assertEqual(self.mock_s3.copy_object.call_count, 2)
            copy_calls = self.mock_s3.copy_object.call_args_list
            keys_copied = [call[1]["Key"] for call in copy_calls]
            self.assertIn("processed/hash-approved.txt", keys_copied)
            self.assertIn("processed/hash-approved.txt.metadata.json", keys_copied)

            # DynamoDB should mark APPROVED
            self.mock_registry_table.update_item.assert_called_once()
            db_call = self.mock_registry_table.update_item.call_args[1]
            self.assertEqual(db_call["ExpressionAttributeValues"][":approved"], "APPROVED")
            self.assertEqual(db_call["ExpressionAttributeValues"][":key"], "processed/hash-approved.txt")

    def test_handler_records_batch_failures(self):
        """Verify partial failure reporting in SQS handler."""
        event = {
            "Records": [
                {
                    "messageId": "msg-retry",
                    "body": json.dumps({
                        "Records": [{
                            "s3": {
                                "bucket": {"name": "test-staging-bucket"},
                                "object": {"key": "processed/inflight.txt.metadata.json"},
                            }
                        }]
                    }),
                }
            ]
        }

        with patch.object(
            curation_lambda,
            "process_metadata_object",
            side_effect=RuntimeError("Retrying due to in-flight lag")
        ):
            result = curation_lambda.handler(event, None)
            self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "msg-retry"}]})


if __name__ == "__main__":
    unittest.main()

