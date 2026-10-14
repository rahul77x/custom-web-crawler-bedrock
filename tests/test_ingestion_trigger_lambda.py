import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test init to set env vars before lambda import
import tests  # noqa: F401

import ingestion_trigger_lambda


class TestIngestionTriggerLambda(unittest.TestCase):
    def setUp(self):
        self.mock_sqs = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_state_table = MagicMock()
        self.mock_registry_table = MagicMock()
        self.mock_bedrock_agent = MagicMock()

        ingestion_trigger_lambda.sqs = self.mock_sqs
        ingestion_trigger_lambda.s3 = self.mock_s3
        ingestion_trigger_lambda.s3_kb = self.mock_s3
        ingestion_trigger_lambda.state_table = self.mock_state_table
        ingestion_trigger_lambda.registry_table = self.mock_registry_table
        ingestion_trigger_lambda.bedrock_agent = self.mock_bedrock_agent

    def test_get_queue_depths(self):
        """Verify queue depth calculation across visible and in-flight messages."""
        self.mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "5",
                "ApproximateNumberOfMessagesNotVisible": "2",
            }
        }
        breakdown, total = ingestion_trigger_lambda.get_queue_depths()

        # 3 queues configured in tests init, each has 7 messages = 21 total
        self.assertEqual(total, 21)
        self.assertEqual(len(breakdown), 3)

    def test_handler_skips_during_discovering_or_queueing(self):
        """Verify trigger does not fire while Discovery or Queueing is active."""
        with patch.object(
            ingestion_trigger_lambda,
            "get_pipeline_state",
            return_value={"pipelineStatus": "DISCOVERING", "discoveryStartedAt": 1000}
        ), patch("time.time", return_value=1100):  # 100s elapsed
            result = ingestion_trigger_lambda.handler({}, None)
            self.assertFalse(result["triggered"])
            self.assertEqual(result["reason"], "pipeline_discovering")

    def test_handler_resets_checks_when_queues_not_empty(self):
        """Verify non-empty queues reset consecutive empty checks."""
        with patch.object(
            ingestion_trigger_lambda,
            "get_pipeline_state",
            return_value={"pipelineStatus": "CRAWLING"}
        ), patch.object(
            ingestion_trigger_lambda,
            "get_queue_depths",
            return_value=({"crawl-queue": "5 visible, 0 in-flight"}, 5)
        ), patch.object(
            ingestion_trigger_lambda, "reset_empty_checks"
        ) as mock_reset:
            result = ingestion_trigger_lambda.handler({}, None)

            mock_reset.assert_called_once()
            self.assertFalse(result["triggered"])
            self.assertEqual(result["reason"], "queues_not_empty")
            self.assertEqual(result["depth"], 5)

    def test_handler_skips_when_idle_and_empty(self):
        """Verify idle pipeline with empty queues does nothing."""
        with patch.object(
            ingestion_trigger_lambda,
            "get_pipeline_state",
            return_value={"pipelineStatus": "IDLE"}
        ), patch.object(
            ingestion_trigger_lambda,
            "get_queue_depths",
            return_value=({}, 0)
        ):
            result = ingestion_trigger_lambda.handler({}, None)
            self.assertFalse(result["triggered"])
            self.assertEqual(result["reason"], "pipeline_idle")

    def test_consecutive_empty_checks_state_machine(self):
        """Verify empty checks counter must reach REQUIRED_EMPTY_CHECKS before triggering."""
        # 1st empty check (returns 1, required is 2)
        with patch.object(
            ingestion_trigger_lambda,
            "get_pipeline_state",
            return_value={"pipelineStatus": "CRAWLING"}
        ), patch.object(
            ingestion_trigger_lambda,
            "get_queue_depths",
            return_value=({}, 0)
        ), patch.object(
            ingestion_trigger_lambda,
            "increment_empty_checks",
            return_value=1
        ):
            result = ingestion_trigger_lambda.handler({}, None)
            self.assertFalse(result["triggered"])
            self.assertEqual(result["reason"], "waiting_for_confirmation")
            self.assertEqual(result["emptyChecks"], 1)

    def test_mark_deleted_removes_content_and_sidecar_from_s3(self):
        """Verify reconciliation deletes both the main content and metadata sidecar in S3."""
        item = {
            "urlHash": "hash-del",
            "curatedS3Key": "processed/hash-del.txt",
        }
        ingestion_trigger_lambda.mark_deleted(item)

        # S3 should delete both processed/hash-del.txt and processed/hash-del.txt.metadata.json
        self.assertEqual(self.mock_s3.delete_object.call_count, 2)
        deleted_keys = [call[1]["Key"] for call in self.mock_s3.delete_object.call_args_list]
        self.assertIn("processed/hash-del.txt", deleted_keys)
        self.assertIn("processed/hash-del.txt.metadata.json", deleted_keys)

        # DynamoDB should be updated to DELETED
        self.mock_registry_table.update_item.assert_called_once()
        db_call = self.mock_registry_table.update_item.call_args[1]
        self.assertEqual(db_call["ExpressionAttributeValues"][":deleted"], "DELETED")

    def test_reconcile_deleted_urls(self):
        """Verify table scan finds stale items and marks them deleted."""
        stale_item = {"urlHash": "stale1", "lastSeenGeneration": 3, "status": "APPROVED"}
        self.mock_registry_table.scan.return_value = {"Items": [stale_item]}

        with patch.object(ingestion_trigger_lambda, "mark_deleted") as mock_mark:
            deleted_count = ingestion_trigger_lambda.reconcile_deleted_urls(current_generation=4)
            self.assertEqual(deleted_count, 1)
            mock_mark.assert_called_once_with(stale_item)

    def test_handler_triggers_bedrock_sync_success(self):
        """Verify complete flow: quiescence reached, reconciles deletions, starts Bedrock job."""
        pipeline_state = {
            "pipelineStatus": "CRAWLING",
            "lastDiscoveredGeneration": 5,
        }

        self.mock_bedrock_agent.start_ingestion_job.return_value = {
            "ingestionJob": {"ingestionJobId": "job-xyz-123"}
        }

        with patch.object(
            ingestion_trigger_lambda, "get_pipeline_state", return_value=pipeline_state
        ), patch.object(
            ingestion_trigger_lambda, "get_queue_depths", return_value=({}, 0)
        ), patch.object(
            ingestion_trigger_lambda, "increment_empty_checks", return_value=2
        ), patch.object(
            ingestion_trigger_lambda, "ingestion_job_in_progress", return_value=False
        ), patch.object(
            ingestion_trigger_lambda, "try_claim_ingestion_slot", return_value=True
        ), patch.object(
            ingestion_trigger_lambda, "reconcile_deleted_urls", return_value=3
        ) as mock_reconcile, patch.object(
            ingestion_trigger_lambda, "mark_pipeline_idle"
        ) as mock_idle:
            result = ingestion_trigger_lambda.handler({}, None)

            self.assertTrue(result["triggered"])
            self.assertEqual(result["ingestionJobId"], "job-xyz-123")
            self.assertEqual(result["deletedCount"], 3)

            # Reconcile called with lastDiscoveredGeneration
            mock_reconcile.assert_called_once_with(5)

            # Bedrock start_ingestion_job invoked
            self.mock_bedrock_agent.start_ingestion_job.assert_called_once_with(
                knowledgeBaseId=ingestion_trigger_lambda.KB_ID,
                dataSourceId=ingestion_trigger_lambda.DATA_SOURCE_ID,
            )

            # Pipeline marked IDLE
            mock_idle.assert_called_once()


if __name__ == "__main__":
    unittest.main()

