import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test init to set env vars before lambda import
import tests  # noqa: F401

import queueing_lambda


class TestQueueingLambda(unittest.TestCase):
    def setUp(self):
        self.mock_sqs = MagicMock()
        self.mock_registry_table = MagicMock()
        self.mock_state_table = MagicMock()

        queueing_lambda.sqs = self.mock_sqs
        queueing_lambda.registry_table = self.mock_registry_table
        queueing_lambda.state_table = self.mock_state_table

    def test_send_batch_formats_and_dispatches_entries(self):
        """Verify SQS batch messages contain correct payload structure and batch IDs."""
        chunk = [
            {"url": "https://bank.com/p1"},
            {"url": "https://bank.com/p2"},
        ]
        self.mock_sqs.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": [],
        }

        sent = queueing_lambda.send_batch(chunk, batch_id="req-123", generation=7)
        self.assertEqual(sent, 2)

        self.mock_sqs.send_message_batch.assert_called_once()
        call_kwargs = self.mock_sqs.send_message_batch.call_args[1]
        self.assertEqual(call_kwargs["QueueUrl"], queueing_lambda.CRAWL_QUEUE_URL)
        entries = call_kwargs["Entries"]
        self.assertEqual(len(entries), 2)

        body0 = json.loads(entries[0]["MessageBody"])
        self.assertEqual(body0["url"], "https://bank.com/p1")
        self.assertEqual(body0["batchId"], "req-123")
        self.assertEqual(body0["generation"], 7)

    def test_stream_and_enqueue_urls_pagination_and_chunking(self):
        """Verify GSI pagination correctly batches across pages and flushes remainder."""
        # Page 1: 15 items
        page1_items = [{"url": f"https://bank.com/u{i}"} for i in range(15)]
        # Page 2: 7 items (total 22 items)
        page2_items = [{"url": f"https://bank.com/u{i}"} for i in range(15, 22)]

        self.mock_registry_table.query.side_effect = [
            {"Items": page1_items, "LastEvaluatedKey": {"urlHash": "hash15"}},
            {"Items": page2_items},
        ]

        with patch.object(queueing_lambda, "send_batch", side_effect=[10, 10, 2]) as mock_send:
            candidates, enqueued = queueing_lambda.stream_and_enqueue_urls(
                batch_id="batch-test", generation=4
            )

            self.assertEqual(candidates, 22)
            self.assertEqual(enqueued, 22)
            # Should have called send_batch 3 times: 10 + 10 + 2 (flush remainder)
            self.assertEqual(mock_send.call_count, 3)

    def test_record_queueing_complete_crawling_when_enqueued(self):
        """Verify pipelineStatus transitions to CRAWLING when URLs are enqueued."""
        queueing_lambda.record_queueing_complete(generation=3, enqueued_count=15)

        self.mock_state_table.update_item.assert_called_once()
        db_call = self.mock_state_table.update_item.call_args[1]
        self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "CRAWLING")
        self.assertEqual(db_call["ExpressionAttributeValues"][":zero"], 0)
        self.assertEqual(db_call["ExpressionAttributeValues"][":gen"], 3)

    def test_record_queueing_complete_idle_when_zero_enqueued(self):
        """Verify pipelineStatus transitions to IDLE when 0 URLs are enqueued."""
        queueing_lambda.record_queueing_complete(generation=3, enqueued_count=0)

        self.mock_state_table.update_item.assert_called_once()
        db_call = self.mock_state_table.update_item.call_args[1]
        self.assertEqual(db_call["ExpressionAttributeValues"][":status"], "IDLE")

    def test_handler(self):
        """Verify handler coordinates streaming, state update, and returns summary."""
        context = MagicMock()
        context.aws_request_id = "req-abc-999"

        with patch.object(
            queueing_lambda, "stream_and_enqueue_urls", return_value=(20, 20)
        ), patch.object(
            queueing_lambda, "record_queueing_complete"
        ) as mock_record:
            result = queueing_lambda.handler({"generation": 5}, context)

            mock_record.assert_called_once_with(5, 20)
            self.assertEqual(result, {
                "batchId": "req-abc-999",
                "generation": 5,
                "candidateCount": 20,
                "urlsEnqueued": 20,
            })


if __name__ == "__main__":
    unittest.main()

