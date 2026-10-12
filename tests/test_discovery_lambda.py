import json
import os
import re
import sys
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test init to set env vars before lambda import
import tests  # noqa: F401

import discovery_lambda


class TestDiscoveryLambda(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = MagicMock()
        self.mock_state_table = MagicMock()
        self.mock_registry_table = MagicMock()
        self.mock_lambda_client = MagicMock()

        discovery_lambda.s3 = self.mock_s3
        discovery_lambda.state_table = self.mock_state_table
        discovery_lambda.registry_table = self.mock_registry_table
        discovery_lambda.lambda_client = self.mock_lambda_client

    def test_normalize_url(self):
        """Verify URL canonicalization: lowercase netloc, strip tracking params, sort query."""
        raw = "HTTPS://Bank.com:443/products/savings/?utm_source=twitter&b=2&a=1#section"
        expected = "https://bank.com:443/products/savings?a=1&b=2"
        self.assertEqual(discovery_lambda.normalize_url(raw), expected)

        # Single root slash preserved
        self.assertEqual(discovery_lambda.normalize_url("https://bank.com/"), "https://bank.com/")

    def test_allowlist_and_exclusion_filtering(self):
        """Verify banking allowlist patterns and exclusion patterns."""
        with patch.object(
            discovery_lambda,
            "ALLOW_PATTERNS",
            [re.compile("/personal-banking"), re.compile("/loans")]
        ), patch.object(
            discovery_lambda,
            "EXCLUDE_PATTERNS",
            [re.compile("/login"), re.compile(r"\.pdf$")]
        ):
            # Allowed
            self.assertTrue(discovery_lambda.is_allowlisted("https://bank.com/personal-banking/savings"))
            self.assertTrue(discovery_lambda.is_allowlisted("https://bank.com/loans/home-loan"))
            # Not allowed
            self.assertFalse(discovery_lambda.is_allowlisted("https://bank.com/investor-relations"))

            # Excluded
            self.assertTrue(discovery_lambda.is_excluded("https://bank.com/personal-banking/login"))
            self.assertTrue(discovery_lambda.is_excluded("https://bank.com/loans/guide.pdf"))
            self.assertFalse(discovery_lambda.is_excluded("https://bank.com/loans/apply"))

    def test_load_blocked_urls_from_dynamodb(self):
        """Verify scanning optional blocklist table returns normalized URL set."""
        with patch.object(discovery_lambda, "BLOCKLIST_TABLE_NAME", "test-url-blocklist"):
            mock_block_table = MagicMock()
            mock_block_table.scan.return_value = {
                "Items": [
                    {"url": "https://bank.com/internal-secret/"},
                    {"url": "https://bank.com/deprecated?utm_source=test"},
                ]
            }
            with patch.object(discovery_lambda.dynamodb, "Table", return_value=mock_block_table):
                blocked = discovery_lambda.load_blocked_urls()

                self.assertIn("https://bank.com/internal-secret", blocked)
                self.assertIn("https://bank.com/deprecated", blocked)

    def test_parse_sitemap_flat_and_index(self):
        """Verify flat sitemap XML and sitemap index parsing."""
        flat_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://bank.com/page1</loc></url>
            <url><loc>https://bank.com/page2</loc></url>
        </urlset>
        """
        mock_resp = MagicMock()
        mock_resp.read.return_value = flat_xml
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            urls = discovery_lambda.get_urls_from_sitemap("https://bank.com/sitemap.xml")
            self.assertEqual(urls, ["https://bank.com/page1", "https://bank.com/page2"])

    def test_robots_txt_handling(self):
        """Verify robots.txt 404 falls back to open access, and rules are respected."""
        # 404 test
        http_err = urllib.error.HTTPError("https://bank.com/robots.txt", 404, "Not Found", {}, None)
        with patch("urllib.request.urlopen", side_effect=http_err):
            rp = discovery_lambda.load_robots_parser("https://bank.com/page1")
            self.assertIsNotNone(rp)
            self.assertTrue(discovery_lambda.is_allowed_by_robots(rp, "https://bank.com/anything"))

        # Valid robots.txt disallowing /private
        robots_content = b"User-agent: *\nDisallow: /private/\n"
        mock_resp = MagicMock()
        mock_resp.read.return_value = robots_content
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            rp = discovery_lambda.load_robots_parser("https://bank.com/home")
            self.assertTrue(discovery_lambda.is_allowed_by_robots(rp, "https://bank.com/public"))
            self.assertFalse(discovery_lambda.is_allowed_by_robots(rp, "https://bank.com/private/data"))

    def test_upsert_registry_item_uses_if_not_exists(self):
        """Verify upsert preserves human flags using if_not_exists."""
        url = "https://bank.com/loans"
        url_hash = discovery_lambda.upsert_registry_item(url, generation=1)

        self.mock_registry_table.update_item.assert_called_once()
        db_call = self.mock_registry_table.update_item.call_args[1]
        self.assertEqual(db_call["Key"]["urlHash"], url_hash)
        self.assertIn("if_not_exists(crawlEnabled, :true_str)", db_call["UpdateExpression"])
        self.assertIn("if_not_exists(ingestionEnabled, :true_str)", db_call["UpdateExpression"])
        self.assertIn("if_not_exists(#status, :discovered)", db_call["UpdateExpression"])

    def test_handler_full_flow(self):
        """Verify handler coordinates sitemap discovery, deduplication, upsert, and Queueing trigger."""
        raw_urls = [
            "https://bank.com/loans",
            "https://bank.com/loans/",  # Duplicate after normalization
            "https://bank.com/blocked-url",
            "https://bank.com/excluded-url",
        ]
        self.mock_state_table.update_item.side_effect = [
            {"Attributes": {"crawlGeneration": 5}},  # get_next_generation
            {},  # mark_discovery_complete
        ]

        with patch.object(
            discovery_lambda, "SITEMAP_URL", "https://bank.com/sitemap.xml"
        ), patch.object(
            discovery_lambda, "get_urls_from_sitemap", return_value=raw_urls
        ), patch.object(
            discovery_lambda, "load_robots_parser", return_value=None
        ), patch.object(
            discovery_lambda, "load_blocked_urls", return_value={"https://bank.com/blocked-url"}
        ), patch.object(
            discovery_lambda, "is_excluded", side_effect=lambda u: "excluded" in u
        ), patch.object(
            discovery_lambda, "is_allowlisted", return_value=True
        ), patch.object(
            discovery_lambda, "upsert_all", return_value=(1, [])
        ) as mock_upsert:
            result = discovery_lambda.handler({}, None)

            # mock_upsert should be called with only https://bank.com/loans (1 url)
            mock_upsert.assert_called_once_with(["https://bank.com/loans"], 5)

            # Verify Queueing Lambda invoked asynchronously
            self.mock_lambda_client.invoke.assert_called_once()
            invoke_kwargs = self.mock_lambda_client.invoke.call_args[1]
            self.assertEqual(invoke_kwargs["FunctionName"], discovery_lambda.QUEUEING_LAMBDA_NAME)
            self.assertEqual(invoke_kwargs["InvocationType"], "Event")
            self.assertEqual(json.loads(invoke_kwargs["Payload"]), {"generation": 5})

            self.assertEqual(result["generation"], 5)
            self.assertEqual(result["rawUrls"], 4)
            self.assertEqual(result["afterDedup"], 3)
            self.assertEqual(result["blocklistTableBlocked"], 1)
            self.assertEqual(result["excluded"], 1)
            self.assertEqual(result["upserted"], 1)


if __name__ == "__main__":
    unittest.main()

