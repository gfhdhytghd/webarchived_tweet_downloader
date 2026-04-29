import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

import download_archive


def make_http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://web.archive.org/test"
    return requests.HTTPError(f"{status_code} error", response=response)


def make_image_response() -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = b"fake image bytes"
    response.headers["Content-Type"] = "image/jpeg"
    return response


class NoteTweetExtractionTests(unittest.TestCase):
    def test_parse_args_accepts_no_any_repair_for_html_from_json(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--no-any-repair"],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.html_from_json)
        self.assertTrue(args.no_any_repair)

    def test_parse_args_keeps_no_media_repair_as_compatibility_alias(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--no-media-repair"],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.no_any_repair)

    def test_parse_args_accepts_effort_level_for_default_run(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--effort-level", "2"],
        ):
            args = download_archive.parse_args()

        self.assertEqual(args.effort_level, 2)
        self.assertFalse(args.x_api_reply_chain_depth_explicit)

    def test_parse_args_rejects_effort_level_with_html_from_json(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--effort-level", "2"],
        ):
            with self.assertRaises(SystemExit):
                download_archive.parse_args()

    def test_resolve_effort_level_two_uses_easy_media_without_x_api_depth(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--effort-level", "2"],
        ):
            args = download_archive.parse_args()

        settings = download_archive.resolve_effort_settings(args)

        self.assertEqual(settings.reply_chain_depth, 8)
        self.assertEqual(settings.stream_media_policy.name, "easy_image")
        self.assertFalse(download_archive.should_load_x_api_auths(args))

    def test_resolve_effort_explicit_x_api_depth_overrides_preset_and_loads_auth(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--effort-level", "2", "--x-api-reply-chain-depth", "3"],
        ):
            args = download_archive.parse_args()

        settings = download_archive.resolve_effort_settings(args)

        self.assertEqual(settings.reply_chain_depth, 3)
        self.assertTrue(args.x_api_reply_chain_depth_explicit)
        self.assertTrue(download_archive.should_load_x_api_auths(args))

    def test_resolve_effort_level_three_runs_hard_final_repair(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--effort-level", "3"],
        ):
            args = download_archive.parse_args()

        settings = download_archive.resolve_effort_settings(args)

        self.assertTrue(settings.run_final_media_repair)
        self.assertEqual(settings.stream_media_policy.name, "easy_image")
        self.assertEqual(settings.final_media_policy.name, "full")
        self.assertTrue(settings.retry_forbidden_media_in_repair)

    def test_reply_tab_uses_existing_html_order_without_dom_reordering(self):
        script = download_archive.ARCHIVE_FILTER_JS

        self.assertNotIn("appendChild(tweet)", script)
        self.assertIn('data-archive-tab="all"', script)
        self.assertIn('t.dataset.hasComments === "true"', script)
        self.assertIn("...replyTweets", script)
        self.assertNotIn("getReplyViewSortTime", script)

    def test_parse_args_rejects_no_any_repair_with_x_api_reply_chain_depth(self):
        with mock.patch(
            "download_archive.sys.argv",
            [
                "download_archive.py",
                "account",
                "--html-from-json",
                "--no-any-repair",
                "--x-api-reply-chain-depth",
                "8",
            ],
        ):
            with self.assertRaises(SystemExit):
                download_archive.parse_args()

    def test_parse_args_accepts_x_api_only_for_reply_chain_repair(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--repair-reply-chain-depth", "8", "--x-api-only"],
        ):
            args = download_archive.parse_args()

        self.assertEqual(args.repair_reply_chain_depth, 8)
        self.assertTrue(args.x_api_only)

    def test_parse_args_accepts_no_wayback_fallback_alias(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--x-api-reply-chain-depth", "8", "--no-wayback-fallback"],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.html_from_json)
        self.assertEqual(args.x_api_reply_chain_depth, 8)
        self.assertTrue(args.x_api_only)

    def test_parse_args_accepts_repeated_env_file(self):
        with mock.patch(
            "download_archive.sys.argv",
            [
                "download_archive.py",
                "account",
                "--repair-reply-chain-depth",
                "8",
                "--env-file",
                "env.one",
                "--env-file",
                "env.two",
            ],
        ):
            args = download_archive.parse_args()

        self.assertEqual(args.env_files, ["env.one", "env.two"])
        self.assertEqual(args.env_file, "env.one")

    def test_parse_args_rejects_x_api_only_without_reply_chain_lookup(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--x-api-only"],
        ):
            with self.assertRaises(SystemExit):
                download_archive.parse_args()

    def test_parse_args_accepts_x_api_timeline_with_x_api_only(self):
        with mock.patch(
            "download_archive.sys.argv",
            [
                "download_archive.py",
                "account",
                "--x-api-timeline",
                "--x-api-timeline-pages",
                "0",
                "--x-api-timeline-page-size",
                "50",
                "--x-api-only",
            ],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.x_api_timeline)
        self.assertEqual(args.x_api_timeline_pages, 0)
        self.assertEqual(args.x_api_timeline_page_size, 50)
        self.assertTrue(args.x_api_only)

    def test_parse_args_accepts_x_api_only_for_repair_media(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--repair-media", "--x-api-only"],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.repair_media)
        self.assertTrue(args.x_api_only)

    def test_parse_args_accepts_x_api_conversations(self):
        with mock.patch(
            "download_archive.sys.argv",
            [
                "download_archive.py",
                "account",
                "--x-api-conversations",
                "--x-api-conversation-search",
                "all",
                "--x-api-conversation-pages",
                "0",
                "--x-api-conversation-page-size",
                "500",
                "--x-api-min-interval",
                "2.5",
            ],
        ):
            args = download_archive.parse_args()

        self.assertTrue(args.x_api_conversations)
        self.assertEqual(args.x_api_conversation_search, "all")
        self.assertEqual(args.x_api_conversation_pages, 0)
        self.assertEqual(args.x_api_conversation_page_size, 500)
        self.assertEqual(args.x_api_min_interval, 2.5)

    def test_parse_args_rejects_x_api_timeline_with_html_from_json(self):
        with mock.patch(
            "download_archive.sys.argv",
            ["download_archive.py", "account", "--html-from-json", "--x-api-timeline"],
        ):
            with self.assertRaises(SystemExit):
                download_archive.parse_args()

    def test_load_env_file_sets_values_without_overriding_existing_environment(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "X_BEARER_TOKEN=file-bearer\n"
                "export X_API_KEY='file-key'\n"
                "X_ACCESS_TOKEN=\"file-access\"\n",
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"X_BEARER_TOKEN": "existing-bearer"}, clear=False):
                loaded = download_archive.load_env_file(env_path)

                self.assertEqual(loaded, 2)
                self.assertEqual(os.environ["X_BEARER_TOKEN"], "existing-bearer")
                self.assertEqual(os.environ["X_API_KEY"], "file-key")
                self.assertEqual(os.environ["X_ACCESS_TOKEN"], "file-access")

    def test_get_with_retry_falls_back_to_http_for_wayback_connection_error(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b"[]"
        fake_session = mock.Mock()
        fake_session.get.side_effect = [
            requests.exceptions.ConnectionError("connection refused"),
            response,
        ]

        with mock.patch.object(download_archive, "get_session", return_value=fake_session):
            result = download_archive.get_with_retry("https://web.archive.org/cdx/search/cdx", timeout=1)

        self.assertIs(result, response)
        self.assertEqual(fake_session.get.call_args_list[0].args[0], "https://web.archive.org/cdx/search/cdx")
        self.assertEqual(fake_session.get.call_args_list[1].args[0], "http://web.archive.org/cdx/search/cdx")

    def test_wait_for_x_api_rate_limit_serializes_x_api_requests(self):
        original_interval = download_archive.X_API_MIN_INTERVAL_SECONDS
        original_last_request_at = download_archive.X_API_LAST_REQUEST_AT
        try:
            download_archive.set_x_api_min_interval(2.0)
            download_archive.X_API_LAST_REQUEST_AT = 10.0
            with mock.patch("download_archive.time.monotonic", side_effect=[11.0, 12.0]):
                with mock.patch("download_archive.time.sleep") as sleep:
                    download_archive.wait_for_x_api_rate_limit("https://api.x.com/2/tweets/search/all")

            sleep.assert_called_once_with(1.0)
            self.assertEqual(download_archive.X_API_LAST_REQUEST_AT, 12.0)
        finally:
            download_archive.set_x_api_min_interval(original_interval)
            download_archive.X_API_LAST_REQUEST_AT = original_last_request_at

    def test_get_with_retry_honors_retry_after_for_429(self):
        rate_limited = requests.Response()
        rate_limited.status_code = 429
        rate_limited.url = "https://api.x.com/2/test"
        rate_limited.headers["Retry-After"] = "3"
        success = requests.Response()
        success.status_code = 200
        success.url = "https://api.x.com/2/test"
        success._content = b"{}"
        fake_session = mock.Mock()
        fake_session.get.side_effect = [rate_limited, success]
        original_interval = download_archive.X_API_MIN_INTERVAL_SECONDS
        try:
            download_archive.set_x_api_min_interval(0)
            with mock.patch.object(download_archive, "get_session", return_value=fake_session):
                with mock.patch("download_archive.time.sleep") as sleep:
                    result = download_archive.get_with_retry("https://api.x.com/2/test", timeout=1)

            self.assertIs(result, success)
            self.assertEqual(sleep.call_args_list[0].args[0], 3.0)
        finally:
            download_archive.set_x_api_min_interval(original_interval)

    def test_refresh_snapshot_media_urls_from_x_api_updates_stale_media_url(self):
        stale_payload = {
            "data": {
                "id": "100",
                "attachments": {"media_keys": ["3_abc"]},
                "entities": {
                    "urls": [
                        {
                            "media_key": "3_abc",
                            "url": "https://t.co/x",
                            "expanded_url": "https://x.com/account/status/100/photo/1",
                            "display_url": "pic.x.com/x",
                        }
                    ]
                },
                "text": "photo https://t.co/x",
            },
            "includes": {
                "media": [
                    {"media_key": "3_abc", "type": "photo", "url": "https://pbs.twimg.com/media/stale.jpg"}
                ]
            },
        }
        fresh_payload = {
            "data": {"id": "100"},
            "includes": {
                "media": [
                    {"media_key": "3_abc", "type": "photo", "url": "https://pbs.twimg.com/media/fresh.jpg"}
                ]
            },
        }
        auth = download_archive.XApiAuth(bearer_token="bearer")
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "20260101000000_100.json"
            json_path.write_text(json.dumps(stale_payload), encoding="utf-8")
            record = download_archive.SnapshotRecord(
                timestamp="20260101000000",
                original_url="https://twitter.com/account/status/100",
                json_path=json_path,
            )
            with mock.patch(
                "download_archive.fetch_x_api_tweet_payload_with_auths",
                return_value=(fresh_payload, "", "x_api_bearer"),
            ):
                changed = download_archive.refresh_snapshot_media_urls_from_x_api(record, [auth])

            updated = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertTrue(changed)
            self.assertEqual(updated["includes"]["media"][0]["url"], "https://pbs.twimg.com/media/fresh.jpg")
            self.assertEqual(updated["data"]["entities"]["urls"][0]["url"], "https://t.co/x")
            self.assertNotIn("type", updated["data"]["entities"]["urls"][0])
            self.assertIn("media_url_refreshed_at", updated["_pale_fire"])
            self.assertEqual(updated["_pale_fire"]["media_url_refresh_source"], "x_api_lookup")

    def test_refresh_snapshot_media_urls_from_local_index_updates_stale_media_url(self):
        stale_payload = {
            "data": {
                "id": "100",
                "attachments": {"media_keys": ["3_abc"]},
                "text": "photo",
            },
            "includes": {
                "media": [
                    {"media_key": "3_abc", "type": "photo", "url": "https://pbs.twimg.com/media/stale.jpg"}
                ]
            },
        }
        fresh_payload = {
            "data": {
                "id": "200",
                "attachments": {"media_keys": ["3_abc"]},
                "text": "photo",
            },
            "includes": {
                "media": [
                    {"media_key": "3_abc", "type": "photo", "url": "https://pbs.twimg.com/media/fresh.jpg"}
                ]
            },
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            media_dir = root / "media"
            media_dir.mkdir()
            stale_path = root / "20260101000000_100.json"
            fresh_path = root / "20260101000001_200.json"
            stale_path.write_text(json.dumps(stale_payload), encoding="utf-8")
            fresh_path.write_text(json.dumps(fresh_payload), encoding="utf-8")
            stale_record = download_archive.SnapshotRecord(
                timestamp="20260101000000",
                original_url="https://twitter.com/account/status/100",
                json_path=stale_path,
            )
            fresh_record = download_archive.SnapshotRecord(
                timestamp="20260101000001",
                original_url="https://twitter.com/account/status/200",
                json_path=fresh_path,
            )
            local_index = download_archive.collect_local_media_urls_by_key([stale_record, fresh_record])
            changed = download_archive.refresh_snapshot_media_urls_from_local_index(
                stale_record,
                {"https://pbs.twimg.com/media/stale.jpg"},
                local_index,
                {"https://pbs.twimg.com/media/stale.jpg": {"status": "404", "reason": "hard-missing"}},
                media_dir,
                {},
            )

            updated = json.loads(stale_path.read_text(encoding="utf-8"))
            self.assertTrue(changed)
            self.assertEqual(updated["includes"]["media"][0]["url"], "https://pbs.twimg.com/media/fresh.jpg")
            self.assertEqual(updated["_pale_fire"]["media_url_refresh_source"], "local_media_key_index")

    def test_get_x_api_auths_from_env_files_keeps_credentials_separate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first = Path(temp_dir) / "one.env"
            second = Path(temp_dir) / "two.env"
            first.write_text(
                "X_API_KEY=key1\n"
                "X_API_KEY_SECRET=secret1\n"
                "X_ACCESS_TOKEN=token1\n"
                "X_ACCESS_TOKEN_SECRET=token-secret1\n",
                encoding="utf-8",
            )
            second.write_text(
                "X_API_KEY=key2\n"
                "X_API_KEY_SECRET=secret2\n"
                "X_ACCESS_TOKEN=token2\n"
                "X_ACCESS_TOKEN_SECRET=token-secret2\n",
                encoding="utf-8",
            )

            auths = download_archive.get_x_api_auths_from_env_files([str(first), str(second)])

        self.assertEqual([auth.label for auth in auths], [str(first), str(second)])
        self.assertEqual([auth.oauth1.api_key for auth in auths if auth.oauth1], ["key1", "key2"])

    def test_fetch_reply_chain_payload_tries_multiple_auths_and_records_access(self):
        payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "100",
                "created_at": "2026-01-02T00:00:00.000Z",
                "text": "parent text",
            },
            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
        }
        auth_one = download_archive.XApiAuth(bearer_token="token-one", label="env.one")
        auth_two = download_archive.XApiAuth(bearer_token="token-two", label="env.two")
        access_index = download_archive.new_x_api_access_index()

        def fake_fetch(auth, tweet_id):
            if auth.label == "env.one":
                return None, "not authorized", ""
            return payload, "", "x_api_bearer"

        with mock.patch.object(download_archive, "fetch_x_api_tweet_payload_with_auth", side_effect=fake_fetch):
            result, error, source = download_archive.fetch_reply_chain_payload(
                "200",
                [auth_one, auth_two],
                download_archive.ReplyChainLookupContext(use_local_json=False, use_wayback=False),
                author_hint="parent_user",
                access_index=access_index,
            )

        self.assertIs(result, payload)
        self.assertEqual(error, "")
        self.assertEqual(source, "x_api_bearer:env.two")
        self.assertEqual(access_index["envs"]["env.one"]["accounts"]["parent_user"]["failure_count"], 1)
        self.assertEqual(access_index["envs"]["env.two"]["accounts"]["parent_user"]["success_count"], 1)

    def test_write_x_api_timeline_snapshot_records_creates_extractable_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir) / "json"
            page_payload = {
                "data": [
                    {
                        "id": "2042245750005088571",
                        "author_id": "u1",
                        "conversation_id": "2042245750005088571",
                        "created_at": "2026-04-09T14:18:24.000Z",
                        "text": "timeline text",
                    }
                ],
                "includes": {
                    "users": [
                        {
                            "id": "u1",
                            "username": "TauCeti_10700",
                            "name": "Pale Fire",
                        }
                    ]
                },
            }

            records, written_count, reused_count = download_archive.write_x_api_timeline_snapshot_records(
                "TauCeti_10700",
                [page_payload],
                json_dir,
                source="x_api_oauth1:env.pale-fire",
            )

            self.assertEqual(written_count, 1)
            self.assertEqual(reused_count, 0)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].timestamp, "20260409141824")
            data = json.loads(records[0].json_path.read_text(encoding="utf-8"))
            text, media_items, metadata, ref_tweets = download_archive.extract_tweet_content(
                data,
                records[0].original_url,
            )

        self.assertEqual(text, "timeline text")
        self.assertEqual(media_items, [])
        self.assertEqual(metadata["tweet_id"], "2042245750005088571")
        self.assertEqual(ref_tweets, [])
        self.assertEqual(data["_pale_fire"]["source"], "x_api_timeline")
        self.assertEqual(data["_pale_fire"]["source_auth"], "x_api_oauth1:env.pale-fire")

    def test_fetch_x_api_timeline_payloads_with_auths_records_successes(self):
        user_payload = {
            "data": {
                "id": "u1",
                "username": "protected_user",
                "name": "Protected",
            }
        }
        first_page = {
            "data": [
                {
                    "id": "101",
                    "author_id": "u1",
                    "conversation_id": "101",
                    "created_at": "2026-01-01T00:00:00.000Z",
                    "text": "first",
                }
            ],
            "includes": {"users": [user_payload["data"]]},
            "meta": {"result_count": 1, "next_token": "next"},
        }
        second_page = {
            "data": [
                {
                    "id": "100",
                    "author_id": "u1",
                    "conversation_id": "100",
                    "created_at": "2025-12-31T00:00:00.000Z",
                    "text": "second",
                }
            ],
            "includes": {"users": [user_payload["data"]]},
            "meta": {"result_count": 1},
        }
        auth = download_archive.XApiAuth(bearer_token="token", label="env.one")
        access_index = download_archive.new_x_api_access_index()

        with mock.patch.object(download_archive, "fetch_x_api_user_by_username_with_auth", return_value=(user_payload, "", "x_api_bearer")):
            with mock.patch.object(
                download_archive,
                "fetch_x_api_timeline_page_with_auth",
                side_effect=[(first_page, "", "x_api_bearer"), (second_page, "", "x_api_bearer")],
            ) as fetch_page:
                payloads, error, source = download_archive.fetch_x_api_timeline_payloads_with_auths(
                    "protected_user",
                    [auth],
                    pages=2,
                    page_size=100,
                    access_index=access_index,
                )

        self.assertEqual(error, "")
        self.assertEqual(source, "x_api_bearer:env.one")
        self.assertEqual(len(payloads), 2)
        self.assertEqual(fetch_page.call_args_list[1].kwargs["pagination_token"], "next")
        account = access_index["envs"]["env.one"]["accounts"]["protected_user"]
        self.assertGreaterEqual(account["success_count"], 3)

    def test_fetch_x_api_timeline_pages_zero_fetches_until_no_next_token(self):
        user_payload = {
            "data": {
                "id": "u1",
                "username": "protected_user",
                "name": "Protected",
            }
        }
        pages = [
            {
                "data": [{"id": "102", "author_id": "u1", "created_at": "2026-01-02T00:00:00.000Z", "text": "one"}],
                "includes": {"users": [user_payload["data"]]},
                "meta": {"result_count": 1, "next_token": "next-1"},
            },
            {
                "data": [{"id": "101", "author_id": "u1", "created_at": "2026-01-01T00:00:00.000Z", "text": "two"}],
                "includes": {"users": [user_payload["data"]]},
                "meta": {"result_count": 1, "next_token": "next-2"},
            },
            {
                "data": [{"id": "100", "author_id": "u1", "created_at": "2025-12-31T00:00:00.000Z", "text": "three"}],
                "includes": {"users": [user_payload["data"]]},
                "meta": {"result_count": 1},
            },
        ]
        auth = download_archive.XApiAuth(bearer_token="token", label="env.one")

        with mock.patch.object(download_archive, "fetch_x_api_user_by_username_with_auth", return_value=(user_payload, "", "x_api_bearer")):
            with mock.patch.object(
                download_archive,
                "fetch_x_api_timeline_page_with_auth",
                side_effect=[(page, "", "x_api_bearer") for page in pages],
            ) as fetch_page:
                payloads, error, _ = download_archive.fetch_x_api_timeline_payloads_with_auths(
                    "protected_user",
                    [auth],
                    pages=0,
                    page_size=100,
                )

        self.assertEqual(error, "")
        self.assertEqual(len(payloads), 3)
        self.assertEqual(fetch_page.call_args_list[0].kwargs["pagination_token"], "")
        self.assertEqual(fetch_page.call_args_list[1].kwargs["pagination_token"], "next-1")
        self.assertEqual(fetch_page.call_args_list[2].kwargs["pagination_token"], "next-2")

    def test_fetch_x_api_conversation_payloads_uses_app_only_bearer_from_oauth1(self):
        class FakeTokenResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"token_type": "bearer", "access_token": "app-bearer"}

        class FakeSearchResponse:
            def json(self):
                return {
                    "data": [
                        {
                            "id": "201",
                            "author_id": "u2",
                            "conversation_id": "100",
                            "created_at": "2026-01-02T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                            "text": "reply",
                        }
                    ],
                    "includes": {"users": [{"id": "u2", "username": "reader", "name": "Reader"}]},
                    "meta": {"result_count": 1},
                }

        auth = download_archive.XApiAuth(
            oauth1=download_archive.XApiOAuth1Credentials(
                api_key="key",
                api_key_secret="secret",
                access_token="token",
                access_token_secret="token-secret",
            ),
            label="env.one",
        )
        original_cache = dict(download_archive.X_API_APP_BEARER_CACHE)
        try:
            download_archive.X_API_APP_BEARER_CACHE.clear()
            with mock.patch("download_archive.requests.post", return_value=FakeTokenResponse()) as post:
                with mock.patch("download_archive.get_with_retry", return_value=FakeSearchResponse()) as get_with_retry:
                    payloads, error, source = download_archive.fetch_x_api_conversation_payloads_with_auths(
                        "100",
                        [auth],
                        search_mode="all",
                        pages=0,
                        page_size=500,
                    )

            self.assertEqual(error, "")
            self.assertEqual(source, "x_api_app_only:env.one")
            self.assertEqual(len(payloads), 1)
            post.assert_called_once()
            headers = get_with_retry.call_args.kwargs["headers"]
            params = get_with_retry.call_args.kwargs["params"]
            self.assertEqual(headers["Authorization"], "Bearer app-bearer")
            self.assertEqual(params["query"], "conversation_id:100 -is:retweet")
            self.assertEqual(params["max_results"], "500")
        finally:
            download_archive.X_API_APP_BEARER_CACHE.clear()
            download_archive.X_API_APP_BEARER_CACHE.update(original_cache)

    def test_write_x_api_conversation_snapshot_records_uses_reply_author_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir) / "json"
            page_payload = {
                "data": [
                    {
                        "id": "201",
                        "author_id": "u2",
                        "conversation_id": "100",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                        "text": "@account reply",
                    }
                ],
                "includes": {
                    "users": [
                        {"id": "u2", "username": "reader", "name": "Reader"},
                    ]
                },
            }

            records, written_count, reused_count = download_archive.write_x_api_conversation_snapshot_records(
                "account",
                [page_payload],
                json_dir,
                source="x_api_app_only:env.one",
                search_mode="all",
            )

            self.assertEqual(written_count, 1)
            self.assertEqual(reused_count, 0)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].original_url, "https://twitter.com/reader/status/201")
            data = json.loads(records[0].json_path.read_text(encoding="utf-8"))

        self.assertEqual(data["_pale_fire"]["source"], "x_api_conversation_search")
        self.assertEqual(data["_pale_fire"]["conversation_id"], "100")
        self.assertEqual(data["_pale_fire"]["conversation_search"], "all")

    def test_collect_root_conversation_ids_skips_replies(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            root_path = json_dir / "20260101000000_100.json"
            reply_path = json_dir / "20260102000000_201.json"
            root_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "100",
                            "author_id": "u1",
                            "conversation_id": "100",
                            "created_at": "2026-01-01T00:00:00.000Z",
                            "text": "root",
                        }
                    }
                ),
                encoding="utf-8",
            )
            reply_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "201",
                            "author_id": "u2",
                            "conversation_id": "100",
                            "created_at": "2026-01-02T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                            "text": "@account reply",
                        }
                    }
                ),
                encoding="utf-8",
            )
            records = [
                download_archive.SnapshotRecord("20260101000000", "https://twitter.com/account/status/100", root_path),
                download_archive.SnapshotRecord("20260102000000", "https://twitter.com/reader/status/201", reply_path),
            ]

            conversation_ids = download_archive.collect_root_conversation_ids(records)

        self.assertEqual(conversation_ids, ["100"])

    def test_attach_local_comment_sections_uses_child_replies(self):
        parent = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:100 --><!-- pale-fire-expand:100 --></div>",
            dt=download_archive.datetime(2026, 1, 1),
            time_text="2026-01-01 00:00:00 UTC",
            body_text="parent",
            body_length=6,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/100",
            index=0,
            tweet_id="100",
            comments_placeholder="<!-- pale-fire-comments:100 -->",
            expand_placeholder="<!-- pale-fire-expand:100 -->",
        )
        child = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:200 --><!-- pale-fire-expand:200 --></div>",
            dt=download_archive.datetime(2026, 1, 2),
            time_text="2026-01-02 00:00:00 UTC",
            body_text="child",
            body_length=5,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/200",
            index=1,
            tweet_id="200",
            in_reply_to_status_id="100",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>child</p></blockquote>\n",
            comments_placeholder="<!-- pale-fire-comments:200 -->",
            expand_placeholder="<!-- pale-fire-expand:200 -->",
        )

        updated = download_archive.attach_local_comment_sections([parent, child])

        self.assertNotIn("tweet-comments", updated[0].block)
        self.assertIn("展开评论 (1)", updated[0].block)
        self.assertIn("data-comment-total='1'", updated[0].deferred_comments_html)
        latest_ts = int(download_archive.datetime(2026, 1, 2, tzinfo=download_archive.timezone.utc).timestamp())
        self.assertEqual(updated[0].deferred_comment_latest_ts, latest_ts)
        self.assertIn(f"data-comment-latest-ts='{latest_ts}'", updated[0].deferred_comments_html)
        self.assertIn("tweet-comment-node", updated[0].deferred_comments_html)
        self.assertIn("<p>child</p>", updated[0].deferred_comments_html)
        self.assertNotIn("pale-fire-comments:100", updated[0].block)
        self.assertNotIn("tweet-comments", updated[1].block)

    def test_attach_local_comment_sections_nests_replies_to_comments(self):
        root = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:100 --><!-- pale-fire-expand:100 --></div>",
            dt=download_archive.datetime(2026, 1, 1),
            time_text="2026-01-01 00:00:00 UTC",
            body_text="root",
            body_length=4,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/100",
            index=0,
            tweet_id="100",
            conversation_id="100",
            comments_placeholder="<!-- pale-fire-comments:100 -->",
            expand_placeholder="<!-- pale-fire-expand:100 -->",
        )
        child = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:200 --><!-- pale-fire-expand:200 --></div>",
            dt=download_archive.datetime(2026, 1, 2),
            time_text="2026-01-02 00:00:00 UTC",
            body_text="child",
            body_length=5,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/200",
            index=1,
            tweet_id="200",
            conversation_id="100",
            in_reply_to_status_id="100",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>child</p></blockquote>\n",
            comments_placeholder="<!-- pale-fire-comments:200 -->",
            expand_placeholder="<!-- pale-fire-expand:200 -->",
        )
        grandchild = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:300 --><!-- pale-fire-expand:300 --></div>",
            dt=download_archive.datetime(2026, 1, 3),
            time_text="2026-01-03 00:00:00 UTC",
            body_text="grandchild",
            body_length=10,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/300",
            index=2,
            tweet_id="300",
            conversation_id="100",
            in_reply_to_status_id="200",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>grandchild</p></blockquote>\n",
            comments_placeholder="<!-- pale-fire-comments:300 -->",
            expand_placeholder="<!-- pale-fire-expand:300 -->",
        )

        updated = download_archive.attach_local_comment_sections([root, child, grandchild])

        root_block = updated[0].block
        self.assertIn("展开评论 (2)", root_block)
        self.assertIn("data-comment-total='2'", updated[0].deferred_comments_html)
        self.assertEqual(updated[0].deferred_comment_count, 2)
        self.assertIn("tweet-comment-children", updated[0].deferred_comments_html)
        self.assertLess(
            updated[0].deferred_comments_html.index("<p>child</p>"),
            updated[0].deferred_comments_html.index("<p>grandchild</p>"),
        )
        self.assertNotIn("pale-fire-comments:100", root_block)

    def test_attach_local_comment_sections_injects_reply_chain_parent_comments(self):
        root = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:100 --><!-- pale-fire-expand:100 --></div>",
            dt=download_archive.datetime(2026, 1, 1),
            time_text="2026-01-01 00:00:00 UTC",
            body_text="root",
            body_length=4,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/100",
            index=0,
            tweet_id="100",
            conversation_id="100",
            comments_placeholder="<!-- pale-fire-comments:100 -->",
            expand_placeholder="<!-- pale-fire-expand:100 -->",
        )
        external_parent = download_archive.ArchiveEntry(
            block="",
            dt=download_archive.datetime(2026, 1, 2),
            time_text="2026-01-02T00:00:00.000Z",
            body_text="reader comment",
            body_length=14,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/reader/status/200",
            index=1,
            tweet_id="200",
            conversation_id="100",
            in_reply_to_status_id="100",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>reader comment</p></blockquote>\n",
        )
        author_reply = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:300 --><!-- pale-fire-expand:300 --></div>",
            dt=download_archive.datetime(2026, 1, 3),
            time_text="2026-01-03 00:00:00 UTC",
            body_text="author reply",
            body_length=12,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/300",
            index=2,
            tweet_id="300",
            conversation_id="100",
            in_reply_to_status_id="200",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>author reply</p></blockquote>\n",
            comments_placeholder="<!-- pale-fire-comments:300 -->",
            expand_placeholder="<!-- pale-fire-expand:300 -->",
            comment_parent_entries=(external_parent,),
        )

        updated = download_archive.attach_local_comment_sections([root, author_reply])

        root_block = updated[0].block
        self.assertIn("展开评论 (2)", root_block)
        self.assertIn("data-comment-total='2'", updated[0].deferred_comments_html)
        self.assertIn("<p>reader comment</p>", updated[0].deferred_comments_html)
        self.assertIn("<p>author reply</p>", updated[0].deferred_comments_html)
        self.assertLess(
            updated[0].deferred_comments_html.index("<p>reader comment</p>"),
            updated[0].deferred_comments_html.index("<p>author reply</p>"),
        )
        self.assertIn("tweet-comment-children", updated[0].deferred_comments_html)

    def test_attach_local_comment_sections_falls_back_to_conversation_root(self):
        root = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:100 --><!-- pale-fire-expand:100 --></div>",
            dt=download_archive.datetime(2026, 1, 1),
            time_text="2026-01-01 00:00:00 UTC",
            body_text="root",
            body_length=4,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/100",
            index=0,
            tweet_id="100",
            conversation_id="100",
            comments_placeholder="<!-- pale-fire-comments:100 -->",
            expand_placeholder="<!-- pale-fire-expand:100 -->",
        )
        nested_reply = download_archive.ArchiveEntry(
            block="<div><!-- pale-fire-comments:300 --><!-- pale-fire-expand:300 --></div>",
            dt=download_archive.datetime(2026, 1, 3),
            time_text="2026-01-03 00:00:00 UTC",
            body_text="nested",
            body_length=6,
            entropy=1.0,
            has_media=False,
            original_url="https://twitter.com/u/status/300",
            index=2,
            tweet_id="300",
            conversation_id="100",
            in_reply_to_status_id="200",
            comment_html="<blockquote class='tweet-ref tweet-comment'><p>nested</p></blockquote>\n",
            comments_placeholder="<!-- pale-fire-comments:300 -->",
            expand_placeholder="<!-- pale-fire-expand:300 -->",
        )

        updated = download_archive.attach_local_comment_sections([root, nested_reply])

        self.assertIn("展开评论 (1)", updated[0].block)
        self.assertIn("tweet-comments", updated[0].deferred_comments_html)
        self.assertIn("<p>nested</p>", updated[0].deferred_comments_html)

    def test_fetch_reply_chain_uses_oauth1_user_context_before_bearer(self):
        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "id": "900000000000000001",
                        "author_id": "u1",
                        "conversation_id": "900000000000000001",
                        "created_at": "2026-01-01T00:00:00.000Z",
                        "text": "oauth parent text",
                    },
                    "includes": {"users": [{"id": "u1", "username": "protected_user", "name": "Protected"}]},
                }

        auth = download_archive.XApiAuth(
            bearer_token="bearer-token",
            oauth1=download_archive.XApiOAuth1Credentials(
                api_key="api-key",
                api_key_secret="api-secret",
                access_token="access-token",
                access_token_secret="access-secret",
            ),
        )
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            with mock.patch("download_archive.get_with_retry", return_value=FakeResponse()) as get_with_retry:
                payload, error, source = download_archive.fetch_reply_chain_payload(
                    "900000000000000001",
                    auth,
                    download_archive.ReplyChainLookupContext(use_local_json=False, use_wayback=False),
                )

            headers = get_with_retry.call_args.kwargs["headers"]
            self.assertEqual(error, "")
            self.assertEqual(source, "x_api_oauth1")
            self.assertEqual(payload["data"]["text"], "oauth parent text")
            self.assertTrue(headers["Authorization"].startswith("OAuth "))
            self.assertIn("oauth_consumer_key=\"api-key\"", headers["Authorization"])
            self.assertIn("oauth_token=\"access-token\"", headers["Authorization"])
            self.assertNotIn("Bearer", headers["Authorization"])
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_extract_note_tweet_text_supports_flat_note_tweet_text(self):
        container = {
            "text": "truncated",
            "note_tweet": {
                "text": "full note tweet body",
            },
        }

        self.assertEqual(download_archive._extract_note_tweet_text(container), "full note tweet body")

    def test_extract_tweet_content_prefers_flat_note_tweet_text(self):
        data = {
            "data": {
                "id": "2022389622991687851",
                "author_id": "1758454063535198208",
                "conversation_id": "2022389622991687851",
                "created_at": "2026-02-13T19:17:14.000Z",
                "attachments": {},
                "text": "关于对huanli近期状况及今后情况的说明。\n\n110 与",
                "note_tweet": {
                    "text": "关于对huanli近期状况及今后情况的说明。\n\n110 与 120 联动，120 也到达了现场进行抢救。"
                },
            }
        }

        text, media_items, metadata, ref_tweets = download_archive.extract_tweet_content(
            data,
            "https://twitter.com/huanli233/status/2022389622991687851",
        )

        self.assertEqual(
            text,
            "关于对huanli近期状况及今后情况的说明。\n\n110 与 120 联动，120 也到达了现场进行抢救。",
        )
        self.assertEqual(media_items, [])
        self.assertEqual(metadata["tweet_id"], "2022389622991687851")
        self.assertEqual(ref_tweets, [])

    def test_extract_tweet_content_marks_missing_replied_to_tweet(self):
        data = {
            "data": {
                "id": "2009527713267331432",
                "author_id": "1741993395910811648",
                "conversation_id": "2009525563116196260",
                "created_at": "2026-01-09T07:28:36.000Z",
                "entities": {
                    "mentions": [
                        {"start": 0, "end": 14, "username": "Kumamushi2021", "id": "1437034228768595977"}
                    ]
                },
                "in_reply_to_user_id": "1437034228768595977",
                "referenced_tweets": [{"type": "replied_to", "id": "2009525563116196260"}],
                "text": "@Kumamushi2021 好耶！！我将和3d眩晕自由搏击（",
            },
            "includes": {
                "users": [
                    {
                        "id": "1437034228768595977",
                        "username": "Kumamushi2021",
                        "name": "🥛💤",
                    }
                ],
                "tweets": [
                    {
                        "id": "2009527713267331432",
                        "author_id": "1741993395910811648",
                        "conversation_id": "2009525563116196260",
                        "in_reply_to_user_id": "1437034228768595977",
                        "referenced_tweets": [{"type": "replied_to", "id": "2009525563116196260"}],
                        "text": "@Kumamushi2021 好耶！！我将和3d眩晕自由搏击（",
                    }
                ],
            },
            "errors": [
                {
                    "resource_id": "2009525563116196260",
                    "title": "Authorization Error",
                    "detail": "Sorry, you are not authorized to see the Tweet with referenced_tweets.id: [2009525563116196260].",
                }
            ],
        }

        _, _, metadata, ref_tweets = download_archive.extract_tweet_content(
            data,
            "https://twitter.com/AnIncandescence/status/2009527713267331432",
        )

        self.assertEqual(metadata["is_reply"], "true")
        self.assertEqual(metadata["in_reply_to_status_id"], "2009525563116196260")
        self.assertEqual(len(ref_tweets), 1)
        self.assertEqual(ref_tweets[0].kind, "replied_to")
        self.assertEqual(ref_tweets[0].author, "Kumamushi2021")
        self.assertEqual(ref_tweets[0].url, "https://twitter.com/Kumamushi2021/status/2009525563116196260")
        self.assertIn("not authorized", ref_tweets[0].unavailable_reason)

    def test_fetch_x_api_reply_chain_uses_cached_payloads(self):
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(
                {
                    "parent": (
                        {
                            "data": {
                                "id": "parent",
                                "author_id": "u2",
                                "conversation_id": "root",
                                "created_at": "2026-01-02T00:00:00.000Z",
                                "referenced_tweets": [{"type": "replied_to", "id": "root"}],
                                "text": "@root parent text",
                            },
                            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
                        },
                        "",
                    ),
                    "root": (
                        {
                            "data": {
                                "id": "root",
                                "author_id": "u1",
                                "conversation_id": "root",
                                "created_at": "2026-01-01T00:00:00.000Z",
                                "text": "root text",
                            },
                            "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
                        },
                        "",
                    ),
                }
            )

            ref_tweets, error = download_archive.fetch_x_api_reply_chain("parent", "fake-token", 5)

            self.assertEqual(error, "")
            self.assertEqual([tweet.text for tweet in ref_tweets], ["@root parent text", "root text"])
            self.assertEqual([tweet.author for tweet in ref_tweets], ["parent_user", "root_user"])
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_extract_tweet_content_extends_existing_reply_with_x_api_parent_chain(self):
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(
                {
                    "root": (
                        {
                            "data": {
                                "id": "root",
                                "author_id": "u1",
                                "conversation_id": "root",
                                "created_at": "2026-01-01T00:00:00.000Z",
                                "text": "root text",
                            },
                            "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
                        },
                        "",
                    )
                }
            )
            data = {
                "data": {
                    "id": "child",
                    "author_id": "u3",
                    "conversation_id": "root",
                    "created_at": "2026-01-03T00:00:00.000Z",
                    "referenced_tweets": [{"type": "replied_to", "id": "parent"}],
                    "text": "@parent_user child text",
                },
                "includes": {
                    "users": [
                        {"id": "u2", "username": "parent_user", "name": "Parent"},
                        {"id": "u3", "username": "child_user", "name": "Child"},
                    ],
                    "tweets": [
                        {
                            "id": "parent",
                            "author_id": "u2",
                            "conversation_id": "root",
                            "created_at": "2026-01-02T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "root"}],
                            "text": "@root_user parent text",
                        }
                    ],
                },
            }

            changed = []
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/child",
                x_bearer_token="fake-token",
                x_api_reply_chain_depth=2,
                x_api_enrichment_changed=changed,
            )

            self.assertEqual([tweet.text for tweet in ref_tweets], ["@root_user parent text", "root text"])
            self.assertEqual([tweet.author for tweet in ref_tweets], ["parent_user", "root_user"])
            self.assertTrue(changed)
            chain = data["_pale_fire"]["x_api_reply_chains"]["root"]
            self.assertEqual(chain["posts"][0]["id"], "root")

            download_archive.X_API_LOOKUP_CACHE.clear()
            _, _, _, cached_ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/child",
            )
            self.assertEqual([tweet.text for tweet in cached_ref_tweets], ["@root_user parent text", "root text"])
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_extract_tweet_content_enriches_local_reply_media_from_x_api(self):
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(
                {
                    "200": (
                        {
                            "data": {
                                "id": "200",
                                "author_id": "u2",
                                "conversation_id": "200",
                                "created_at": "2026-01-02T00:00:00.000Z",
                                "attachments": {"media_keys": ["3_200"]},
                                "text": "parent with photo https://t.co/photo",
                            },
                            "includes": {
                                "users": [{"id": "u2", "username": "parent_user", "name": "Parent"}],
                                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/photo.jpg"}],
                            },
                        },
                        "",
                    )
                }
            )
            data = {
                "data": {
                    "id": "300",
                    "author_id": "u3",
                    "conversation_id": "200",
                    "created_at": "2026-01-03T00:00:00.000Z",
                    "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                    "text": "@parent_user child text",
                },
                "includes": {
                    "users": [
                        {"id": "u2", "username": "parent_user", "name": "Parent"},
                        {"id": "u3", "username": "child_user", "name": "Child"},
                    ],
                    "tweets": [
                        {
                            "id": "200",
                            "author_id": "u2",
                            "conversation_id": "200",
                            "created_at": "2026-01-02T00:00:00.000Z",
                            "attachments": {"media_keys": ["3_200"]},
                            "entities": {
                                "urls": [
                                    {
                                        "url": "https://t.co/photo",
                                        "expanded_url": "https://twitter.com/parent_user/status/200/photo/1",
                                        "display_url": "pic.x.com/photo",
                                        "media_key": "3_200",
                                    }
                                ]
                            },
                            "text": "parent with photo https://t.co/photo",
                        }
                    ],
                },
            }

            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token="fake-token",
            )

            self.assertEqual(len(ref_tweets), 1)
            self.assertEqual(ref_tweets[0].media[0].url, "https://pbs.twimg.com/media/photo.jpg")
            cached = data["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]
            self.assertEqual(cached["source"], "x_api_bearer")
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_extract_tweet_content_falls_back_to_wayback_when_x_api_has_no_media(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }
        x_api_payload_without_media = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "parent with photo https://t.co/photo",
            },
            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
        }
        wayback_payload_with_media = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "parent with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "parent_user", "name": "Parent"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/wayback.jpg"}],
            },
        }

        with mock.patch.object(
            download_archive,
            "fetch_x_api_tweet_payload_with_auth",
            return_value=(x_api_payload_without_media, "", "x_api_oauth1"),
        ) as x_api_fetch, mock.patch.object(
            download_archive,
            "fetch_wayback_tweet_payload",
            return_value=(wayback_payload_with_media, "", "wayback:20260102000000:https://twitter.com/parent_user/status/200"),
        ) as wayback_fetch:
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token=download_archive.XApiAuth(
                    oauth1=download_archive.XApiOAuth1Credentials("key", "secret", "token", "token-secret")
                ),
            )

        self.assertEqual(ref_tweets[0].media[0].url, "https://pbs.twimg.com/media/wayback.jpg")
        self.assertEqual(data["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]["source"], "wayback:20260102000000:https://twitter.com/parent_user/status/200")
        x_api_fetch.assert_called_once()
        wayback_fetch.assert_called_once()

    def test_extract_tweet_content_enriches_quoted_media_from_x_api(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "300",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "quoted", "id": "200"}],
                "text": "quote text https://t.co/quote",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "quoted_user", "name": "Quoted"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "quoted with photo https://t.co/photo",
                    }
                ],
            },
        }
        x_api_payload_with_media = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "quoted with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "quoted_user", "name": "Quoted"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/quoted.jpg"}],
            },
        }

        with mock.patch.object(
            download_archive,
            "fetch_x_api_tweet_payload_with_auth",
            return_value=(x_api_payload_with_media, "", "x_api_oauth1"),
        ) as x_api_fetch:
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token=download_archive.XApiAuth(
                    oauth1=download_archive.XApiOAuth1Credentials("key", "secret", "token", "token-secret")
                ),
            )

        self.assertEqual(ref_tweets[0].kind, "quoted")
        self.assertEqual(ref_tweets[0].media[0].url, "https://pbs.twimg.com/media/quoted.jpg")
        self.assertEqual(data["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]["payload"]["includes"]["media"][0]["url"], "https://pbs.twimg.com/media/quoted.jpg")
        x_api_fetch.assert_called_once()

    def test_extract_tweet_content_enriches_quoted_media_from_local_json_before_network(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "300",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "quoted", "id": "200"}],
                "text": "quote text https://t.co/quote",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "quoted_user", "name": "Quoted"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "quoted with photo https://t.co/photo",
                    }
                ],
            },
        }
        local_payload_with_media = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "quoted with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "quoted_user", "name": "Quoted"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/local.jpg"}],
            },
        }
        local_payload_without_media = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "300",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "quoted", "id": "200"}],
                "text": "quote text https://t.co/quote",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "quoted_user", "name": "Quoted"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "quoted with photo https://t.co/photo",
                    }
                ],
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            (json_dir / "20260103000000_300.json").write_text(
                json.dumps(local_payload_without_media),
                encoding="utf-8",
            )
            (json_dir / "20260102000000_200.json").write_text(
                json.dumps(local_payload_with_media),
                encoding="utf-8",
            )
            with mock.patch.object(download_archive, "fetch_x_api_tweet_payload_with_auth") as x_api_fetch, mock.patch.object(
                download_archive,
                "fetch_wayback_tweet_payload",
            ) as wayback_fetch:
                _, _, _, ref_tweets = download_archive.extract_tweet_content(
                    data,
                    "https://twitter.com/child_user/status/300",
                    x_bearer_token=download_archive.XApiAuth(
                        oauth1=download_archive.XApiOAuth1Credentials("key", "secret", "token", "token-secret")
                    ),
                    reply_chain_lookup_context=download_archive.ReplyChainLookupContext(
                        json_dir=json_dir,
                        snapshot_timestamp="20260103000000",
                    ),
                    allow_reply_chain_media_lookup=False,
                )

        self.assertEqual(ref_tweets[0].kind, "quoted")
        self.assertEqual(ref_tweets[0].media[0].url, "https://pbs.twimg.com/media/local.jpg")
        self.assertEqual(data["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]["source"], "local_json")
        x_api_fetch.assert_not_called()
        wayback_fetch.assert_not_called()

    def test_extract_tweet_content_does_not_network_lookup_media_when_disabled(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }

        with mock.patch.object(download_archive, "fetch_x_api_tweet_payload_with_auth") as x_api_fetch, mock.patch.object(
            download_archive,
            "fetch_wayback_tweet_payload",
        ) as wayback_fetch:
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token=download_archive.XApiAuth(
                    oauth1=download_archive.XApiOAuth1Credentials("key", "secret", "token", "token-secret")
                ),
                allow_reply_chain_media_lookup=False,
            )

        self.assertEqual(len(ref_tweets), 1)
        self.assertEqual(ref_tweets[0].media, (download_archive.TweetMediaItem(kind="image", url=""),))
        x_api_fetch.assert_not_called()
        wayback_fetch.assert_not_called()

    def test_extract_tweet_content_ignores_referenced_tweet_stub_when_targeting_parent(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "text": "parent full text",
                    }
                ],
            },
        }

        text, _, metadata, _ = download_archive.extract_tweet_content(
            data,
            "https://twitter.com/i/status/200",
        )

        self.assertEqual(text, "parent full text")
        self.assertEqual(metadata["tweet_id"], "200")

    def test_wayback_tweet_lookup_queries_do_not_use_i_status(self):
        queries = download_archive.build_wayback_tweet_lookup_queries("200", "@parent_user")

        self.assertEqual(
            queries,
            [
                "twitter.com/parent_user/status/200",
                "x.com/parent_user/status/200",
                "mobile.twitter.com/parent_user/status/200",
                "twitter.com/*/status/200",
                "x.com/*/status/200",
                "mobile.twitter.com/*/status/200",
            ],
        )
        self.assertFalse(any("/i/status/" in query for query in queries))

    def test_reply_chain_wayback_author_hint_uses_leading_reply_mention(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "  @parent_user child text",
            },
            "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
        }

        with mock.patch.object(
            download_archive,
            "fetch_wayback_tweet_payload",
            return_value=(None, "missing", "wayback"),
        ) as wayback_fetch:
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_api_reply_chain_depth=1,
                reply_chain_lookup_context=download_archive.ReplyChainLookupContext(
                    use_local_json=False,
                    use_wayback=True,
                ),
            )

        wayback_fetch.assert_called_once_with("200", "parent_user", "")
        self.assertEqual(ref_tweets[0].author, "parent_user")

    def test_load_cached_reply_chain_recurses_into_nested_payload_cache(self):
        root_payload = {
            "data": {
                "id": "100",
                "author_id": "u1",
                "conversation_id": "100",
                "created_at": "2026-01-01T00:00:00.000Z",
                "text": "root post",
            },
            "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
        }
        parent_payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "100",
                "created_at": "2026-01-01T01:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                "text": "@root_user parent reply",
            },
            "includes": {
                "users": [
                    {"id": "u1", "username": "root_user", "name": "Root"},
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                ]
            },
            "_pale_fire": {
                "x_api_reply_chains": {
                    "100": {
                        "version": 1,
                        "fetched_at": "2026-04-18T00:00:00Z",
                        "max_depth": 1,
                        "posts": [{"id": "100", "source": "local_json", "payload": root_payload}],
                    }
                }
            },
        }
        data = {
            "_pale_fire": {
                "x_api_reply_chains": {
                    "200": {
                        "version": 1,
                        "fetched_at": "2026-04-18T00:00:00Z",
                        "max_depth": 2,
                        "posts": [{"id": "200", "source": "local_json", "payload": parent_payload}],
                    }
                }
            }
        }

        with mock.patch.object(download_archive, "fetch_wayback_tweet_payload") as wayback_fetch:
            refs, depth, _, _ = download_archive._load_cached_x_api_reply_chain(data, "200", 8)

        self.assertEqual([ref.tweet_id for ref in refs], ["200", "100"])
        self.assertEqual([ref.text for ref in refs], ["@root_user parent reply", "root post"])
        self.assertGreaterEqual(depth, 2)
        wayback_fetch.assert_not_called()

    def test_extract_tweet_content_skips_x_api_and_uses_wayback_without_auth(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }
        wayback_payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "parent with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "parent_user", "name": "Parent"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/wayback.jpg"}],
            },
        }

        with mock.patch.object(download_archive, "fetch_x_api_tweet_payload_with_auth") as x_api_fetch, mock.patch.object(
            download_archive,
            "fetch_wayback_tweet_payload",
            return_value=(wayback_payload, "", "wayback:20260102000000:https://twitter.com/parent_user/status/200"),
        ) as wayback_fetch:
            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token="",
            )

        self.assertEqual(ref_tweets[0].media[0].url, "https://pbs.twimg.com/media/wayback.jpg")
        x_api_fetch.assert_not_called()
        wayback_fetch.assert_called_once()

    def test_extract_tweet_content_records_wayback_media_lookup_failure(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }
        changed = []

        with mock.patch.object(
            download_archive,
            "fetch_wayback_tweet_payload",
            return_value=(None, "Wayback fallback did not find archived JSON for tweet id 200.", "wayback"),
        ) as wayback_fetch:
            download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_api_enrichment_changed=changed,
            )

        cache_entry = data["_pale_fire"]["x_api_reply_chains"]["200"]
        self.assertTrue(changed)
        self.assertEqual(cache_entry["posts"], [])
        self.assertEqual(cache_entry["terminal_error"]["id"], "200")
        self.assertIn("Wayback fallback did not find archived JSON", cache_entry["terminal_error"]["message"])
        wayback_fetch.assert_called_once()

        with mock.patch.object(download_archive, "fetch_wayback_tweet_payload") as retry_wayback_fetch:
            download_archive.extract_tweet_content(data, "https://twitter.com/child_user/status/300")

        retry_wayback_fetch.assert_not_called()

    def test_collect_snapshot_media_jobs_persists_reply_chain_media_enrichment(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u2", "username": "parent_user", "name": "Parent"},
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }
        wayback_payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "parent with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "parent_user", "name": "Parent"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/wayback.jpg"}],
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "20260103000000_300.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=json_path,
            )

            with mock.patch.object(
                download_archive,
                "fetch_wayback_tweet_payload",
                return_value=(wayback_payload, "", "wayback:20260102000000:https://twitter.com/parent_user/status/200"),
            ):
                jobs = download_archive.collect_snapshot_media_jobs(record)

            persisted = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertIn(("image", "https://pbs.twimg.com/media/wayback.jpg", "20260103000000"), jobs)
        cached = persisted["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]
        self.assertEqual(cached["source"], "wayback:20260102000000:https://twitter.com/parent_user/status/200")
        self.assertEqual(cached["payload"]["includes"]["media"][0]["url"], "https://pbs.twimg.com/media/wayback.jpg")

    def test_repair_missing_media_does_not_wayback_lookup_during_job_collection(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [{"id": "u3", "username": "child_user", "name": "Child"}],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir) / "archive_assets"
            json_dir = asset_dir / "json"
            json_dir.mkdir(parents=True)
            json_path = json_dir / "20260103000000_300.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=json_path,
            )

            with mock.patch.object(download_archive, "fetch_wayback_tweet_payload") as wayback_fetch:
                total_jobs, missing_before, repaired_count, missing_after = (
                    download_archive.repair_missing_media_from_snapshot_records([record], asset_dir, workers=1)
                )

        self.assertEqual((total_jobs, missing_before, repaired_count, missing_after), (0, 0, 0, 0))
        wayback_fetch.assert_not_called()

    def test_repair_missing_media_uses_x_api_for_quoted_reference_media_without_wayback_lookup(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "300",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "quoted", "id": "200"}],
                "text": "quote text https://t.co/quote",
            },
            "includes": {
                "users": [{"id": "u3", "username": "child_user", "name": "Child"}],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "quoted with photo https://t.co/photo",
                    }
                ],
            },
        }
        x_api_payload_with_media = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "200",
                "created_at": "2026-01-02T00:00:00.000Z",
                "attachments": {"media_keys": ["3_200"]},
                "text": "quoted with photo https://t.co/photo",
            },
            "includes": {
                "users": [{"id": "u2", "username": "quoted_user", "name": "Quoted"}],
                "media": [{"media_key": "3_200", "type": "photo", "url": "https://pbs.twimg.com/media/quoted.jpg"}],
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            asset_dir = Path(temp_dir) / "archive_assets"
            json_dir = asset_dir / "json"
            json_dir.mkdir(parents=True)
            json_path = json_dir / "20260103000000_300.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=json_path,
            )

            def fake_download_image(image_url, snapshot_timestamp, media_dir, asset_dir_name, cache=None, *args, **kwargs):
                media_dir.mkdir(parents=True, exist_ok=True)
                output_path = media_dir / "quoted.jpg"
                output_path.write_bytes(b"image")
                relative_path = f"{asset_dir_name}/media/quoted.jpg"
                if cache is not None:
                    cache[image_url] = relative_path
                return relative_path

            with mock.patch.object(
                download_archive,
                "fetch_x_api_tweet_payload_with_auth",
                return_value=(x_api_payload_with_media, "", "x_api_oauth1"),
            ) as x_api_fetch, mock.patch.object(
                download_archive,
                "fetch_wayback_tweet_payload",
            ) as wayback_fetch, mock.patch.object(
                download_archive,
                "download_image_asset",
                side_effect=fake_download_image,
            ):
                result = download_archive.repair_missing_media_from_snapshot_records(
                    [record],
                    asset_dir,
                    workers=1,
                    x_bearer_token=download_archive.XApiAuth(
                        oauth1=download_archive.XApiOAuth1Credentials("key", "secret", "token", "token-secret")
                    ),
                )
            persisted = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual(result, (1, 1, 1, 0))
        self.assertEqual(persisted["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]["payload"]["includes"]["media"][0]["url"], "https://pbs.twimg.com/media/quoted.jpg")
        x_api_fetch.assert_called_once()
        wayback_fetch.assert_not_called()

    def test_extract_tweet_content_reads_persisted_x_api_reply_chain_without_token(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "100",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [
                    {"id": "u3", "username": "child_user", "name": "Child"},
                ],
            },
            "_pale_fire": {
                "x_api_reply_chains": {
                    "200": {
                        "version": 1,
                        "fetched_at": "2026-04-17T00:00:00Z",
                        "max_depth": 2,
                        "posts": [
                            {
                                "id": "200",
                                "payload": {
                                    "data": {
                                        "id": "200",
                                        "author_id": "u2",
                                        "conversation_id": "100",
                                        "created_at": "2026-01-02T00:00:00.000Z",
                                        "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                                        "text": "@root_user parent text",
                                    },
                                    "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
                                },
                            },
                            {
                                "id": "100",
                                "payload": {
                                    "data": {
                                        "id": "100",
                                        "author_id": "u1",
                                        "conversation_id": "100",
                                        "created_at": "2026-01-01T00:00:00.000Z",
                                        "text": "root text",
                                    },
                                    "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
                                },
                            },
                        ],
                    }
                }
            },
        }

        _, _, _, ref_tweets = download_archive.extract_tweet_content(
            data,
            "https://twitter.com/child_user/status/300",
        )

        self.assertEqual([tweet.text for tweet in ref_tweets], ["@root_user parent text", "root text"])
        self.assertEqual([tweet.author for tweet in ref_tweets], ["parent_user", "root_user"])

    def test_write_snapshot_json_payload_persists_enrichment(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "snapshot.json"
            data = {
                "data": {"id": "1", "text": "tweet"},
                "_pale_fire": {
                    "x_api_reply_chains": {
                        "2": {
                            "version": 1,
                            "fetched_at": "2026-04-17T00:00:00Z",
                            "max_depth": 1,
                            "posts": [{"id": "2", "payload": {"data": {"id": "2", "text": "parent"}}}],
                        }
                    }
                },
            }

            download_archive.write_snapshot_json_payload(json_path, data)
            persisted = json.loads(json_path.read_text(encoding="utf-8"))

            self.assertEqual(persisted["_pale_fire"]["x_api_reply_chains"]["2"]["posts"][0]["payload"]["data"]["text"], "parent")

    def test_repair_reply_chains_from_snapshot_records_writes_json_cache(self):
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(
                {
                    "200": (
                        {
                            "data": {
                                "id": "200",
                                "author_id": "u2",
                                "conversation_id": "100",
                                "created_at": "2026-01-02T00:00:00.000Z",
                                "text": "@root_user parent text",
                            },
                            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
                        },
                        "",
                    )
                }
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                json_path = Path(temp_dir) / "20260103000000_300.json"
                json_path.write_text(
                    json.dumps(
                        {
                            "data": {
                                "id": "300",
                                "author_id": "u3",
                                "conversation_id": "100",
                                "created_at": "2026-01-03T00:00:00.000Z",
                                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                                "text": "@parent_user child text",
                            },
                            "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                record = download_archive.SnapshotRecord(
                    timestamp="20260103000000",
                    original_url="https://twitter.com/child_user/status/300",
                    json_path=json_path,
                )

                total, candidates, updated, failed = download_archive.repair_reply_chains_from_snapshot_records(
                    [record],
                    "fake-token",
                    1,
                    1,
                )
                persisted = json.loads(json_path.read_text(encoding="utf-8"))

                self.assertEqual((total, candidates, updated, failed), (1, 1, 1, 0))
                self.assertEqual(
                    persisted["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]["payload"]["data"]["text"],
                    "@root_user parent text",
                )
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_repair_reply_chains_uses_local_json_before_x_api(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            parent_path = json_dir / "20260102000000_200.json"
            parent_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "200",
                            "author_id": "u2",
                            "conversation_id": "100",
                            "created_at": "2026-01-02T00:00:00.000Z",
                            "text": "local parent text",
                        },
                        "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            child_path = json_dir / "20260103000000_300.json"
            child_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "300",
                            "author_id": "u3",
                            "conversation_id": "100",
                            "created_at": "2026-01-03T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                            "text": "@parent_user child text",
                        },
                        "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=child_path,
            )

            with mock.patch("download_archive.fetch_x_api_tweet_payload") as fetch_x_api:
                total, candidates, updated, failed = download_archive.repair_reply_chains_from_snapshot_records(
                    [record],
                    "fake-token",
                    1,
                    1,
                )

            persisted = json.loads(child_path.read_text(encoding="utf-8"))
            post = persisted["_pale_fire"]["x_api_reply_chains"]["200"]["posts"][0]
            self.assertEqual((total, candidates, updated, failed), (1, 1, 1, 0))
            self.assertEqual(post["source"], "local_json")
            self.assertEqual(post["payload"]["data"]["text"], "local parent text")
            fetch_x_api.assert_not_called()

    def test_fetch_reply_chain_uses_local_snapshot_for_large_tweet_id_before_network(self):
        tweet_id = "1820611167137526019"
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            local_path = json_dir / f"20240806000142_{tweet_id}.json"
            local_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": tweet_id,
                            "author_id": "u1",
                            "conversation_id": "1820609609016676663",
                            "created_at": "2024-08-06T00:01:42.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "1820609609016676663"}],
                            "text": "@_mucha_000 你在土星我在火星！我们都有光明的的未来～♪",
                        },
                        "includes": {"users": [{"id": "u1", "username": "AnIncandescence", "name": "An Incandescence"}]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            context = download_archive.ReplyChainLookupContext(
                json_dir=json_dir,
                snapshot_timestamp="20240806000142",
                author_hint="AnIncandescence",
            )

            with mock.patch("download_archive.fetch_x_api_tweet_payload") as fetch_x_api:
                with mock.patch("download_archive.fetch_wayback_tweet_payload") as fetch_wayback:
                    payload, error, source = download_archive.fetch_reply_chain_payload(tweet_id, "fake-token", context)

            self.assertEqual(error, "")
            self.assertEqual(source, "local_json")
            self.assertEqual(payload["data"]["id"], tweet_id)
            fetch_x_api.assert_not_called()
            fetch_wayback.assert_not_called()

    def test_repair_reply_chains_uses_embedded_local_tweet_before_network(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            holder_path = json_dir / "20260101000000_999.json"
            holder_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "999",
                            "author_id": "u9",
                            "conversation_id": "999",
                            "created_at": "2026-01-01T00:00:00.000Z",
                            "text": "holder tweet",
                        },
                        "includes": {
                            "users": [
                                {"id": "u2", "username": "parent_user", "name": "Parent"},
                                {"id": "u9", "username": "holder_user", "name": "Holder"},
                            ],
                            "tweets": [
                                {
                                    "id": "200",
                                    "author_id": "u2",
                                    "conversation_id": "100",
                                    "created_at": "2026-01-02T00:00:00.000Z",
                                    "text": "embedded local parent text",
                                }
                            ],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            child_path = json_dir / "20260103000000_300.json"
            child_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "300",
                            "author_id": "u3",
                            "conversation_id": "100",
                            "created_at": "2026-01-03T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                            "text": "@parent_user child text",
                        },
                        "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=child_path,
            )

            with mock.patch("download_archive.fetch_x_api_tweet_payload") as fetch_x_api:
                with mock.patch("download_archive.fetch_wayback_tweet_payload") as fetch_wayback:
                    total, candidates, updated, failed = download_archive.repair_reply_chains_from_snapshot_records(
                        [record],
                        "fake-token",
                        1,
                        1,
                    )

            persisted = json.loads(child_path.read_text(encoding="utf-8"))
            cache_entry = persisted["_pale_fire"]["x_api_reply_chains"]["200"]
            cached_refs, _, _, _ = download_archive._load_cached_x_api_reply_chain(persisted, "200", 1)
            self.assertEqual((total, candidates, updated, failed), (1, 1, 1, 0))
            self.assertEqual(cache_entry["posts"][0]["source"], "local_json")
            self.assertEqual([tweet.text for tweet in cached_refs], ["embedded local parent text"])
            fetch_x_api.assert_not_called()
            fetch_wayback.assert_not_called()

    def test_fetch_reply_chain_falls_back_to_wayback_after_x_api_error(self):
        wayback_payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "100",
                "created_at": "2026-01-02T00:00:00.000Z",
                "text": "wayback parent text",
            },
            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
        }

        with mock.patch("download_archive.fetch_x_api_tweet_payload", return_value=(None, "not authorized")):
            with mock.patch(
                "download_archive.fetch_wayback_tweet_payload",
                return_value=(wayback_payload, "", "wayback:20260102000000:https://twitter.com/parent_user/status/200"),
            ) as fetch_wayback:
                payloads = []
                ref_tweets, error = download_archive.fetch_x_api_reply_chain(
                    "200",
                    "fake-token",
                    1,
                    payloads_out=payloads,
                    lookup_context=download_archive.ReplyChainLookupContext(author_hint="parent_user"),
                )

        self.assertEqual(error, "")
        self.assertEqual([tweet.text for tweet in ref_tweets], ["wayback parent text"])
        self.assertEqual(payloads[0][2], "wayback:20260102000000:https://twitter.com/parent_user/status/200")
        fetch_wayback.assert_called_once()

    def test_fetch_reply_chain_can_disable_wayback_fallback(self):
        with mock.patch("download_archive.fetch_x_api_tweet_payload", return_value=(None, "not authorized")):
            with mock.patch("download_archive.fetch_wayback_tweet_payload") as fetch_wayback:
                payloads = []
                ref_tweets, error = download_archive.fetch_x_api_reply_chain(
                    "200",
                    "fake-token",
                    1,
                    payloads_out=payloads,
                    lookup_context=download_archive.ReplyChainLookupContext(
                        author_hint="parent_user",
                        use_wayback=False,
                    ),
                )

        self.assertEqual(ref_tweets, [])
        self.assertEqual(payloads, [])
        self.assertEqual(error, "not authorized")
        fetch_wayback.assert_not_called()

    def test_fetch_reply_chain_recovers_earlier_local_conversation_posts_after_gap(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            root_payload = {
                "data": {
                    "id": "100",
                    "author_id": "u1",
                    "conversation_id": "100",
                    "created_at": "2026-01-01T00:00:00.000Z",
                    "text": "conversation root text",
                },
                "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
            }
            (json_dir / "20260101000000_100.json").write_text(
                json.dumps(root_payload, ensure_ascii=False),
                encoding="utf-8",
            )

            with mock.patch("download_archive.fetch_x_api_tweet_payload", return_value=(None, "not authorized")):
                with mock.patch("download_archive.fetch_wayback_tweet_payload") as fetch_wayback:
                    payloads = []
                    terminal_errors = []
                    ref_tweets, error = download_archive.fetch_x_api_reply_chain(
                        "200",
                        "fake-token",
                        3,
                        payloads_out=payloads,
                        terminal_errors_out=terminal_errors,
                        lookup_context=download_archive.ReplyChainLookupContext(
                            json_dir=json_dir,
                            conversation_id_hint="100",
                            before_created_at_hint="2026-01-03T00:00:00.000Z",
                            use_wayback=False,
                        ),
                    )

        self.assertEqual(error, "")
        self.assertEqual([tweet.tweet_id for tweet in ref_tweets], ["200", "100"])
        self.assertEqual(ref_tweets[0].unavailable_reason, "not authorized")
        self.assertEqual(ref_tweets[1].text, "conversation root text")
        self.assertEqual(payloads[0][0], "100")
        self.assertEqual(payloads[0][2], "conversation_recovery:local_json")
        self.assertEqual(terminal_errors, [("200", "not authorized")])
        fetch_wayback.assert_not_called()

    def test_fetch_reply_chain_allows_wayback_for_conversation_root_recovery(self):
        root_payload = {
            "data": {
                "id": "100",
                "author_id": "u1",
                "conversation_id": "100",
                "created_at": "2026-01-01T00:00:00.000Z",
                "text": "wayback root text",
            },
            "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
        }

        with mock.patch("download_archive.fetch_x_api_tweet_payload", return_value=(None, "not authorized")):
            with mock.patch(
                "download_archive.fetch_wayback_tweet_payload",
                return_value=(root_payload, "", "wayback:20260101000000:https://twitter.com/root_user/status/100"),
            ) as fetch_wayback:
                payloads = []
                ref_tweets, error = download_archive.fetch_x_api_reply_chain(
                    "200",
                    "fake-token",
                    3,
                    payloads_out=payloads,
                    lookup_context=download_archive.ReplyChainLookupContext(
                        conversation_id_hint="100",
                        before_created_at_hint="2026-01-03T00:00:00.000Z",
                        use_wayback=False,
                    ),
                )

        self.assertEqual(error, "")
        self.assertEqual([tweet.tweet_id for tweet in ref_tweets], ["200", "100"])
        self.assertEqual(ref_tweets[1].text, "wayback root text")
        self.assertEqual(payloads[0][2], "conversation_recovery:wayback:20260101000000:https://twitter.com/root_user/status/100")
        fetch_wayback.assert_called_once()
        self.assertEqual(fetch_wayback.call_args.args[0], "100")

    def test_cached_conversation_recovery_renders_gap_before_recovered_posts(self):
        root_payload = {
            "data": {
                "id": "100",
                "author_id": "u1",
                "conversation_id": "100",
                "created_at": "2026-01-01T00:00:00.000Z",
                "text": "cached root text",
            },
            "includes": {"users": [{"id": "u1", "username": "root_user", "name": "Root"}]},
        }
        data = {
            "_pale_fire": {
                "x_api_reply_chains": {
                    "200": {
                        "version": 1,
                        "fetched_at": "2026-04-18T00:00:00Z",
                        "max_depth": 3,
                        "posts": [
                            {
                                "id": "100",
                                "source": "conversation_recovery:local_json",
                                "payload": root_payload,
                            }
                        ],
                        "terminal_error": {"id": "200", "message": "not authorized"},
                        "recovery": {"method": "conversation_id_timestamp", "missing_id": "200"},
                    }
                }
            }
        }

        cached_refs, _, terminal_error_id, terminal_error = download_archive._load_cached_x_api_reply_chain(data, "200", 3)

        self.assertEqual([tweet.tweet_id for tweet in cached_refs], ["200", "100"])
        self.assertEqual(cached_refs[0].unavailable_reason, "not authorized")
        self.assertEqual(cached_refs[1].text, "cached root text")
        self.assertEqual(terminal_error_id, "")
        self.assertEqual(terminal_error, "")

    def test_repair_reply_chains_disable_parent_wayback_but_allow_conversation_root_wayback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            json_dir = Path(temp_dir)
            child_path = json_dir / "20260103000000_300.json"
            child_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "300",
                            "author_id": "u3",
                            "conversation_id": "100",
                            "created_at": "2026-01-03T00:00:00.000Z",
                            "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                            "text": "@parent_user child text",
                        },
                        "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=child_path,
            )

            with mock.patch("download_archive.fetch_x_api_tweet_payload", return_value=(None, "not authorized")):
                with mock.patch(
                    "download_archive.fetch_wayback_tweet_payload",
                    return_value=(None, "root missing", "wayback"),
                ) as fetch_wayback:
                    total, candidates, updated, failed = download_archive.repair_reply_chains_from_snapshot_records(
                        [record],
                        "fake-token",
                        1,
                        1,
                        reply_chain_use_wayback=False,
                    )

            persisted = json.loads(child_path.read_text(encoding="utf-8"))
            cache_entry = persisted["_pale_fire"]["x_api_reply_chains"]["200"]
            self.assertEqual((total, candidates, updated, failed), (1, 1, 1, 0))
            self.assertEqual(cache_entry["posts"], [])
            self.assertEqual(cache_entry["terminal_error"], {"id": "200", "message": "not authorized"})
            fetch_wayback.assert_called_once()
            self.assertEqual(fetch_wayback.call_args.args[0], "100")

    def test_repair_retries_cached_terminal_errors(self):
        original_cache = dict(download_archive.X_API_LOOKUP_CACHE)
        try:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(
                {
                    "200": (
                        {
                            "data": {
                                "id": "200",
                                "author_id": "u2",
                                "conversation_id": "100",
                                "created_at": "2026-01-02T00:00:00.000Z",
                                "text": "retried parent text",
                            },
                            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
                        },
                        "",
                    )
                }
            )
            data = {
                "data": {
                    "id": "300",
                    "author_id": "u3",
                    "conversation_id": "100",
                    "created_at": "2026-01-03T00:00:00.000Z",
                    "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                    "text": "@parent_user child text",
                },
                "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
                "_pale_fire": {
                    "x_api_reply_chains": {
                        "200": {
                            "version": 1,
                            "fetched_at": "2026-04-17T00:00:00Z",
                            "max_depth": 1,
                            "posts": [],
                            "terminal_error": {"id": "200", "message": "not authorized"},
                        }
                    }
                },
            }
            changed = []

            _, _, _, ref_tweets = download_archive.extract_tweet_content(
                data,
                "https://twitter.com/child_user/status/300",
                x_bearer_token="fake-token",
                x_api_reply_chain_depth=1,
                x_api_enrichment_changed=changed,
                x_api_retry_cached_errors=True,
            )

            self.assertEqual([tweet.text for tweet in ref_tweets], ["retried parent text"])
            self.assertTrue(changed)
            self.assertNotIn("terminal_error", data["_pale_fire"]["x_api_reply_chains"]["200"])
        finally:
            download_archive.X_API_LOOKUP_CACHE.clear()
            download_archive.X_API_LOOKUP_CACHE.update(original_cache)

    def test_repair_retries_cached_terminal_errors_without_x_token_via_wayback(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "100",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {"users": [{"id": "u3", "username": "child_user", "name": "Child"}]},
            "_pale_fire": {
                "x_api_reply_chains": {
                    "200": {
                        "version": 1,
                        "fetched_at": "2026-04-17T00:00:00Z",
                        "max_depth": 1,
                        "posts": [],
                        "terminal_error": {"id": "200", "message": "not authorized"},
                    }
                }
            },
        }
        wayback_payload = {
            "data": {
                "id": "200",
                "author_id": "u2",
                "conversation_id": "100",
                "created_at": "2026-01-02T00:00:00.000Z",
                "text": "wayback retried parent text",
            },
            "includes": {"users": [{"id": "u2", "username": "parent_user", "name": "Parent"}]},
        }
        changed = []

        with mock.patch.object(download_archive, "fetch_x_api_tweet_payload") as fetch_x_api:
            with mock.patch.object(
                download_archive,
                "fetch_wayback_tweet_payload",
                return_value=(
                    wayback_payload,
                    "",
                    "wayback:20260102000000:https://twitter.com/parent_user/status/200",
                ),
            ):
                _, _, _, ref_tweets = download_archive.extract_tweet_content(
                    data,
                    "https://twitter.com/child_user/status/300",
                    x_bearer_token="",
                    x_api_reply_chain_depth=1,
                    x_api_enrichment_changed=changed,
                    x_api_retry_cached_errors=True,
                    reply_chain_lookup_context=download_archive.ReplyChainLookupContext(author_hint="parent_user"),
                )

        fetch_x_api.assert_not_called()
        self.assertEqual([tweet.text for tweet in ref_tweets], ["wayback retried parent text"])
        self.assertTrue(changed)
        cache_entry = data["_pale_fire"]["x_api_reply_chains"]["200"]
        self.assertNotIn("terminal_error", cache_entry)
        self.assertEqual(cache_entry["posts"][0]["source"], "wayback:20260102000000:https://twitter.com/parent_user/status/200")

    def test_legacy_hard_missing_negative_cache_defaults_to_404_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            (asset_dir / "media_negative_index.json").write_text(
                json.dumps(
                    {
                        "version": download_archive.MEDIA_NEGATIVE_CACHE_VERSION,
                        "entries": {
                            "https://pbs.twimg.com/media/missing.jpg": {
                                "reason": "hard-missing",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            cache = download_archive.load_negative_media_cache(asset_dir)

        self.assertEqual(cache["https://pbs.twimg.com/media/missing.jpg"]["status"], "404")

    def test_no_wayback_capture_marks_negative_cache_as_404(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            negative_cache = {}
            failure_statuses = {}
            media_url = "https://pbs.twimg.com/media/no_capture.jpg"

            with mock.patch.object(
                download_archive,
                "build_image_candidate_urls",
                return_value=[f"https://web.archive.org/web/20260101000000im_/{media_url}"],
            ):
                with mock.patch.object(download_archive, "build_closest_capture_candidate_urls", return_value=[]):
                    with mock.patch.object(download_archive, "media_cdx_has_any_capture", return_value=False):
                        with mock.patch.object(download_archive, "get_with_retry", side_effect=RuntimeError("timeout")):
                            result = download_archive.download_image_asset(
                                media_url,
                                "20260101000000",
                                media_dir,
                                asset_dir.name,
                                cache={},
                                negative_cache=negative_cache,
                                failure_statuses=failure_statuses,
                            )

        self.assertEqual(result, "")
        self.assertEqual(failure_statuses[media_url], "404")
        self.assertEqual(negative_cache[media_url]["status"], "404")
        self.assertEqual(negative_cache[media_url]["reason"], "no-capture")

    def test_download_image_asset_can_disable_wayback_media_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            failure_statuses = {}
            media_url = "https://pbs.twimg.com/media/direct_only.jpg"

            with mock.patch.object(download_archive, "build_closest_capture_candidate_urls") as closest_lookup:
                with mock.patch.object(download_archive, "get_with_retry", side_effect=make_http_error(404)) as get_with_retry:
                    result = download_archive.download_image_asset(
                        media_url,
                        "20260101000000",
                        media_dir,
                        asset_dir.name,
                        cache={},
                        negative_cache={},
                        failure_statuses=failure_statuses,
                        allow_wayback_fallback=False,
                    )

        self.assertEqual(result, "")
        self.assertEqual(failure_statuses[media_url], "404")
        closest_lookup.assert_not_called()
        requested_urls = [call.args[0] for call in get_with_retry.call_args_list]
        self.assertTrue(requested_urls)
        self.assertTrue(all("web.archive.org" not in url for url in requested_urls))

    def test_download_image_asset_uses_existing_local_media_when_downloads_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp) / "Account_archive_assets"
            media_dir = asset_dir / "media"
            media_dir.mkdir(parents=True)
            image_path = media_dir / "100-local.jpg"
            image_path.write_bytes(b"already local")
            cache = {}
            negative_cache = {str(image_path): {"status": "404", "reason": "hard-missing"}}

            with mock.patch.object(download_archive, "get_with_retry") as get_with_retry:
                result = download_archive.download_image_asset(
                    str(image_path),
                    "20260101000000",
                    media_dir,
                    asset_dir.name,
                    cache=cache,
                    negative_cache=negative_cache,
                    download_missing=False,
                )

        self.assertEqual(result, f"{asset_dir.name}/media/{image_path.name}")
        self.assertEqual(cache[str(image_path)], result)
        self.assertNotIn(str(image_path), negative_cache)
        get_with_retry.assert_not_called()

    def test_easy_image_download_does_not_query_closest_cdx(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            media_url = "https://pbs.twimg.com/media/easy.jpg"

            with mock.patch.object(
                download_archive,
                "build_image_candidate_urls",
                return_value=[f"https://web.archive.org/web/20260101000000im_/{media_url}"],
            ):
                with mock.patch.object(download_archive, "build_closest_capture_candidate_urls") as closest_lookup:
                    with mock.patch.object(download_archive, "media_cdx_has_any_capture") as exists_lookup:
                        with mock.patch.object(download_archive, "get_with_retry", side_effect=RuntimeError("timeout")):
                            result = download_archive.download_image_asset(
                                media_url,
                                "20260101000000",
                                media_dir,
                                asset_dir.name,
                                cache={},
                                negative_cache={},
                                allow_closest_cdx_lookup=False,
                            )

        self.assertEqual(result, "")
        closest_lookup.assert_not_called()
        exists_lookup.assert_not_called()

    def test_easy_media_policy_does_not_actively_download_video(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            video_url = "https://video.twimg.com/ext_tw_video/video.mp4"

            with mock.patch.object(download_archive, "get_with_retry") as get_with_retry:
                result = download_archive.download_video_asset(
                    video_url,
                    "20260101000000",
                    media_dir,
                    asset_dir.name,
                    cache={},
                    negative_cache={},
                    download_missing=download_archive.MEDIA_FETCH_EASY_IMAGES.should_download_kind("video"),
                )

        self.assertEqual(result, "")
        get_with_retry.assert_not_called()

    def test_no_media_policy_prefetch_does_not_lookup_reference_media(self):
        data = {
            "data": {
                "id": "300",
                "author_id": "u3",
                "conversation_id": "200",
                "created_at": "2026-01-03T00:00:00.000Z",
                "referenced_tweets": [{"type": "replied_to", "id": "200"}],
                "text": "@parent_user child text",
            },
            "includes": {
                "users": [{"id": "u3", "username": "child_user", "name": "Child"}],
                "tweets": [
                    {
                        "id": "200",
                        "author_id": "u2",
                        "conversation_id": "200",
                        "created_at": "2026-01-02T00:00:00.000Z",
                        "attachments": {"media_keys": ["3_200"]},
                        "text": "parent with photo https://t.co/photo",
                    }
                ],
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp) / "archive_assets"
            json_dir = asset_dir / "json"
            media_dir = asset_dir / "media"
            json_dir.mkdir(parents=True)
            media_dir.mkdir(parents=True)
            json_path = json_dir / "20260103000000_300.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            record = download_archive.SnapshotRecord(
                timestamp="20260103000000",
                original_url="https://twitter.com/child_user/status/300",
                json_path=json_path,
            )

            with mock.patch.object(download_archive, "fetch_wayback_tweet_payload") as wayback_fetch:
                download_archive.prefetch_snapshot_media(
                    record,
                    asset_dir.name,
                    media_dir,
                    image_cache={},
                    negative_cache={},
                    media_fetch_policy=download_archive.MEDIA_FETCH_NONE,
                )

        wayback_fetch.assert_not_called()

    def test_forbidden_media_tries_candidates_then_marks_negative_cache_as_403(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            negative_cache = {}
            failure_statuses = {}
            media_url = "https://pbs.twimg.com/media/forbidden.jpg"
            candidate_urls = [
                f"https://web.archive.org/web/20260101000000im_/{media_url}",
                f"https://web.archive.org/web/20260101000000if_/{media_url}",
            ]
            closest_urls = [f"https://web.archive.org/web/20260102000000im_/{media_url}"]

            with mock.patch.object(download_archive, "build_image_candidate_urls", return_value=candidate_urls):
                with mock.patch.object(download_archive, "build_closest_capture_candidate_urls", return_value=closest_urls):
                    with mock.patch.object(
                        download_archive,
                        "get_with_retry",
                        side_effect=[
                            make_http_error(403),
                            make_http_error(404),
                            make_http_error(404),
                        ],
                    ) as get_with_retry:
                        result = download_archive.download_image_asset(
                            media_url,
                            "20260101000000",
                            media_dir,
                            asset_dir.name,
                            cache={},
                            negative_cache=negative_cache,
                            failure_statuses=failure_statuses,
                        )

        self.assertEqual(result, "")
        self.assertEqual(get_with_retry.call_count, 3)
        self.assertEqual(failure_statuses[media_url], "403")
        self.assertEqual(negative_cache[media_url]["status"], "403")
        self.assertEqual(negative_cache[media_url]["reason"], "forbidden")

    def test_retry_403_ignores_cached_forbidden_media_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            media_dir = asset_dir / "media"
            media_dir.mkdir()
            media_url = "https://pbs.twimg.com/media/retry_forbidden.jpg"
            negative_cache = {media_url: {"status": "403", "reason": "forbidden"}}

            with mock.patch.object(download_archive, "build_image_candidate_urls", return_value=[media_url]):
                with mock.patch.object(download_archive, "get_with_retry", return_value=make_image_response()) as get_with_retry:
                    result = download_archive.download_image_asset(
                        media_url,
                        "20260101000000",
                        media_dir,
                        asset_dir.name,
                        cache={},
                        negative_cache=negative_cache,
                        retry_forbidden_media=True,
                    )

        self.assertTrue(result.startswith(f"{asset_dir.name}/media/"))
        get_with_retry.assert_called_once()
        self.assertNotIn(media_url, negative_cache)

    def test_retry_403_cache_drop_removes_persisted_forbidden_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp)
            (asset_dir / "media_negative_index.json").write_text(
                json.dumps(
                    {
                        "version": download_archive.MEDIA_NEGATIVE_CACHE_VERSION,
                        "entries": {
                            "https://pbs.twimg.com/media/forbidden.jpg": {
                                "status": "403",
                                "reason": "forbidden",
                            },
                            "https://pbs.twimg.com/media/missing.jpg": {
                                "status": "404",
                                "reason": "hard-missing",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            cache = download_archive.load_negative_media_cache(asset_dir)

            removed = download_archive.drop_forbidden_negative_media_cache_entries(asset_dir, cache)
            reloaded = download_archive.load_negative_media_cache(asset_dir)

        self.assertEqual(removed, 1)
        self.assertNotIn("https://pbs.twimg.com/media/forbidden.jpg", reloaded)
        self.assertIn("https://pbs.twimg.com/media/missing.jpg", reloaded)

    def test_render_snapshot_entry_keeps_media_only_tweet_when_media_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp) / "Account_archive_assets"
            media_dir = asset_dir / "media"
            media_dir.mkdir(parents=True)
            json_path = Path(tmp) / "20260101000000_100.json"
            json_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "100",
                            "author_id": "u1",
                            "conversation_id": "100",
                            "created_at": "2026-01-01T00:00:00.000Z",
                            "text": "",
                            "attachments": {"media_keys": ["3_100", "7_100"]},
                        },
                        "includes": {
                            "users": [{"id": "u1", "username": "account", "name": "Account"}],
                            "media": [
                                {
                                    "media_key": "3_100",
                                    "type": "photo",
                                    "url": "https://pbs.twimg.com/media/missing.jpg",
                                },
                                {
                                    "media_key": "7_100",
                                    "type": "video",
                                    "preview_image_url": "https://pbs.twimg.com/ext_thumb/missing.jpg",
                                    "variants": [
                                        {
                                            "content_type": "video/mp4",
                                            "url": "https://video.twimg.com/missing.mp4",
                                            "bit_rate": 832000,
                                        }
                                    ],
                                },
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260101000000",
                original_url="https://twitter.com/account/status/100",
                json_path=json_path,
            )

            with mock.patch.object(download_archive, "download_image_asset", return_value=""):
                with mock.patch.object(download_archive, "download_video_asset", return_value=""):
                    entry = download_archive.render_snapshot_entry(
                        record,
                        0,
                        asset_dir.name,
                        media_dir,
                        image_cache={},
                        negative_cache={},
                    )

        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertIn("[图片媒体不可用]", entry.block)
        self.assertIn("[视频媒体不可用]", entry.block)
        self.assertIn("[图片媒体不可用]", entry.body_text)
        self.assertIn("[视频媒体不可用]", entry.body_text)
        self.assertTrue(entry.has_media)
        self.assertNotIn("tweet-media-item", entry.block)

    def test_render_snapshot_entry_keeps_media_only_tweet_with_unresolved_attachment(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp) / "Account_archive_assets"
            media_dir = asset_dir / "media"
            media_dir.mkdir(parents=True)
            json_path = Path(tmp) / "20260101000000_100.json"
            json_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "100",
                            "author_id": "u1",
                            "conversation_id": "100",
                            "created_at": "2026-01-01T00:00:00.000Z",
                            "text": "",
                            "attachments": {"media_keys": ["3_100"]},
                        },
                        "includes": {
                            "users": [{"id": "u1", "username": "account", "name": "Account"}],
                            "media": [],
                        },
                    }
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260101000000",
                original_url="https://twitter.com/account/status/100",
                json_path=json_path,
            )

            with mock.patch.object(download_archive, "download_image_asset") as download_image:
                entry = download_archive.render_snapshot_entry(
                    record,
                    0,
                    asset_dir.name,
                    media_dir,
                    image_cache={},
                    negative_cache={},
                )

        self.assertIsNotNone(entry)
        assert entry is not None
        download_image.assert_not_called()
        self.assertIn("[图片媒体不可用]", entry.block)
        self.assertEqual(entry.body_text, "[图片媒体不可用]")
        self.assertTrue(entry.has_media)

    def test_render_snapshot_entry_marks_reply_chain_media_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            asset_dir = Path(tmp) / "Account_archive_assets"
            media_dir = asset_dir / "media"
            media_dir.mkdir(parents=True)
            json_path = Path(tmp) / "20260101010000_200.json"
            json_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "id": "200",
                            "author_id": "u2",
                            "conversation_id": "100",
                            "created_at": "2026-01-01T01:00:00.000Z",
                            "text": "@account reply",
                            "referenced_tweets": [{"type": "replied_to", "id": "100"}],
                        },
                        "includes": {
                            "users": [
                                {"id": "u1", "username": "account", "name": "Account"},
                                {"id": "u2", "username": "reply_user", "name": "Reply User"},
                            ],
                            "tweets": [
                                {
                                    "id": "100",
                                    "author_id": "u1",
                                    "conversation_id": "100",
                                    "created_at": "2026-01-01T00:00:00.000Z",
                                    "text": "",
                                    "attachments": {"media_keys": ["3_100"]},
                                }
                            ],
                            "media": [
                                {
                                    "media_key": "3_100",
                                    "type": "photo",
                                    "url": "https://pbs.twimg.com/media/missing_parent.jpg",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            record = download_archive.SnapshotRecord(
                timestamp="20260101010000",
                original_url="https://twitter.com/reply_user/status/200",
                json_path=json_path,
            )

            with mock.patch.object(download_archive, "download_image_asset", return_value=""):
                entry = download_archive.render_snapshot_entry(
                    record,
                    0,
                    asset_dir.name,
                    media_dir,
                    image_cache={},
                    negative_cache={},
                )

        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertIn("tweet-reply-chain", entry.block)
        self.assertIn("[图片媒体不可用]", entry.deferred_reply_chain_html)
        self.assertEqual(len(entry.comment_parent_entries), 1)
        parent_entry = entry.comment_parent_entries[0]
        self.assertIn("[图片媒体不可用]", parent_entry.comment_html)
        self.assertEqual(parent_entry.body_text, "[图片媒体不可用]")
        self.assertTrue(parent_entry.has_media)


if __name__ == "__main__":
    unittest.main()
