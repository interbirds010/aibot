from __future__ import annotations

import unittest
from unittest import mock

from src import solana_rpc


class Response:
    status = 200
    headers = {}

    def __init__(self, result):
        self.result = result
        self._body = b"private body"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self):
        return {"result": self.result}


class Session:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, *, json):
        self.calls.append((url, json))
        return self.response


class RpcFailureMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_transaction_result_identity_and_request_are_preserved(self):
        result = {"transaction": {"secret": "never retain"}}
        session = Session(Response(result))
        provider = solana_rpc.RpcProvider(
            name="ankr", url="https://example.invalid", max_rps=5,
        )
        with mock.patch.object(solana_rpc, "mark_current_phase") as mark:
            actual = await solana_rpc._provider_request_once(
                session, provider, "getTransaction", ["signature"],
            )
        self.assertIs(actual, result)
        self.assertEqual(session.calls[0][1]["params"], ["signature"])
        self.assertEqual([c.args[0] for c in mark.call_args_list],
                         ["rpc_decode_begin", "http_body_decoded"])
        metadata = mark.call_args_list[-1].kwargs
        self.assertEqual(metadata["raw_transaction_count"], 1)
        self.assertEqual(metadata["response_body_bytes"], 12)
        self.assertNotIn("never retain", repr(metadata))
        self.assertNotIn("private body", repr(metadata))

    async def test_signature_rows_and_unknown_body_are_scalar_only(self):
        result = [{"signature": "private signature"}, {"signature": "other"}]
        response = Response(result)
        del response._body
        provider = solana_rpc.RpcProvider(
            name="ankr", url="https://example.invalid", max_rps=5,
        )
        with mock.patch.object(solana_rpc, "mark_current_phase") as mark:
            actual = await solana_rpc._provider_request_once(
                Session(response), provider, "getSignaturesForAddress", [],
            )
        self.assertIs(actual, result)
        metadata = mark.call_args_list[-1].kwargs
        self.assertEqual(metadata["raw_signature_count"], 2)
        self.assertFalse(metadata["response_body_known"])
        self.assertNotIn("private signature", repr(metadata))

    async def test_other_rpc_methods_keep_existing_flow(self):
        provider = solana_rpc.RpcProvider(
            name="ankr", url="https://example.invalid", max_rps=5,
        )
        with mock.patch.object(solana_rpc, "mark_current_phase") as mark:
            actual = await solana_rpc._provider_request_once(
                Session(Response(42)), provider, "getBalance", [],
            )
        self.assertEqual(actual, 42)
        mark.assert_not_called()
