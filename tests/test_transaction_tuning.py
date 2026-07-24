import asyncio
import logging
import struct
import unittest

from solders.compute_budget import set_compute_unit_limit
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.transaction import VersionedTransaction

import src.executor as executor


class _Response:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


class _Session:
    def __init__(self, payload):
        self.payload = payload

    def get(self, *_args, **_kwargs):
        return _Response(self.payload)


class TransactionTuningTests(unittest.TestCase):
    def setUp(self):
        executor._last_successful_tip_lamports = None

    def test_compute_limit_is_replaced_with_simulated_units_plus_margin(self):
        payer = Keypair()
        message = MessageV0.try_compile(
            payer.pubkey(),
            [set_compute_unit_limit(400_000)],
            [],
            Hash.default(),
        )
        transaction = VersionedTransaction(message, [payer])

        tuned = VersionedTransaction.from_bytes(
            executor.apply_compute_unit_limit(bytes(transaction), 55_000)
        )

        data = bytes(tuned.message.instructions[0].data)
        self.assertEqual(data[0], 2)
        self.assertEqual(struct.unpack("<I", data[1:])[0], 55_000)

    def test_normal_tip_uses_midpoint_of_50th_and_75th_then_caps(self):
        payload = [{
            "landed_tips_50th_percentile": 0.001,
            "landed_tips_75th_percentile": 0.003,
            "landed_tips_95th_percentile": 0.007,
        }]
        with self.assertLogs("executor", level=logging.INFO) as logs:
            tip = asyncio.run(
                executor.dynamic_jito_tip(
                    _Session(payload), order_notional_lamports=50_000_000, urgency="normal"
                )
            )
        self.assertEqual(tip, 1_000_000)  # 2% cap beats the interpolated 0.002 SOL.
        self.assertTrue(any("Jito dynamic tip" in line for line in logs.output))

    def test_emergency_tip_interpolates_75th_to_95th_for_82_5th(self):
        payload = [{
            "landed_tips_50th_percentile": 0.00001,
            "landed_tips_75th_percentile": 0.00002,
            "landed_tips_95th_percentile": 0.00010,
        }]
        tip = asyncio.run(
            executor.dynamic_jito_tip(
                _Session(payload), order_notional_lamports=1_000_000_000,
                urgency="emergency",
            )
        )
        self.assertEqual(tip, 50_000)

    def test_tip_floor_failure_uses_fallback_and_still_caps(self):
        with self.assertLogs("executor", level=logging.WARNING):
            tip = asyncio.run(
                executor.dynamic_jito_tip(
                    _Session([]), order_notional_lamports=10_000_000, urgency="normal"
                )
            )
        self.assertEqual(tip, 200_000)


if __name__ == "__main__":
    unittest.main()
