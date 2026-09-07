from __future__ import annotations

import json
import unittest

from src.research.alpha_review import build_alpha_review_summary


def candidate(index: int, *, status: str = "UNSTABLE") -> dict:
    return {
        "candidate_id": f"SMART_MONEY:single:score:{index:02d}",
        "labels": {"score": str(index)},
        "status": status,
        "status_reasons": ["HOLDOUT_EXPECTANCY_NOT_POSITIVE"],
        "holdout_expectancy_retention_ratio": 0.5,
        "first_signal_per_mint": {
            "horizons": {
                "60m": {
                    "overall": {
                        "sampled_count": 80,
                        "trackable_coverage_rate_percent": 90.0,
                    },
                    "train": {
                        "sampled_count": 64,
                        "expectancy_percent": 2.0,
                        "profit_factor": 1.2,
                        "profit_factor_above_one": True,
                    },
                    "holdout": {
                        "sampled_count": 16,
                        "expectancy_percent": float(index),
                        "profit_factor": 1.1,
                        "profit_factor_above_one": True,
                    },
                }
            }
        },
    }


def report(candidates: list[dict]) -> dict:
    return {
        "primary_horizon": "60m",
        "configuration": {
            "minimum_total_sampled": 50,
            "minimum_holdout_sampled": 15,
            "minimum_trackable_coverage_percent": 80,
            "source_path": "C:/private/secret.json",
        },
        "families": {
            "SMART_MONEY": {
                "single_features": [{
                    "feature": "score",
                    "source_path": "secret-token",
                    "buckets": candidates,
                }],
                "interactions": [],
            },
            "MOMENTUM": {"single_features": [], "interactions": []},
        },
        "candidate_counts": {"UNSTABLE": 999},
        "ranked_promising_candidate_count": 0,
        "input_summary": {"source_path": "/var/www/aibot/data/private.json"},
    }


class AlphaReviewTests(unittest.TestCase):
    def test_candidate_output_is_bounded_and_deterministic(self) -> None:
        rows = [candidate(index) for index in range(15)]
        forward = build_alpha_review_summary(report(rows))
        reverse = build_alpha_review_summary(report(list(reversed(rows))))

        selected = forward["families"]["SMART_MONEY"]["near_misses"]
        self.assertEqual(len(selected), 10)
        self.assertEqual(
            [row["holdout_expectancy_percent"] for row in selected],
            list(range(14, 4, -1)),
        )
        self.assertEqual(forward, reverse)

    def test_status_counts_are_recomputed_from_candidates(self) -> None:
        rows = [
            candidate(1, status="PROMISING"),
            candidate(2, status="UNSTABLE"),
            candidate(3, status="INSUFFICIENT_DATA"),
        ]
        summary = build_alpha_review_summary(report(rows))

        self.assertEqual(summary["evaluated_hypothesis_count"], 3)
        self.assertEqual(summary["PROMISING"], 1)
        self.assertEqual(summary["UNSTABLE"], 1)
        self.assertEqual(summary["INSUFFICIENT_DATA"], 1)

    def test_private_paths_and_unselected_fields_are_not_exposed(self) -> None:
        encoded = json.dumps(build_alpha_review_summary(report([candidate(1)])))
        self.assertNotIn("/var/www", encoded)
        self.assertNotIn("C:/private", encoded)
        self.assertNotIn("secret-token", encoded)
        self.assertNotIn("source_path", encoded)

    def test_missing_candidate_fields_are_safe(self) -> None:
        summary = build_alpha_review_summary(report([{}]))
        row = summary["families"]["SMART_MONEY"]["near_misses"][0]
        self.assertEqual(row["status"], "UNKNOWN")
        self.assertEqual(row["sampled_count"], 0)
        self.assertIsNone(row["holdout_expectancy_percent"])


if __name__ == "__main__":
    unittest.main()
