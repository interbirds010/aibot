"""외부 N3 feasibility 등록/단발 점검/제한된 감시 CLI."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research.n3_feasibility import checkpoint_status, create_amendment, record_checkpoint
from src.research.alpha_transition import record_transition


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--amendment", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--transition-output", type=Path, help="checkpoint 기록 시 외부 transition 보고서 경로")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--register", action="store_true")
    modes.add_argument("--record", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, choices=(30, 60), default=60)
    parser.add_argument("--max-polls", type=int, default=1440)
    args = parser.parse_args(argv)
    if args.max_polls < 1 or args.max_polls > 2880:
        parser.error("--max-polls must be between 1 and 2880")
    if args.transition_output and not args.record:
        parser.error("--transition-output requires --record")
    if args.register:
        if not args.output or args.watch:
            parser.error("--register requires --output and cannot --watch")
        result = create_amendment(args.runtime, args.output)
    else:
        if not args.amendment or (args.record and not args.output):
            parser.error("--amendment required; --record also requires --output")
        for index in range(args.max_polls if args.watch else 1):
            result = checkpoint_status(args.runtime, args.amendment)
            if args.record and result["checkpoint_status"] == "CHECKPOINT_REACHED":
                result = record_checkpoint(args.runtime, args.amendment, args.output)
                transition_path = args.transition_output or args.output.with_name(args.output.stem + "_transition.json")
                try:
                    record_transition(args.runtime, result, transition_path)
                except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
                    result = {**result, "transition_status": "BLOCKED", "transition_reason": str(exc)}
            if result["checkpoint_status"] != "WAITING_FOR_30_OR_7D" or not args.watch:
                break
            if index + 1 < args.max_polls:
                time.sleep(args.interval)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 2 if result.get("checkpoint_status") == "BLOCKED" or result.get("transition_status") == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
