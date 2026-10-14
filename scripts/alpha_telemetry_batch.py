"""Activation 이후 natural acceptance와 Alpha Batch 001 등록·동결 CLI."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.research import alpha_telemetry_batch as batch


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    accept = sub.add_parser("acceptance")
    accept.add_argument("--root", type=Path, required=True)
    accept.add_argument("--baseline", type=Path)
    accept.add_argument("--policy", type=Path)
    accept.add_argument("--output", type=Path, required=True)
    register = sub.add_parser("register")
    register.add_argument("--acceptance", type=Path, required=True)
    register.add_argument("--output", type=Path, required=True)
    register.add_argument("--start-seq", type=int, required=True)
    register.add_argument("--start-utc", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--root", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    freeze.add_argument("--as-of", default=None)
    args = parser.parse_args(argv)
    try:
        if args.command == "acceptance":
            from scripts.entry_telemetry_cutover import _paper_config
            result = batch.acceptance_report(args.root, baseline=batch.read_evidence(args.baseline) if args.baseline else None,
                policy=batch.read_evidence(args.policy) if args.policy else None, config=_paper_config(args.root.resolve()))
            batch.write_evidence(args.output, result, runtime_root=args.root)
        elif args.command == "register":
            result = batch.register_batch(args.output, batch.read_evidence(args.acceptance), start_seq=args.start_seq, start_utc=args.start_utc)
        else:
            result = batch.freeze_batch(args.root, args.output, as_of=args.as_of or datetime.now(timezone.utc).isoformat())
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["status"] in {"TELEMETRY_RUNTIME_ACCEPTED", "REGISTERED", "FROZEN"} else 2
    except (ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
        print(json.dumps({"status": "BLOCKED", "reason": str(error), "category": type(error).__name__}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
