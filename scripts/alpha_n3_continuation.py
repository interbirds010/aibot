"""N3 원본을 변경하지 않는 별도 telemetry 관찰 segment CLI."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research import alpha_n3_continuation as continuation
from src.research import entry_telemetry_epoch as epoch
from scripts.entry_telemetry_cutover import _paper_config, stopped_proof


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("register", "observe"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--original-manifest", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    try:
        if not args.execute:
            result = {"mode": "PREVIEW", "action": args.action, "writes": False, "control_changes": False}
        elif args.action == "register":
            if not args.original_manifest:
                parser.error("register requires --original-manifest")
            marker = epoch.require_epoch(args.root, config=_paper_config(args.root))
            stopped_proof(args.root)
            if args.output.resolve() == args.root.resolve() or args.root.resolve() in args.output.resolve().parents:
                raise ValueError("continuation output must be outside runtime")
            manifest, entries, completed = continuation.registration_sources(args.root, args.original_manifest)
            result = continuation.register_continuation(args.output, manifest, marker,
                baseline_entries=entries, baseline_completed=completed)
        else:
            result = continuation.observe(args.root, args.output, config=_paper_config(args.root))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
