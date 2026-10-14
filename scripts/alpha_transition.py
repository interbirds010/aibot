"""N3 checkpoint 전환 보고서/계획만 외부에 저장한다. 프로세스는 변경하지 않는다."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research.alpha_transition import cutover_plan, record_cutover_plan, record_transition, transition_report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("report", "plan"))
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--telemetry-runtime", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.action == "plan" and not args.telemetry_runtime:
        parser.error("plan requires --telemetry-runtime")
    try:
        if args.action == "report":
            result = (record_transition(args.runtime, args.checkpoint, args.output)
                      if args.output else transition_report(args.checkpoint))
        elif args.output:
            result = record_cutover_plan(args.runtime, args.checkpoint, args.telemetry_runtime, args.output)
        else:
            result = cutover_plan(args.checkpoint, frozen_runtime=args.runtime, telemetry_runtime=args.telemetry_runtime)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
        print(json.dumps({"transition_status": "BLOCKED", "reason": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
