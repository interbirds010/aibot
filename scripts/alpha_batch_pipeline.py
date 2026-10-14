"""등록된 미래 batch만 감시하여 동결·join·forensic·proposal까지 연결한다."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.research import alpha_telemetry_batch as batch
from scripts.accelerated_alpha_research import import_telemetry_batch, input_definition, external_output


def run_once(root: Path, batch_output: Path, research_output: Path, *, as_of: str,
             execute=False, screen_policy=None, label_contract=None):
    external_output(batch_output, root)
    external_output(research_output, root)
    if batch_output.resolve() == research_output.resolve() or batch_output.resolve() in research_output.resolve().parents or research_output.resolve() in batch_output.resolve().parents:
        raise ValueError("batch evidence and research registry require separate namespaces")
    if not execute:
        return {"status": "PREVIEW", "writes": False, "control_changes": False}
    frozen = batch.freeze_batch(root, batch_output, as_of=as_of)
    if frozen["status"] == "PENDING":
        return frozen
    report = import_telemetry_batch(batch_output / batch.BATCH_ID / "dataset.json", research_output,
        screen_policy=screen_policy, label_contract=label_contract)
    return {"status": "BATCH_PIPELINE_COMPLETED", "source_batch_id": batch.BATCH_ID,
        "completed_control_trades": frozen["completed_control_trades"], "close_reason": frozen["close_reason"],
        "forensic": report, "challenger_registration": "PROPOSAL_ONLY", "control_changes": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--batch-output", type=Path, required=True)
    parser.add_argument("--research-output", type=Path, required=True)
    parser.add_argument("--screen-policy", type=Path)
    parser.add_argument("--label-contract", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, choices=(30, 60), default=60)
    parser.add_argument("--max-polls", type=int, default=1440)
    args = parser.parse_args(argv)
    if not 1 <= args.max_polls <= 2880 or args.watch and not args.execute:
        parser.error("bounded watch requires --execute and --max-polls between 1 and 2880")
    try:
        policy = input_definition(args.screen_policy) if args.screen_policy else None
        labels = input_definition(args.label_contract) if args.label_contract else None
        previous = None
        for index in range(args.max_polls if args.watch else 1):
            result = run_once(args.root, args.batch_output, args.research_output,
                as_of=datetime.now(timezone.utc).isoformat(), execute=args.execute,
                screen_policy=policy, label_contract=labels)
            encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False)
            if encoded != previous:
                print(encoded, flush=True)
                previous = encoded
            if result["status"] != "PENDING" or not args.watch:
                return 0
            if index + 1 < args.max_polls:
                time.sleep(args.interval)
        return 2  # 감시 예산 종료는 완료 판정이 아니다.
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
