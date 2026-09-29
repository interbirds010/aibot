from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from scripts.storage_retention import (
    BACKUP_FILE_NAMES,
    Classification,
    ProcScan,
    _allocated_bytes,
    cleanup_legacy_temporary_files,
    plan_legacy_temporary_files,
    plan_validated_backups,
    retain_validated_backups,
    scan_open_regular_files,
)


def _archive_manifest(backup: Path, records: dict[str, bytes]) -> dict[str, object]:
    root = backup / "research_archive" / "records"
    digest = hashlib.sha256()
    size = 0
    for relative, payload in sorted(records.items()):
        path = root / Path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        digest.update(Path(relative).as_posix().encode("utf-8"))
        digest.update(hashlib.sha256(payload).hexdigest().encode("ascii"))
        size += len(payload)
    return {
        "record_count": len(records),
        "size_bytes": size,
        "tree_sha256": digest.hexdigest(),
    }


def _make_backup(
    root: Path,
    *,
    day: int,
    sha_digit: str,
    payload: bytes = b"ledger",
    archive: dict[str, bytes] | None = None,
) -> tuple[Path, str]:
    revision = sha_digit * 40
    backup = root / f"predeploy-202609{day:02d}T010203Z-{revision[:12]}"
    backup.mkdir()
    file_items: dict[str, dict[str, object]] = {
        name: {"exists": False} for name in BACKUP_FILE_NAMES
    }
    ledger = backup / BACKUP_FILE_NAMES[0]
    ledger.write_bytes(payload)
    file_items[BACKUP_FILE_NAMES[0]] = {
        "exists": True,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
        "summary": {},
    }
    manifest = {
        "deploy_sha": revision,
        "created_at": "2026-09-29T01:02:03+00:00",
        "created_at_epoch": 1790643723.0,
        "files": file_items,
        "research_archive": _archive_manifest(backup, archive or {}),
    }
    (backup / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (root / f"latest-{revision}.txt").write_text(str(backup), encoding="utf-8")
    return backup, revision


def _reason(plan, name: str) -> tuple[Classification, str]:
    item = next(candidate for candidate in plan.candidates if candidate.path.name == name)
    return item.classification, item.reason


class _FakeProcRoot:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def iterdir(self):
        return iter([_FakeProcess(self.error)])

    def is_dir(self) -> bool:
        return True


class _FakeProcess:
    name = "123"

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def __truediv__(self, name: str):
        if name != "fd":
            raise AssertionError(name)
        return _FakeDescriptors(self.error)


class _FakeDescriptors:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def iterdir(self):
        raise self.error


class TemporaryRetentionTests(unittest.TestCase):
    def test_classifies_stale_young_wrong_owner_open_and_invalid_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            now = time.time()
            stale = root / "tmpabcdefgh"
            young = root / "tmp12345678"
            opened = root / "tmp87654321"
            malformed = root / "tmp-too-long"
            for path in (stale, young, opened, malformed):
                path.write_bytes(b"x" * 10)
            for path in (stale, opened, malformed):
                os.utime(path, (now - 700, now - 700))
            opened_stat = opened.stat()

            plan = plan_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                proc_scan=ProcScan(
                    frozenset({(opened_stat.st_dev, opened_stat.st_ino)}), True
                ),
            )

            self.assertEqual(
                _reason(plan, stale.name),
                (Classification.SAFE_TO_DELETE, "eligible"),
            )
            self.assertEqual(_reason(plan, young.name)[1], "too_young")
            self.assertEqual(_reason(plan, opened.name)[1], "open")
            self.assertEqual(_reason(plan, malformed.name)[1], "invalid_name")
            inventory = json.dumps(plan.summary(), sort_keys=True)
            self.assertIn('"eligible_count": 1', inventory)
            self.assertIn('"total_allocated_bytes"', inventory)
            self.assertNotIn(str(root), inventory)

            owner_plan = plan_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                proc_scan=ProcScan(frozenset(), True),
                owner_uid=stale.stat().st_uid + 1,
            )
            self.assertEqual(_reason(owner_plan, stale.name)[1], "owner_mismatch")

    def test_symlink_and_non_regular_are_never_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "target"
            target.write_text("x", encoding="utf-8")
            link = root / "tmpabcdefgh"
            try:
                link.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlink unavailable: {exc}")
            directory = root / "tmp12345678"
            directory.mkdir()
            plan = plan_legacy_temporary_files(
                root,
                minimum_age_seconds=0,
                proc_scan=ProcScan(frozenset(), True),
            )
            self.assertEqual(_reason(plan, link.name)[1], "symlink")
            self.assertEqual(_reason(plan, directory.name)[1], "non_regular")

    def test_proc_permission_failure_is_unknown_and_pid_disappearance_is_safe(self) -> None:
        denied = scan_open_regular_files(_FakeProcRoot(PermissionError()))
        vanished = scan_open_regular_files(_FakeProcRoot(FileNotFoundError()))
        self.assertFalse(denied.complete)
        self.assertTrue(vanished.complete)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            stale.write_text("x", encoding="utf-8")
            old = time.time() - 700
            os.utime(stale, (old, old))
            plan = plan_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                proc_scan=denied,
            )
            self.assertEqual(_reason(plan, stale.name)[1], "proc_unknown")

    def test_dry_run_and_real_run_select_same_static_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            stale.write_bytes(b"x" * 10)
            now = time.time()
            os.utime(stale, (now - 700, now - 700))
            scan = ProcScan(frozenset(), True)
            dry_plan = plan_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                proc_scan=scan,
            )
            dry_result = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                dry_run=True,
                proc_scanner=lambda: scan,
            )
            self.assertEqual(dry_plan.safe_names, {stale.name})
            self.assertEqual(dry_result, (0, 0))
            self.assertTrue(stale.exists())

            result = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                proc_scanner=lambda: scan,
            )
            self.assertEqual(result, (1, 10))
            self.assertFalse(stale.exists())

    def test_inode_or_metadata_change_before_delete_cancels_removal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            stale.write_text("old", encoding="utf-8")
            old = time.time() - 700
            os.utime(stale, (old, old))
            scan = ProcScan(frozenset(), True)

            def replace(path: Path) -> None:
                path.unlink()
                path.write_text("replacement-is-different", encoding="utf-8")

            result = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                proc_scanner=lambda: scan,
                before_delete=replace,
            )
            self.assertEqual(result, (0, 0))
            self.assertTrue(stale.exists())

    def test_opened_after_initial_scan_cancels_removal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            stale.write_text("x", encoding="utf-8")
            old = time.time() - 700
            os.utime(stale, (old, old))
            item = stale.stat()
            scans = iter(
                [
                    ProcScan(frozenset(), True),
                    ProcScan(frozenset({(item.st_dev, item.st_ino)}), True),
                ]
            )
            result = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                proc_scanner=lambda: next(scans),
            )
            self.assertEqual(result, (0, 0))
            self.assertTrue(stale.exists())

    def test_inode_change_during_final_proc_scan_cancels_removal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            stale.write_text("old", encoding="utf-8")
            old = time.time() - 700
            os.utime(stale, (old, old))
            calls = 0

            def scanner() -> ProcScan:
                nonlocal calls
                calls += 1
                if calls == 2:
                    stale.unlink()
                    stale.write_text("replacement", encoding="utf-8")
                return ProcScan(frozenset(), True)

            result = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                proc_scanner=scanner,
            )
            self.assertEqual(result, (0, 0))
            self.assertTrue(stale.exists())

    @unittest.skipUnless(os.name == "posix" and Path("/proc").is_dir(), "Linux /proc required")
    def test_linux_proc_scan_finds_current_process_open_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "tmpabcdefgh"
            path.write_text("open", encoding="utf-8")
            with path.open("rb"):
                metadata = path.stat()
                scan = scan_open_regular_files()
            self.assertIn((metadata.st_dev, metadata.st_ino), scan.opened)

    def test_allocated_bytes_uses_platform_block_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "tmpabcdefgh"
            path.write_bytes(b"x" * 123)
            metadata = path.stat()
            expected = (
                metadata.st_blocks * 512
                if hasattr(metadata, "st_blocks")
                else metadata.st_size
            )
            self.assertEqual(_allocated_bytes(metadata), expected)


class BackupRetentionTests(unittest.TestCase):
    def _fixture(self, root: Path):
        backups = [
            _make_backup(root, day=index + 1, sha_digit=str(index + 1))
            for index in range(6)
        ]
        return backups, backups[1][1], backups[5][1]

    def test_latest_active_successful_and_safe_generations_are_classified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backups, deployed, active = self._fixture(root)
            plan = plan_validated_backups(
                root, keep=3, deployed_sha=deployed, active_source_sha=active
            )
            self.assertEqual(plan.safe_names, {backups[0][0].name, backups[2][0].name})
            self.assertEqual(_reason(plan, backups[1][0].name)[1], "protected_successful")
            self.assertEqual(_reason(plan, backups[5][0].name)[1], "protected_active")
            self.assertEqual(_reason(plan, backups[4][0].name)[1], "protected_latest")

    def test_dry_run_and_real_run_remove_only_same_safe_set(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backups, deployed, active = self._fixture(root)
            plan = plan_validated_backups(
                root, keep=3, deployed_sha=deployed, active_source_sha=active
            )
            expected = plan.safe_names
            self.assertEqual(
                retain_validated_backups(
                    root,
                    keep=3,
                    deployed_sha=deployed,
                    active_source_sha=active,
                    dry_run=True,
                ),
                (0, 0),
            )
            self.assertTrue(all(path.exists() for path, _ in backups))
            count, size = retain_validated_backups(
                root,
                keep=3,
                deployed_sha=deployed,
                active_source_sha=active,
            )
            self.assertEqual(count, len(expected))
            self.assertGreater(size, 0)
            remaining = {path.name for path, _ in backups if path.exists()}
            self.assertEqual(remaining, {path.name for path, _ in backups} - expected)

    def test_protected_sha_inputs_are_required_and_strict(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "requires deployed"):
                retain_validated_backups(root, keep=3)
            with self.assertRaisesRegex(ValueError, "40-character SHA"):
                plan_validated_backups(
                    root, keep=3, deployed_sha="bad", active_source_sha="a" * 40
                )

    def test_unknown_marker_state_makes_all_valid_backups_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backups, deployed, active = self._fixture(root)
            (root / f"latest-{active}.txt").write_text("relative/path", encoding="utf-8")
            plan = plan_validated_backups(
                root, keep=3, deployed_sha=deployed, active_source_sha=active
            )
            valid_records = [item for item in plan.candidates if item.revision]
            self.assertTrue(valid_records)
            self.assertTrue(
                all(
                    item.classification == Classification.UNKNOWN_DO_NOT_TOUCH
                    and item.reason == "unknown_marker_state"
                    for item in valid_records
                )
            )

    def test_malformed_name_manifest_missing_json_and_revision_mismatch(self) -> None:
        cases = ("invalid_name", "manifest_missing", "invalid_manifest", "deploy_sha_mismatch")
        for wanted in cases:
            with self.subTest(wanted=wanted), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                _, deployed = _make_backup(root, day=8, sha_digit="8")
                _, active = _make_backup(root, day=9, sha_digit="9")
                if wanted == "invalid_name":
                    target = root / "predeploy-invalid"
                    target.mkdir()
                else:
                    target, _ = _make_backup(root, day=1, sha_digit="1")
                    manifest = target / "manifest.json"
                    if wanted == "manifest_missing":
                        manifest.unlink()
                    elif wanted == "invalid_manifest":
                        manifest.write_text("{", encoding="utf-8")
                    else:
                        data = json.loads(manifest.read_text(encoding="utf-8"))
                        data["deploy_sha"] = "f" * 40
                        manifest.write_text(json.dumps(data), encoding="utf-8")
                plan = plan_validated_backups(
                    root, keep=1, deployed_sha=deployed, active_source_sha=active
                )
                self.assertEqual(_reason(plan, target.name)[1], wanted)

    def test_file_checksum_size_missing_and_symlink_fail_closed(self) -> None:
        for wanted in ("checksum_mismatch", "size_mismatch", "missing_file", "symlink"):
            with self.subTest(wanted=wanted), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                target, target_sha = _make_backup(root, day=1, sha_digit="1", payload=b"ledger")
                _, deployed = _make_backup(root, day=8, sha_digit="8")
                _, active = _make_backup(root, day=9, sha_digit="9")
                ledger = target / BACKUP_FILE_NAMES[0]
                if wanted == "checksum_mismatch":
                    ledger.write_bytes(b"badger")
                elif wanted == "size_mismatch":
                    ledger.write_bytes(b"longer-ledger")
                elif wanted == "missing_file":
                    ledger.unlink()
                else:
                    ledger.unlink()
                    try:
                        ledger.symlink_to(target / "manifest.json")
                    except OSError as exc:
                        self.skipTest(f"symlink unavailable: {exc}")
                plan = plan_validated_backups(
                    root, keep=1, deployed_sha=deployed, active_source_sha=active
                )
                self.assertEqual(_reason(plan, target.name)[1], wanted)
                self.assertEqual(target_sha, "1" * 40)

    def test_archive_count_size_and_digest_mismatch_fail_closed(self) -> None:
        for wanted in (
            "archive_count_mismatch",
            "archive_size_mismatch",
            "archive_digest_mismatch",
        ):
            with self.subTest(wanted=wanted), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                target, _ = _make_backup(
                    root,
                    day=1,
                    sha_digit="1",
                    archive={"aa/record.json": b"archive"},
                )
                _, deployed = _make_backup(root, day=8, sha_digit="8")
                _, active = _make_backup(root, day=9, sha_digit="9")
                manifest_path = target / "manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                key = {
                    "archive_count_mismatch": "record_count",
                    "archive_size_mismatch": "size_bytes",
                    "archive_digest_mismatch": "tree_sha256",
                }[wanted]
                manifest["research_archive"][key] = (
                    "0" * 64 if key == "tree_sha256" else manifest["research_archive"][key] + 1
                )
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                plan = plan_validated_backups(
                    root, keep=1, deployed_sha=deployed, active_source_sha=active
                )
                self.assertEqual(_reason(plan, target.name)[1], wanted)

    def test_changed_backup_before_delete_is_not_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backups, deployed, active = self._fixture(root)
            changed = backups[0][0]

            def mutate(path: Path) -> None:
                if path == changed:
                    (path / BACKUP_FILE_NAMES[0]).write_text("changed", encoding="utf-8")

            retain_validated_backups(
                root,
                keep=3,
                deployed_sha=deployed,
                active_source_sha=active,
                before_delete=mutate,
            )
            self.assertTrue(changed.exists())

    def test_backup_allocated_bytes_are_sum_of_validated_regular_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target, deployed = _make_backup(
                root,
                day=1,
                sha_digit="1",
                archive={"aa/record.json": b"archive"},
            )
            _, active = _make_backup(root, day=2, sha_digit="2")
            plan = plan_validated_backups(
                root, keep=1, deployed_sha=deployed, active_source_sha=active
            )
            record = next(item for item in plan.candidates if item.path == target)
            expected = sum(
                _allocated_bytes(path.stat())
                for path in target.rglob("*")
                if path.is_file()
            )
            self.assertEqual(record.allocated_bytes, expected)


if __name__ == "__main__":
    unittest.main()
