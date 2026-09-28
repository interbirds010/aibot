from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from scripts.storage_retention import (
    cleanup_legacy_temporary_files,
    retain_validated_backups,
)


class StorageRetentionTests(unittest.TestCase):
    def test_legacy_cleanup_requires_exact_pattern_age_owner_and_closed_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stale = root / "tmpabcdefgh"
            fresh = root / "tmp12345678"
            unrelated = root / "tmp-too-long"
            opened = root / "tmp87654321"
            for path in (stale, fresh, unrelated, opened):
                path.write_bytes(b"x" * 10)
            now = time.time()
            old = now - 700
            for path in (stale, unrelated, opened):
                os.utime(path, (old, old))
            opened_stat = opened.stat()

            count, size_bytes = cleanup_legacy_temporary_files(
                root,
                minimum_age_seconds=600,
                now=now,
                open_files={(opened_stat.st_dev, opened_stat.st_ino)},
            )

            self.assertEqual((count, size_bytes), (1, 10))
            self.assertFalse(stale.exists())
            self.assertTrue(fresh.exists())
            self.assertTrue(unrelated.exists())
            self.assertTrue(opened.exists())

    def test_backup_retention_keeps_three_newest_validated_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            names = []
            for index in range(5):
                revision = f"{index + 1:040x}"
                name = f"predeploy-2026092{index + 1}T010203Z-{revision[:12]}"
                names.append(name)
                backup = root / name
                backup.mkdir()
                (backup / "manifest.json").write_text(
                    json.dumps({"deploy_sha": revision}), encoding="utf-8"
                )
                (backup / "ledger.json").write_bytes(b"x" * (index + 1))
                (root / f"latest-{revision}.txt").write_text(
                    str(backup), encoding="utf-8"
                )

            count, size_bytes = retain_validated_backups(root, keep=3)

            self.assertEqual(count, 2)
            self.assertGreater(size_bytes, 0)
            remaining = sorted(path.name for path in root.glob("predeploy-*"))
            self.assertEqual(remaining, sorted(names[-3:]))
            self.assertFalse((root / f"latest-{1:040x}.txt").exists())
            self.assertFalse((root / f"latest-{2:040x}.txt").exists())
