from __future__ import annotations

import unittest

from src.dashboard_progress import (
    clear_manual_close_progress_document,
    manual_close_progress_document,
)


class DashboardProgressTests(unittest.TestCase):
    def test_manual_close_overlay_is_immediate_and_accessible(self) -> None:
        document = manual_close_progress_document()

        self.assertIn('button.textContent.trim() !== "포지션 종료"', document)
        self.assertIn('hostDocument.addEventListener("click"', document)
        self.assertIn('hostDocument.body.appendChild(overlay)', document)
        self.assertIn("hostDocument.head.appendChild(style)", document)
        self.assertIn('role", "status"', document)
        self.assertIn('aria-live", "assertive"', document)
        self.assertIn("포지션 종료 진행 중입니다", document)

    def test_installer_does_not_clear_an_active_operation(self) -> None:
        document = manual_close_progress_document()

        remove_snippet = (
            "hostDocument.getElementById(overlayId)?.remove();"
        )
        self.assertEqual(document.count(remove_snippet), 1)
        self.assertGreater(
            document.index(remove_snippet),
            document.index('hostDocument.addEventListener("click"'),
        )
        self.assertIn("position: fixed", document)
        self.assertIn("background: rgba(0, 0, 0, 0.72)", document)

    def test_completion_document_removes_overlay(self) -> None:
        document = clear_manual_close_progress_document()

        self.assertIn("manual-close-progress-overlay", document)
        self.assertIn("?.remove()", document)


if __name__ == "__main__":
    unittest.main()
