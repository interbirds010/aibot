import unittest

from src.dashboard_clipboard import clipboard_button_document, clipboard_script


class DashboardClipboardTests(unittest.TestCase):
    def test_script_falls_back_when_clipboard_api_is_unavailable_or_rejected(self):
        script = clipboard_script()

        self.assertIn("navigator.clipboard", script)
        self.assertIn("catch (error)", script)
        self.assertIn("document.execCommand('copy')", script)
        self.assertIn("copy-failed", script)

    def test_position_component_preserves_full_ca_and_escapes_markup(self):
        address = 'Mint"<unsafe>&123456789'

        document = clipboard_button_document(address)

        self.assertIn('data-ca="Mint&quot;&lt;unsafe&gt;&amp;123456789"', document)
        self.assertNotIn(address, document)
        self.assertIn("Mint&quot;&lt;u…56789", document)
        self.assertIn("copyCaText", document)


if __name__ == "__main__":
    unittest.main()
