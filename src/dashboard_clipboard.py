"""대시보드 CA 복사 컴포넌트용 HTML 조각."""

from __future__ import annotations

import html


def clipboard_script() -> str:
    """권한 정책에 따라 Clipboard API 또는 동기 fallback을 사용한다."""
    return """
      async function copyCaText(text) {
        if (navigator.clipboard && window.isSecureContext) {
          try {
            await navigator.clipboard.writeText(text);
            return;
          } catch (error) {
            // iframe 권한이 거부되면 사용자 동작 기반 fallback을 사용한다.
          }
        }
        const input = document.createElement('textarea');
        input.value = text;
        input.setAttribute('readonly', '');
        input.style.position = 'fixed';
        input.style.opacity = '0';
        document.body.appendChild(input);
        input.focus();
        input.select();
        input.setSelectionRange(0, input.value.length);
        const copied = document.execCommand('copy');
        input.remove();
        if (!copied) {
          throw new Error('clipboard copy was rejected');
        }
      }

      document.querySelectorAll('.copy-ca').forEach((button) => {
        button.addEventListener('click', async () => {
          const originalLabel = button.textContent;
          try {
            await copyCaText(button.dataset.ca);
            button.textContent = '✓';
            button.classList.add('copied');
            button.title = '복사 완료';
          } catch (error) {
            button.textContent = '!';
            button.classList.add('copy-failed');
            button.title = '복사할 수 없습니다';
          }
          setTimeout(() => {
            button.textContent = originalLabel;
            button.classList.remove('copied', 'copy-failed');
            button.title = '토큰 CA 복사';
          }, 900);
        });
      });
    """


def clipboard_button_document(address: str) -> str:
    """보유 포지션 표 한 셀에 표시할 CA 복사 컴포넌트를 만든다."""
    safe_address = html.escape(address, quote=True)
    compact = (
        f"{address[:7]}…{address[-5:]}" if len(address) > 15 else address
    )
    return f"""
    <style>
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; background: transparent; color: #edf8f4;
              font: 14px system-ui, -apple-system, "Segoe UI", sans-serif; }}
      .mint-copy {{ height: 40px; display: flex; align-items: center; gap: 6px; }}
      code {{ min-width: 0; flex: 1; overflow: hidden; text-overflow: ellipsis;
              white-space: nowrap; padding: 9px 8px; border-radius: 7px;
              background: #1b1e26; color: #edf8f4; }}
      .copy-ca {{ flex: 0 0 auto; width: 34px; height: 34px; border-radius: 7px;
                  border: 1px solid #44504e; background: #1b1e26; color: #c9d6d1;
                  cursor: pointer; font-size: 17px; line-height: 1; }}
      .copy-ca:hover, .copy-ca.copied {{ color: #50e3ad; border-color: #50e3ad; }}
      .copy-ca.copy-failed {{ color: #ff7a83; border-color: #ff7a83; }}
    </style>
    <div class="mint-copy">
      <code title="{safe_address}">{html.escape(compact)}</code>
      <button class="copy-ca" data-ca="{safe_address}"
              title="토큰 CA 복사" aria-label="토큰 CA 복사">⧉</button>
    </div>
    <script>{clipboard_script()}</script>
    """
