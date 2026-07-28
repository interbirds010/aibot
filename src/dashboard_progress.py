"""대시보드의 장시간 작업 진행 오버레이 HTML."""

from __future__ import annotations


def manual_close_progress_document() -> str:
    """포지션 종료 버튼 클릭 즉시 브라우저에 진행 오버레이를 표시한다."""
    return """
    <style id="manual-close-progress-style-source">
      #manual-close-progress-overlay {
        position: fixed;
        inset: 0;
        z-index: 2147483647;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 24px;
        background: rgba(0, 0, 0, 0.72);
        color: #f4fffb;
        font: 600 18px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
        text-align: center;
        backdrop-filter: blur(2px);
      }
      #manual-close-progress-overlay .progress-card {
        min-width: min(360px, calc(100vw - 48px));
        padding: 24px 28px;
        border: 1px solid rgba(80, 227, 173, 0.45);
        border-radius: 14px;
        background: rgba(13, 25, 22, 0.94);
        box-shadow: 0 18px 60px rgba(0, 0, 0, 0.45);
      }
      #manual-close-progress-overlay .progress-spinner {
        width: 30px;
        height: 30px;
        margin: 0 auto 14px;
        border: 3px solid rgba(80, 227, 173, 0.25);
        border-top-color: #50e3ad;
        border-radius: 50%;
        animation: manual-close-spin 0.8s linear infinite;
      }
      #manual-close-progress-overlay .progress-detail {
        margin-top: 6px;
        color: #a9bbb5;
        font-size: 13px;
        font-weight: 400;
      }
      @keyframes manual-close-spin {
        to { transform: rotate(360deg); }
      }
    </style>
    <script>
      (() => {
        const hostWindow = window.parent;
        const hostDocument = hostWindow.document;
        const overlayId = "manual-close-progress-overlay";
        const styleId = "manual-close-progress-style";

        if (!hostDocument.getElementById(styleId)) {
          const style = hostDocument.createElement("style");
          style.id = styleId;
          style.textContent = document.getElementById(
            "manual-close-progress-style-source"
          ).textContent;
          hostDocument.head.appendChild(style);
        }

        if (hostWindow.__manualCloseProgressInstalled) {
          return;
        }
        hostWindow.__manualCloseProgressInstalled = true;

        hostDocument.addEventListener("click", (event) => {
          const target = event.target instanceof hostWindow.Element
            ? event.target
            : event.target?.parentElement;
          const button = target?.closest("button");
          if (!button || button.disabled ||
              button.textContent.trim() !== "포지션 종료") {
            return;
          }
          hostDocument.getElementById(overlayId)?.remove();
          const overlay = hostDocument.createElement("div");
          overlay.id = overlayId;
          overlay.setAttribute("role", "status");
          overlay.setAttribute("aria-live", "assertive");
          overlay.innerHTML = `
            <div class="progress-card">
              <div class="progress-spinner" aria-hidden="true"></div>
              <div>포지션 종료 진행 중입니다</div>
              <div class="progress-detail">처리가 완료될 때까지 잠시 기다려 주세요.</div>
            </div>
          `;
          hostDocument.body.appendChild(overlay);
        }, true);
      })();
    </script>
    """


def clear_manual_close_progress_document() -> str:
    """완료 또는 실패한 포지션 종료 오버레이를 제거한다."""
    return """
    <script>
      window.parent.document.getElementById(
        "manual-close-progress-overlay"
      )?.remove();
    </script>
    """
