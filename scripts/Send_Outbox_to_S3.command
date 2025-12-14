#!/bin/bash
set -euo pipefail

# ====== 設定ここだけ直す ======
BUCKET="MYBUCKET"     # 例: my-data-lake-bucket
S3_PREFIX_BASE="raw"          # 例: raw
# ==============================

OUTBOX="$HOME/Library/Mobile Documents/com~apple~CloudDocs/Outbox"
SRC_DIR="$OUTBOX/notes"

LOG_DIR="$HOME/sync-logs"
TS="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/outbox_notes_to_s3_${TS}.log"

YYYY="$(date +%Y)"
MM="$(date +%m)"
DD="$(date +%d)"
SENT_BASE="$OUTBOX/_sent/notes/$YYYY/$MM/$DD"

mkdir -p "$LOG_DIR"

echo "=== Outbox/notes -> S3（成功分だけ _sent へ移動） ===" | tee -a "$LOG_FILE"
echo "Outbox: $OUTBOX" | tee -a "$LOG_FILE"
echo "From:   $SRC_DIR" | tee -a "$LOG_FILE"
echo "To:     s3://$BUCKET/$S3_PREFIX_BASE/notes/" | tee -a "$LOG_FILE"
echo "Log:    $LOG_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI が見つからない。先に入れて。" | tee -a "$LOG_FILE"
  exit 1
fi

if [ ! -d "$SRC_DIR" ]; then
  echo "notes フォルダがない: $SRC_DIR" | tee -a "$LOG_FILE"
  exit 0
fi

# --- DRY-RUN（送信候補一覧）---
TOTAL_FILES=0
TOTAL_BYTES=0

echo "----- DRY-RUN（送信候補）-----" | tee -a "$LOG_FILE"

# findの結果をヌル区切りで読む（スペース安全）
while IFS= read -r -d '' f; do
  SZ=$(stat -f%z "$f" 2>/dev/null || echo 0)
  echo "  + $f ($SZ bytes)" | tee -a "$LOG_FILE"
  TOTAL_FILES=$((TOTAL_FILES + 1))
  TOTAL_BYTES=$((TOTAL_BYTES + SZ))
done < <(find "$SRC_DIR" -type f -print0)

echo "" | tee -a "$LOG_FILE"
echo "合計: $TOTAL_FILES files / $TOTAL_BYTES bytes" | tee -a "$LOG_FILE"

if [ "$TOTAL_FILES" -eq 0 ]; then
  echo "送るものがない。終了。" | tee -a "$LOG_FILE"
  exit 0
fi

read -r -p "この内容でアップロードして、成功分だけ _sent に移動する？ (y/N): " ANS
if [[ "$ANS" != "y" && "$ANS" != "Y" ]]; then
  echo "中止。ログだけ残した。" | tee -a "$LOG_FILE"
  exit 0
fi

echo "" | tee -a "$LOG_FILE"
echo "===== UPLOAD START =====" | tee -a "$LOG_FILE"

FAIL_COUNT=0

while IFS= read -r -d '' f; do
  rel="${f#"$SRC_DIR"/}"
  s3dst="s3://$BUCKET/$S3_PREFIX_BASE/notes/$rel"
  sent_path="$SENT_BASE/$rel"

  echo "UPLOAD: $f -> $s3dst" | tee -a "$LOG_FILE"

  if aws s3 cp "$f" "$s3dst" --only-show-errors 2>&1 | tee -a "$LOG_FILE"; then
    mkdir -p "$(dirname "$sent_path")"
    mv "$f" "$sent_path"
    echo "OK: moved to $sent_path" | tee -a "$LOG_FILE"
  else
    echo "NG: upload failed, keep in Outbox: $f" | tee -a "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
done < <(find "$SRC_DIR" -type f -print0)

# 空ディレクトリ掃除（見た目スッキリ）
find "$SRC_DIR" -mindepth 1 -type d -empty -delete >/dev/null 2>&1 || true

echo "" | tee -a "$LOG_FILE"
echo "===== UPLOAD DONE =====" | tee -a "$LOG_FILE"

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "失敗: $FAIL_COUNT 件。失敗分は Outbox に残してある。" | tee -a "$LOG_FILE"
  exit 2
fi

echo "全部成功。Outbox/notes は軽くなってるはず。" | tee -a "$LOG_FILE"

