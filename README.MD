# Outbox → S3 Sync (Notes)

iPhone/iPad のショートカットで iCloud Drive に吐き出したメモ（テキスト）を、Mac から手動で AWS S3 にアップロードし、
成功したファイルだけ `Outbox/_sent/...` に退避するためのスクリプトです。

## 目的

- iCloud は “一時置き場（Outbox）”
- 原本は S3 に集約（後で Comprehend 等に連携しやすくする）
- Mac を常時起動しない前提で、手動トリガ + ログで運用する

## 想定フロー

1. **iPhone/iPad (Shortcuts)**
   - Notes から複数メモを選択
   - `iCloud Drive/Outbox/notes/` に「1メモ=1ファイル」で保存（ファイル名はユニーク）
2. **Mac (manual)**
   - スクリプト実行（Dry-run → 確認 → Upload）
   - 成功したファイルを `Outbox/_sent/notes/YYYY/MM/DD/` に移動
   - ログを `~/sync-logs/` に保存

## ディレクトリ構成

### iCloud Drive

- `Outbox/notes/`  
  未送信ファイル（ショートカットがここに保存）

- `Outbox/_sent/notes/YYYY/MM/DD/`  
  送信済みファイル（S3アップロード成功分）

### S3

- `s3://<BUCKET>/raw/notes/`  
  送信された原本（raw）

## 必要なもの

- macOS
- AWS CLI v2（`aws`）
- S3 バケット（Versioning 推奨）
- AWS 認証情報（**Mac のみに設定**）

## セットアップ

1) リポジトリを配置
```bash
git clone https://github.com/<you>/<repo>.git ~/src/outbox-s3-sync

