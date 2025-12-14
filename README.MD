# Outbox → S3 Sync (Notes)

iPhone/iPad のショートカットで iCloud Drive に吐き出したメモ（テキスト）を、Mac から手動で AWS S3 にアップロードし、
成功したファイルだけ `Outbox/_sent/...` に退避するためのスクリプト。

## 目的

- iCloud は “一時置き場（Outbox）として使用。”
- 原本は S3 に集約（後で Comprehend 等に連携しやすくする）し、ローカルのストレージと同期させて使用することを想定する。
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

2) 実行権限を付与
```bash
chmod +x ~/src/outbox-s3-sync/scripts/send_notes_outbox_to_s3.command

3) 設定
BUCKET="YOUR-BUCKET-NAME"
S3_PREFIX_BASE="raw"（通常はそのまま）
⚠️ AWSアクセスキーなどの秘密情報はコミットしないこと。

4) 実行
```bash
~/src/outbox-s3-sync/scripts/send_notes_outbox_to_s3.command


最初に Dry-run（送信候補）が表示されます
y を入力するとアップロードを実行します
成功したファイルのみ _sent に移動します
ログは ~/sync-logs/ に出ます

よくあるトラブル
認証エラー
Mac で以下が通るか要確認。

```bash
aws s3 ls


macOS が実行をブロックする（quarantine）
```bash
xattr -dr com.apple.quarantine ~/src/outbox-s3-sync

セキュリティメモ
IAM は最小権限（対象バケット + raw/notes/ だけ書ける等）を推奨
iPhone/iPad には AWS の鍵を置かない運用にする


