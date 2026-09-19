# LQS

開発・テスト用のSQLiteベース SQS シミュレーターです。Standard キューと FIFO キューをサポートします。キュー定義、メッセージ、FIFO重複排除キーはSQLiteへ永続化され、AWS SDKからSQS互換HTTP APIへ接続できます。

## HTTPサーバー

```bash
cargo run
curl http://127.0.0.1:9324/health
```

サーバーは次の環境変数で設定できます。

| 環境変数 | 既定値 | 用途 |
| --- | --- | --- |
| `LQS_BIND_ADDR` | `127.0.0.1:9324` | 待ち受けアドレス |
| `LQS_BASE_URL` | 待ち受けアドレスから生成 | Queue URLに使用する公開URL |
| `LQS_DATABASE_PATH` | `lqs.sqlite` | SQLiteファイル |

`CreateQueue`、`SendMessage`、`ReceiveMessage`、`DeleteMessage`、`ChangeMessageVisibility`をサポートします。現行AWS SDKが使用するJSON形式と、`Action=...`を送るSQS Query形式の両方を受け付けます。成功応答、エラーコード、リクエストIDはSQS互換形式で返します。署名は検証しないため、ローカル用のダミー認証情報を利用できます。

## Rustライブラリ

```rust
use lqs::{Lqs, QueueOptions, QueueType, SendRequest};

let mut lqs = Lqs::open("lqs.sqlite")?;
lqs.create_queue(
    "orders.fifo",
    QueueType::Fifo,
    QueueOptions {
        content_based_deduplication: true,
        ..QueueOptions::default()
    },
)?;

lqs.send("orders.fifo", SendRequest::fifo("created", "order-42"), 0)?;
let message = lqs.receive("orders.fifo", 1, 1)?.pop().unwrap();
// 処理が成功した場合にだけ削除する
lqs.delete("orders.fifo", &message.receipt_handle)?;
# Ok::<(), lqs::LqsError>(())
```

`Lqs::open(path)` は指定されたSQLiteファイルを開き、必要なテーブルと索引を自動作成します。`Lqs::new()` / `Lqs::in_memory()` はテスト用の一時DBです。

時刻を引数 `now_ms` として渡すため、可視性タイムアウトと FIFO 重複排除をテストで再現できます。設計と制約は [DESIGN.md](DESIGN.md) を参照してください。

## テスト

Rustの単体テストと、実際のHTTPサーバーへAWS SDK for Go v2で接続する結合テストがあります。

```bash
cargo test

# 別ターミナルで cargo run を実行してから
cd integration
LQS_ENDPOINT=http://127.0.0.1:9324 go test -v ./...
```

GitHub ActionsではRustの整形・Clippy・単体テストに続けてLQSサーバーを起動し、Go SDKからFIFOキューの作成、送信、受信、可視性変更、削除、エラーコードの復元を検証します。
