# LQS

開発・テスト用のSQLiteベース SQS シミュレーターです。Standard キューと FIFO キューをサポートします。キュー定義、メッセージ、FIFO重複排除キーはSQLiteへ永続化されます。

```bash
cargo test
cargo run
```

`cargo run` は FIFO の例を実行します。出力では `order-A` の2件目が、1件目を削除するまで配信されず、別グループの `order-B` は並列に受信されることを確認できます。

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
