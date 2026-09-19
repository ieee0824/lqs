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

## DLQ（Dead-letter queue）

先にソースと同じ種別のDLQを作成し、`CreateQueue`の属性または`SetQueueAttributes`で`RedrivePolicy`を設定します。

```json
{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:failed.fifo","maxReceiveCount":3}
```

ローカルARNのリージョンは`us-east-1`、アカウントは`000000000000`固定です。`GetQueueAttributes`の`QueueArn`からDLQのARNを取得できます。`maxReceiveCount`は1〜1000で、ソースとDLQの種別一致・DLQの存在を検証します。`RedrivePolicy`を空文字列に設定すると解除できます。

上限回数の受信後、メッセージが再び可視になった状態でソースを受信すると、メッセージをDLQへ原子的に移します。処理中のメッセージは移動しません。`ListDeadLetterSourceQueues`でDLQを参照するソース一覧を取得できます（`MaxResults` / `NextToken`対応）。設定・受信回数・元キュー情報はSQLiteへ永続化します。

Rustでは`QueueOptions::redrive_policy`、`set_redrive_policy`、`redrive_policy`、`list_dead_letter_source_queues`を利用できます。再投入の基礎APIは`redrive_dead_letters(dlq, source, max_messages, now_ms)`です。元キューが一致する可視メッセージを新しいID・受信回数で元キューの末尾へ戻します。HTTPの非同期move-task APIは未実装です。

属性管理は今回必要な範囲に対応しています。`SetQueueAttributes`は`RedrivePolicy`のみ、`GetQueueAttributes`は`QueueArn`・`RedrivePolicy`・`VisibilityTimeout`・FIFO設定と`All`を扱います。タグなどのキュー管理機能は別途実装予定です。

## テスト

Rustの単体テストと、実際のHTTPサーバーへAWS SDK for Go v2で接続する結合テストがあります。

```bash
cargo test

# 別ターミナルで cargo run を実行してから
cd integration
LQS_ENDPOINT=http://127.0.0.1:9324 go test -v ./...
```

GitHub ActionsではRustの整形・Clippy・単体テストに続けてLQSサーバーを起動し、Go SDKからFIFOキューの作成、送信、受信、可視性変更、削除、エラーコードの復元を検証します。
DLQについてもStandard/FIFOの転送、属性設定・取得、ソース一覧のページング、ポリシー解除、不正設定時のHTTP 400を同じCIで検証します。
