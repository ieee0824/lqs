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

属性管理は`RedrivePolicy`と下記の配信設定を扱います。`GetQueueAttributes`では`QueueArn`・`VisibilityTimeout`・FIFO設定と`All`も取得できます。タグなどのキュー管理機能は別途実装予定です。

## 遅延・保持期限・サイズ制限

`CreateQueue` / `SetQueueAttributes`で設定し、`GetQueueAttributes`で取得できます。

| HTTP属性 | 範囲・既定値 | RustのQueueOptions |
| --- | --- | --- |
| `DelaySeconds` | 0〜900秒、既定0 | `delay_ms`（ミリ秒） |
| `MessageRetentionPeriod` | 60〜1,209,600秒、既定345,600秒（4日） | `message_retention_ms`（ミリ秒） |
| `MaximumMessageSize` | 1,024〜1,048,576バイト、既定1 MiB | `maximum_message_size` |

本文自体は1バイトから設定上限まで送信できます。サイズはJSON/URLエンコード前のUTF-8バイト数です。Standardは`SendMessage.DelaySeconds`（Rustは`SendRequest.delay_ms`）でキュー既定値を上書きでき、明示的な0は即時配信になります。FIFOはキュー単位のみで、メッセージ単位の指定は0を含めエラーになります。

遅延は初回配信、可視性タイムアウトは受信後の再配信に適用します。保持期限は送信時刻から数え、遅延中・処理中でも期限到達後は配信しません。送信・受信・再投入時に対象キューの期限切れデータを削除します。`queue_depth`は時刻を受け取らないため、削除前の期限切れ行を含む物理件数です。

設定変更はLQSでは即時反映されます。保持期間を短くすると既存メッセージにも適用します。Standardの遅延変更は新規メッセージだけ、FIFOでは未受信メッセージの遅延期限も更新します。Rustからは`set_queue_attributes(name, QueueUpdate { .. }, now_ms)`で更新できます。

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
遅延・1 MiB本文・設定上限・UTF-8サイズ・保持期限もGo SDKから検証します。保持期限テストはSQSの最短設定60秒を実時間で検証するため、結合テスト全体は約1分以上かかります。
