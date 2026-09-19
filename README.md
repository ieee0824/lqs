# LQS

開発・テスト用の SQLite ベース SQS シミュレーターです。Standard / FIFO キューをサポートし、AWS SDK から SQS 互換 HTTP API に接続するか、Rust ライブラリとして利用できます。キュー設定・メッセージ・FIFO 重複排除などの状態は SQLite に永続化します。

> ローカル開発専用です。AWS 署名の検証や実データの暗号化は行いません。信頼できないクライアントやインターネットへ公開しないでください。

[クイックスタート](#quickstart) · [設定](#configuration) · [対応機能](#features) · [Rust ライブラリ](#rust) · [制約](#limitations) · [テスト](#tests)

<a id="quickstart"></a>

## クイックスタート

Rust / Cargo（edition 2024 対応）が必要です。リポジトリのルートで起動します。

```bash
cargo run --locked
```

別ターミナルで稼働確認とキュー作成ができます。

```bash
curl --fail http://127.0.0.1:9324/health

curl --fail http://127.0.0.1:9324/ \
  --data-urlencode 'Action=CreateQueue' \
  --data-urlencode 'QueueName=orders'
```

既定では `127.0.0.1:9324` で待ち受け、カレントディレクトリの `lqs.sqlite` に保存します。停止・再起動後もデータは残ります。

### AWS SDK から接続する

SDK の SQS クライアントに以下を設定してください。JSON / SQS Query の両形式を受け付けます。

| 項目 | ローカル用の設定値 |
| --- | --- |
| エンドポイント | `http://127.0.0.1:9324` |
| リージョン | `us-east-1` |
| アクセスキー / シークレットキー | ダミー値（例: どちらも `test`） |
| Queue URL | `CreateQueue` または `GetQueueUrl` の戻り値 |

実際の接続例は [AWS SDK for Go v2 の結合テスト](integration/sqs_test.go) を参照してください。署名から呼び出し元の権限を推定することはありません。

<a id="configuration"></a>

## サーバー設定

| 環境変数 | 既定値 | 用途 |
| --- | --- | --- |
| `LQS_BIND_ADDR` | `127.0.0.1:9324` | 待ち受けアドレス |
| `LQS_BASE_URL` | `http://` + 待ち受けアドレス | Queue URL に使用する公開 URL |
| `LQS_DATABASE_PATH` | `lqs.sqlite` | SQLite ファイル |
| `LQS_TRUST_PRINCIPAL_HEADER` | `false` | `true` の場合、権限テスト用の `x-lqs-principal` を受け付ける |

`x-lqs-principal` はクライアントの自己申告であり、認証ではありません。有効化はローカルの権限テストに限定してください。詳細は [呼び出し元の識別・ポリシー](docs/compatibility.md#security) を参照してください。

<a id="features"></a>

## 対応機能

以下の HTTP API を JSON / Query の両方で利用できます。応答・エラーコード・リクエスト ID は SQS 互換形式です。

| 分類 | API |
| --- | --- |
| キュー管理 | `CreateQueue` / `ListQueues` / `GetQueueUrl` / `DeleteQueue` / `PurgeQueue` |
| 属性・メトリクス | `GetQueueAttributes` / `SetQueueAttributes` |
| メッセージ | `SendMessage` / `ReceiveMessage` / `DeleteMessage` / `ChangeMessageVisibility` |
| バッチ | `SendMessageBatch` / `DeleteMessageBatch` / `ChangeMessageVisibilityBatch` |
| DLQ | `RedrivePolicy` 属性 / `ListDeadLetterSourceQueues` |
| タグ | `TagQueue` / `UntagQueue` / `ListQueueTags` |
| 権限 | `AddPermission` / `RemovePermission` / `Policy` 属性 |

設定範囲・既定値・AWS との差異は [SQS 互換仕様・制約](docs/compatibility.md) にまとめています。

- [配信](docs/compatibility.md#delivery): 遅延、保持期限、本文・属性のサイズ制限
- [受信](docs/compatibility.md#polling): ロングポーリング、in-flight 上限、可視性タイムアウト
- [FIFO](docs/compatibility.md#fifo): グループ順序、固定 5 分の重複排除、受信再試行、旧 DB の移行時の注意
- [属性](docs/compatibility.md#attributes): String / Number / Binary、MD5、システム属性
- [バッチ](docs/compatibility.md#batch) / [DLQ](docs/compatibility.md#dlq): 部分成功、転送、Rust からの再投入
- [キュー管理](docs/compatibility.md#management): ページング、タグ、メトリクス、パージ・削除の安全性
- [セキュリティ](docs/compatibility.md#security): ポリシー評価、認可フック、SSE-SQS / KMS 設定モデル

<a id="rust"></a>

## Rust ライブラリ

HTTP サーバーを介さずに利用できます。次の例はインメモリ DB で FIFO キューを作成し、送信・受信・削除します。

```rust
use lqs::{Lqs, QueueOptions, QueueType, SendRequest};

fn main() -> Result<(), lqs::LqsError> {
    let mut lqs = Lqs::in_memory()?;
    lqs.create_queue(
        "orders.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..QueueOptions::default()
        },
    )?;

    lqs.send("orders.fifo", SendRequest::fifo("created", "order-42"), 0)?;
    if let Some(message) = lqs.receive("orders.fifo", 1, 1)?.pop() {
        // 処理が成功した場合にだけ削除する
        lqs.delete("orders.fifo", &message.receipt_handle)?;
    }
    Ok(())
}
```

永続化する場合は `Lqs::open("lqs.sqlite")?` を使います。必要なスキーマは自動作成・移行されます。`Lqs::new()` / `Lqs::in_memory()` はテスト用のインメモリ DB です。

時刻を `now_ms`（ミリ秒）として渡すため、期限や重複排除をテストで再現できます。永続 DB を複数の実行から利用する場合は、Unix 時刻など一貫した基準を使ってください。同期の `receive` 自体は待機せず、ロングポーリングは HTTP 層で行います。

Rust の直接操作は信頼された管理用 API で、HTTP の認可を自動適用しません。内部構造は [DESIGN.md](DESIGN.md) を参照してください。

<a id="limitations"></a>

## 利用上の制約

- AWS SQS の完全な代替ではありません。リージョンは `us-east-1`、アカウントは `000000000000` 固定です。分散処理、リージョン別 TPS、非同期の設定反映・近似メトリクス更新は再現しません。
- Policy 未設定のキューはローカル開放モードです。設定済みの場合は明示的 Allow が必要で、Deny が優先します。`Policy=""` で解除すると再び開放されます。
- SSE-SQS / KMS は設定モデルのみです。本文・属性・SQLite / WAL・バックアップは平文のままです。TLS、SigV4 認証、CloudTrail 相当の監査ログも提供しません。
- HTTP の非同期 DLQ move-task API、`RedriveAllowPolicy`、ポリシーの `Condition` / `Not*`、`AWSTraceHeader` 送信、メッセージ属性のリスト値は未対応です。
- バッチは HTTP 200 でも部分失敗があります。必ず `Failed` を確認してください。パージ・削除したメッセージは復元できません。

<a id="tests"></a>

## 開発・テスト

### Rust

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
```

### AWS SDK / Query 結合テスト

Go 1.24 以降が必要です（[go.mod](integration/go.mod)）。通常のサーバーとは別ポート・一時 DB で、権限テスト用モードを起動します。

```bash
lqs_test_dir=$(mktemp -d)
LQS_BIND_ADDR=127.0.0.1:19324 \
LQS_DATABASE_PATH="$lqs_test_dir/lqs.sqlite" \
LQS_TRUST_PRINCIPAL_HEADER=true \
cargo run --locked
```

別ターミナルで実行します。

```bash
cd integration
LQS_ENDPOINT=http://127.0.0.1:19324 go test -count=1 -v ./...
```

保持期限の 60 秒を実時間で検証するため、全体で 1 分以上かかります。テスト終了後はサーバーを `Ctrl+C` で停止してください。一時 DB は検証用に残ります。

GitHub Actions でも Rust の検査と SDK / Query 結合テストを実行します。配信・FIFO・DLQ・バッチ・属性・キュー管理・認可・SSE/KMS 設定を検証し、Rust テストでは永続化・旧 DB 移行・競合・DB 障害時の動作も確認します。
