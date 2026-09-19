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

`CreateQueue`、`SendMessage`、`ReceiveMessage`、`DeleteMessage`、`ChangeMessageVisibility`と、送信・削除・可視性変更のバッチ操作をサポートします。現行AWS SDKが使用するJSON形式と、`Action=...`を送るSQS Query形式の両方を受け付けます。成功応答、エラーコード、リクエストIDはSQS互換形式で返します。署名は検証しないため、ローカル用のダミー認証情報を利用できます。

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

## バッチ操作

`SendMessageBatch`、`DeleteMessageBatch`、`ChangeMessageVisibilityBatch`は1〜10件を処理します。各エントリーの`Id`は1〜80文字の英数字・`-`・`_`で、バッチ内で一意にします。送信本文とメッセージ属性の合計は最大1 MiBです。

件数・ID・合計サイズが不正な場合はHTTP 400となり、何も変更しません。それ以外のエントリー単位の失敗はHTTP 200の`Failed`へ、成功は`Successful`へ返します。**HTTP 200でも必ず`Failed`を確認してください。** エラーには`Id`・`Code`・`Message`・`SenderFault`が含まれます。

FIFOは入力順で処理し、単体送信と同じ重複排除を適用します。グループID・明示的な重複排除IDは1〜128文字のASCII英数字・記号です。成功エントリーごとにコミットするため、途中の失敗で他の成功分が取り消されることはありません。

Rustでは`send_batch(queue, Vec<BatchEntry<SendRequest>>, now_ms)`、`delete_batch(queue, Vec<BatchEntry<String>>)`、`change_visibility_batch(queue, Vec<BatchEntry<VisibilityChange>>, now_ms)`を利用できます。`BatchEntry`は`id`と`value`、戻り値の`BatchResult`は`successful`と`failed`を持ちます。`VisibilityChange`には`receipt_handle`と`timeout_ms`（0〜43,200,000ミリ秒）を指定します。

## DLQ（Dead-letter queue）

先にソースと同じ種別のDLQを作成し、`CreateQueue`の属性または`SetQueueAttributes`で`RedrivePolicy`を設定します。

```json
{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:failed.fifo","maxReceiveCount":3}
```

ローカルARNのリージョンは`us-east-1`、アカウントは`000000000000`固定です。`GetQueueAttributes`の`QueueArn`からDLQのARNを取得できます。`maxReceiveCount`は1〜1000で、ソースとDLQの種別一致・DLQの存在を検証します。`RedrivePolicy`を空文字列に設定すると解除できます。

上限回数の受信後、メッセージが再び可視になった状態でソースを受信すると、メッセージをDLQへ原子的に移します。処理中のメッセージは移動しません。`ListDeadLetterSourceQueues`でDLQを参照するソース一覧を取得できます（`MaxResults` / `NextToken`対応）。設定・受信回数・元キュー情報はSQLiteへ永続化します。

Rustでは`QueueOptions::redrive_policy`、`set_redrive_policy`、`redrive_policy`、`list_dead_letter_source_queues`を利用できます。再投入の基礎APIは`redrive_dead_letters(dlq, source, max_messages, now_ms)`です。元キューが一致する可視メッセージを新しいID・受信回数で元キューの末尾へ戻します。HTTPの非同期move-task APIは未実装です。

属性管理は`RedrivePolicy`と下記の配信設定を扱います。`GetQueueAttributes`では`QueueArn`・`VisibilityTimeout`・FIFO設定と`All`も取得できます。キュー管理・タグ操作については後述します。

## 遅延・保持期限・サイズ制限

`CreateQueue` / `SetQueueAttributes`で設定し、`GetQueueAttributes`で取得できます。

| HTTP属性 | 範囲・既定値 | RustのQueueOptions |
| --- | --- | --- |
| `DelaySeconds` | 0〜900秒、既定0 | `delay_ms`（ミリ秒） |
| `MessageRetentionPeriod` | 60〜1,209,600秒、既定345,600秒（4日） | `message_retention_ms`（ミリ秒） |
| `MaximumMessageSize` | 1,024〜1,048,576バイト、既定1 MiB | `maximum_message_size` |

本文自体は1バイト以上が必要で、本文とメッセージ属性の合計が設定上限まで送信できます。文字列はJSON/URLエンコード前のUTF-8バイト数、Binary属性はBase64化前の生バイト数です。Standardは`SendMessage.DelaySeconds`（Rustは`SendRequest.delay_ms`）でキュー既定値を上書きでき、明示的な0は即時配信になります。FIFOはキュー単位のみで、メッセージ単位の指定は0を含めエラーになります。

遅延は初回配信、可視性タイムアウトは受信後の再配信に適用します。保持期限は送信時刻から数え、遅延中・処理中でも期限到達後は配信しません。送信・受信・再投入時に対象キューの期限切れデータを削除します。`queue_depth`は時刻を受け取らないため、削除前の期限切れ行を含む物理件数です。

設定変更はLQSでは即時反映されます。保持期間を短くすると既存メッセージにも適用します。Standardの遅延変更は新規メッセージだけ、FIFOでは未受信メッセージの遅延期限も更新します。Rustからは`set_queue_attributes(name, QueueUpdate { .. }, now_ms)`で更新できます。

## メッセージ属性とシステム属性

`SendMessage` / `SendMessageBatch`の`MessageAttributes`に最大10個の属性を指定できます。`String`・`Number`・`Binary`と、`String.label`・`Number.int`・`Binary.image`などのカスタム接尾辞を保持します。Numberは最大38桁の精度で検証し、余分な先頭・末尾ゼロを除いた十進表現で保存します。BinaryはJSON/QueryではBase64、Rustでは`Vec<u8>`です。

Rustでは`SendRequest::message_attributes`に`MessageAttributes`（属性名から`MessageAttribute { data_type, value }`へのマップ）を指定します。`value`は`MessageAttributeValue::String`（Numberも同じ）または`Binary`です。受信結果の`message_attributes`と`system_attributes`には全属性が入ります。

HTTPの`ReceiveMessage`では、要求された属性だけを返します。指定なしの場合は属性と属性MD5を省略します。

- `MessageAttributeNames`: 属性名、`All`、`.*`、`prefix.*`を指定できます。
- `MessageSystemAttributeNames`: `SentTimestamp`、`ApproximateFirstReceiveTimestamp`、`ApproximateReceiveCount`、`SenderId`、`SqsManagedSseEnabled`、`MessageGroupId`、`MessageDeduplicationId`、`SequenceNumber`、`DeadLetterQueueSourceArn`または`All`を指定できます。FIFO/DLQ固有の属性は該当時だけ返します。
- 旧`AttributeNames`もシステム属性の指定として受け付けます。Query形式では`MessageAttributeName.N`、`MessageSystemAttributeName.N`、`AttributeName.N`を使います。

本文と属性はSQLiteに保存され、再起動・再受信・DLQ転送でも保持されます。初回受信時刻は再受信で変わりません。`ApproximateReceiveCount`はDLQ転送をまたいで累計し、手動再投入では新規メッセージとして時刻・回数をリセットします。既存DBの過去の初回受信時刻は復元できないため、移行後の最初の受信時刻になります。

属性サイズは名前・型名（接尾辞込み）・値を合算し、単体上限とバッチ合計上限の両方に含めます。属性MD5は送信応答と、選択された受信属性に対して返します。FIFOの本文ベース重複排除は属性を含めず、本文が同じなら属性が異なっても重複扱いとなり、元の属性は上書きしません。生成する重複排除IDには本文のUTF-8バイト列のSHA-256（小文字16進数）を使用します。

署名を検証しないローカルサービスのため`SenderId`は`000000000000`、暗号化状態は`false`固定です。X-Rayの`AWSTraceHeader`送信や属性のリスト値は未対応です。

## ロングポーリングとin-flight上限

`ReceiveMessage.WaitTimeSeconds`は0〜20秒です。省略時はキュー属性`ReceiveMessageWaitTimeSeconds`（既定0秒）、明示的な0は即時の短ポーリングになります。キュー属性は`CreateQueue` / `SetQueueAttributes` / `GetQueueAttributes`で管理できます。Rustの設定は`QueueOptions::receive_wait_time_ms` / `QueueUpdate::receive_wait_time_ms`です。

ロングポーリングは、受信可能なメッセージが見つかるとすぐ返り、見つからない間は指定期限まで待ちます。待機中にDBをロックせず100ms間隔で再確認するので、他の送受信、遅延や可視性期限の終了、別DB接続からの書き込みにも対応します。`MaxNumberOfMessages`（Rustの`receive`の件数引数も同様）は1〜10件です。Rustの同期`receive`自体は待機せず、HTTP層が非同期の待機を行います。

in-flight上限はキューごとに既定120,000件です。ローカル検証用の独自属性`LqsMaxInFlightMessages`（1〜120,000件）で小さくできます。この属性はAWSにはありません。Rustでは`QueueOptions::max_in_flight` / `QueueUpdate::max_in_flight`で設定します。

| 上限到達時 | Standard | FIFO |
| --- | --- | --- |
| 短ポーリング | HTTP 400 `OverLimit` | 空結果 |
| ロングポーリング | 空きができるまで待機、期限到達で空結果 | 同左 |

削除、可視性期限の終了・0への変更、保持期限で空きが戻ります。`GetQueueAttributes`の`ApproximateNumberOfMessagesNotVisible`とRustの`in_flight_count(queue, now_ms)`で現在の件数を取得できます。上限を現在の件数より小さくしても受信済みメッセージは取り消さず、件数が下がるまで新規受信を止めます。可視性期限切れのハンドルでの延長は`MessageNotInflight`になります。

## FIFO の重複排除と受信再試行

送信の重複排除期間は最初の送信から固定5分です。再送で期間は延長せず、メッセージを削除してもキーは期間中保持します。明示的な`MessageDeduplicationId`は本文ハッシュより優先され、生成済みハッシュと同じIDなら同じ重複排除キーになります。Rustの既存フィールド`deduplication_window_ms`には300,000以外を指定できません。

`CreateQueue` / `SetQueueAttributes` / `GetQueueAttributes`で次のFIFO専用属性を扱えます。

| 属性 | 値 | 既定値 |
| --- | --- | --- |
| `ContentBasedDeduplication` | `true` / `false` | `false` |
| `DeduplicationScope` | `queue` / `messageGroup` | `queue` |
| `FifoThroughputLimit` | `perQueue` / `perMessageGroupId` | `perQueue` |

`perMessageGroupId`には`messageGroup`が必須です。設定の組み合わせは更新後の値で検証し、不正なら全属性を変更しません。`messageGroup`では同じ重複排除IDでもグループが異なれば別メッセージとして受理します。LQSは設定と重複排除範囲をモデル化しますが、AWSのリージョン別TPS制限・パーティション分散は再現しません。

FIFOの`ReceiveMessage.ReceiveRequestAttemptId`には1〜128文字のASCII英数字・記号を指定できます。同じIDの再試行は初回応答から5分間、同じメッセージ・receipt handle・受信回数を返し、可視性期限をリセットします。受信結果はSQLiteに保存するため再起動・別接続でも有効です。空の応答も保存し、ロングポーリングの空応答は待機終了時に確定します。初回受信で使った`MaxNumberOfMessages`と`VisibilityTimeout`（省略を含む）は再試行でも同じ指定にしてください。

対象の一部でも削除・可視性変更・別リクエストでの再受信・DLQ転送・保持期限切れが起きた場合、LQSはそのIDの再試行を`InvalidParameterValue`で拒否します。期限切れ後は同じIDを新しい受信として扱います。`VisibilityTimeout`は受信単位に0〜43,200秒で指定できます。Rustでは`receive_with_options`と`ReceiveOptions`を使用します。受信済み結果の再試行はin-flight上限到達時も可能ですが、可視性が切れたメッセージを再びin-flightにする際にローカル上限を超える場合は`OverLimit`です。

既存DBは自動移行し、メッセージ・明示的な重複排除キーを保持します。旧設定の重複排除窓は5分へ統一します。旧FNVハッシュを使う履歴は明示IDと区別できないためSHA-256へ書き換えません。アップグレードをまたぐ本文ベースの再送は重複排除されない可能性があるため、送信を5分以上停止してから切り替えるか、明示IDを利用してください。旧履歴で削除済みメッセージのグループが不明なキーは、残りの有効期間だけ全グループに適用します。

仕様参考: [FIFOの重複排除](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html)、[FIFO属性](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html)、[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)。

## キュー管理・タグ・メトリクス

JSON / Queryの両方で`ListQueues`、`GetQueueUrl`、`DeleteQueue`、`PurgeQueue`、`TagQueue`、`UntagQueue`、`ListQueueTags`を利用できます。

`ListQueues`は名前順・大文字小文字を区別するリテラルの`QueueNamePrefix`検索です。`MaxResults`は1〜1,000で、明示した場合だけ続きの`NextToken`を返します。次のページにも同じprefixを指定してください。ページングは名前を基準とし、一覧全体のスナップショットではありません。`GetQueueUrl`は名前からURLを取得し、任意の`QueueOwnerAWSAccountId`はローカルの`000000000000`のみを受け付けます。

`GetQueueAttributes`には以下も含まれます。属性を省略すると空、`All`は対応する全属性を返します。

| 属性 | LQSでの意味 |
| --- | --- |
| `ApproximateNumberOfMessages` | 可視状態のメッセージ数（FIFOで先行メッセージにブロックされたものを含む） |
| `ApproximateNumberOfMessagesNotVisible` | 可視性期限内のin-flight数 |
| `ApproximateNumberOfMessagesDelayed` | 遅延期限前でin-flightではない件数 |
| `FifoQueue` | FIFOは`true`、Standardは`false` |
| `CreatedTimestamp` / `LastModifiedTimestamp` | 作成・設定更新のUnix秒 |

メトリクスは保持期限切れを除いたSQLite上の即時集計です。AWSの非同期な近似更新は再現しません。旧DBで不明な作成・更新日時は0です。`SetQueueAttributes.VisibilityTimeout`は0〜43,200秒（既定30秒）で、受信済みの可視性期限は変更せず、次回の受信から適用します。設定変更とタグは再起動後も維持されます。IAMポリシー、KMS暗号化、RedriveAllowPolicy等の未対応設定はエラーにします。

タグはキー1〜128文字・値0〜256文字のUnicode文字列です。Unicode英数字・空白と`_ . : / = + - @`を使え、`aws:`プレフィックスは使用できません。LQSの上限は1キュー50タグです（AWSでは50以下が推奨）。同名キーは上書き、存在しないキーの削除は成功し、不正な更新は全体を巻き戻します。`CreateQueue`の`tags`にも対応します。同名・同設定での再作成は既存タグを変更しないため、更新には`TagQueue`を使ってください。Query形式は`Tag.N.Key` / `Tag.N.Value`、`TagKey.N`です。

### パージと削除の安全性

どちらも破壊的な操作で、削除したメッセージは復元できません。HTTPでは`LQS_BASE_URL/000000000000/キュー名`または`/000000000000/キュー名`の正確な指定を要求します。末尾のスラッシュは許容しますが、別ホスト・別アカウント・クエリ文字列・キュー名だけのURLでは実行しません。

- `PurgeQueue`は実行時点の対象キューの可視・in-flight・遅延メッセージを同一トランザクションで即時削除します。キュー設定・タグ・DLQ設定・送信重複排除キーは維持し、受信再試行の履歴を無効化します。別キューやDLQのメッセージ、パージ完了後の新規送信は削除しません。60秒以内の再パージは`AWS.SimpleQueueService.PurgeQueueInProgress`です。この待機時間も永続化します。AWSの最大60秒の非同期削除は再現しません。
- `DeleteQueue`は対象キューとそのメッセージ・タグ・重複排除・受信再試行データを原子的に削除します。対象を参照するDLQポリシーは解除し、他キューに残るメッセージの元キュー参照は外しますが、他キュー内のメッセージそのものは残します。同名キューは60秒間再作成できず、`AWS.SimpleQueueService.QueueDeletedRecently`となります。削除自体は即時です。

Rustでは`list_queues`、`queue_exists`、`queue_metrics`、`tag_queue`、`untag_queue`、`list_queue_tags`、`purge_queue`、`delete_queue`を利用できます。時刻を制御した作成・初期タグ設定には`create_queue_with_tags_at`を使用します。

仕様参考: [ListQueues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ListQueues.html)、[PurgeQueue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_PurgeQueue.html)、[DeleteQueue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteQueue.html)、[タグ制約](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html)。

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
バッチ3操作の部分成功、全体エラー、FIFO順序・重複排除、Queryの数値順・XMLエラーも検証します。RustテストではDB障害の注入と再起動後の永続化も確認します。
ロングポーリングの到着・期限・設定上書きと、Standard/FIFOのin-flight上限・バッチ操作による空きの解放も結合テストに含みます。Rustでは競合する待機リクエスト、別DB接続からの書き込み、同時受信の上限保証、待機futureのキャンセルと既存DB移行も確認します。
メッセージ属性の型・Binaryバイト列・MD5・受信時の選択、システム属性、属性込みのサイズとFIFO重複排除もSDK/Queryから検証します。RustではDB再起動・旧スキーマ移行・DLQ/再投入時の属性保持を確認します。
