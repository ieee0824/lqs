# LQS 設計

LQS は、開発・テスト用の SQLite ベース SQS シミュレーターです。`Queue` は通常キューと FIFO キューを持ち、利用側は同じ `send` / `receive` / `delete` API を使います。

## キュー種別

| 種別 | キュー名 | 配信保証 |
| --- | --- | --- |
| Standard | 任意（`.fifo` 以外） | 到着順を基本に配信。順序は保証しない設計へ拡張可能 |
| FIFO | 必ず `.fifo` で終わる | 同一 `MessageGroupId` 内での先入れ先出し、同一グループの同時配信なし |

FIFO では送信時に `MessageGroupId` が必須です。重複排除IDは明示的に指定でき、`content_based_deduplication` を有効にしたキューは本文から安定ハッシュを生成できます。重複排除ウィンドウ内の再送は成功として扱いますが、新しいメッセージは積みません。

## 状態遷移

```text
send -> available -> receive -> in-flight -- delete --> removed
                    ^                |
                    |                +-- visibility timeout --> available
```

FIFO の `receive` は、未削除の先行メッセージがあるグループを後続メッセージより常に優先します。また、先行メッセージが in-flight の間はそのグループの次のメッセージを返しません。他グループは並列に受信できます。

## 永続化

SQLite をDB層として使い、キュー定義・メッセージ・FIFO重複排除キーをそれぞれ `queues`、`messages`、`deduplication_keys` テーブルへ保存します。`Lqs::open(path)` は指定ファイルを開いてスキーマを自動作成します。テスト向けの `Lqs::new()` は同じスキーマを SQLite のインメモリDBに作成します。

`receive` はトランザクションで候補の選択と可視性タイムアウトへの遷移を行います。FIFO候補は `NOT EXISTS` により、同一グループの先行メッセージまたはin-flightメッセージがあれば除外します。

## HTTP API

HTTP層は `ServerConfig`、Axumルーター、SQLite-backed `Lqs`を分離しています。CLIは環境変数からサーバー設定を構築してSQLite接続を所有し、ルーターへ渡します。テストでは同じルーターへインメモリDBを注入できます。

`CreateQueue`、`SendMessage`、`ReceiveMessage`、`DeleteMessage`、`ChangeMessageVisibility`を対象とし、AWS JSON 1.0のquery-compatible形式と従来のSQS Query形式を受け付けます。応答形式はリクエストのプロトコルに合わせ、すべての応答へリクエストIDを付与します。AWS署名は受け入れますが検証しません。

サーバーからライブラリAPIへ渡す時刻にはUnix時刻のミリ秒を使い、SQLiteファイルを開き直した後も可視性期限を比較できるようにします。

## DLQと再投入

`redrive_policies`にソース・DLQ・上限受信回数を、`dead_letter_origins`に移動したメッセージの元キューを保存します。既存DBには追加テーブルと索引のみを作成するため、既存キュー・メッセージを保持したまま更新できます。キュー作成とポリシー登録は同一トランザクションで行い、不正設定の場合はキューも残しません。

`receive`は候補選択・回数判定・DLQ移動・可視性更新を単一のImmediateトランザクションで行います。上限に達したメッセージは可視になるまで移動せず、その後のソース受信で移動します。候補はFIFOの先頭制約を通るため、in-flight中の先行メッセージを追い越しません。移動先では新しいsequenceを採番して末尾へ追加し、旧receipt handleを破棄し、受信回数をリセットします。本文・MessageId・グループIDは保持します。Standardの送信時刻は保持し、FIFOの送信時刻は移動時刻にします。

元キューへの再投入は`redrive_dead_letters`で同期実行できます。元キュー情報が一致する可視メッセージだけを、FIFOグループの先頭制約を維持して移動します。再投入は新しいMessageId・sequence・送信時刻・受信回数を使い、元の送信の重複排除キーには抑制されません。DLQへ直接送信されたメッセージや他ソース由来のメッセージは対象外です。ポリシー解除後も元キュー情報を保持するため再投入可能です。

DLQ移動・再投入の途中でDB操作が失敗すると、削除を含む全変更がロールバックされます。FIFOでは各キュー内の順序を維持しますが、失敗メッセージを別キューへ分離した後の業務処理全体の順序は保証しません。これは[AWSのDLQに関する注意](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)と同じ制約です。

HTTPはJSON/Query双方でRedrivePolicyのCreate/Set/GetとListDeadLetterSourceQueuesに対応します。ARNは`arn:aws:sqs:us-east-1:000000000000:<queue-name>`固定です。非同期のStartMessageMoveTask/Cancel/List操作、RedriveAllowPolicy、メッセージ保持期限はこの実装の対象外です。

## 時刻とテスト容易性

API はホストの時計を直接読まず、呼び出し側から単調増加の `now_ms` を渡します。これにより、可視性タイムアウトと重複排除の境界を sleep なしで決定的にテストできます。実運用用のアダプターでは `Instant` などの単調時計をミリ秒へ変換して渡します。

## 境界と将来の拡張

- SQLiteファイルへの永続化とDLQ転送に対応。ロングポーリングは未実装。
- APIエラーは `LqsError` として返し、設定または送信・受信の誤りを明示する。
- FIFO のスループット分割は `MessageGroupId` が単位。独立した処理を並列化したい場合はグループを分ける。
