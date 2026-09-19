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

`CreateQueue`、`SendMessage`、`ReceiveMessage`、`DeleteMessage`、`ChangeMessageVisibility`と送信・削除・可視性変更のバッチ操作を対象とし、AWS JSON 1.0のquery-compatible形式と従来のSQS Query形式を受け付けます。応答形式はリクエストのプロトコルに合わせ、すべての応答へリクエストIDを付与します。AWS署名は受け入れますが検証しません。

サーバーからライブラリAPIへ渡す時刻にはUnix時刻のミリ秒を使い、SQLiteファイルを開き直した後も可視性期限を比較できるようにします。

## バッチの検証とトランザクション境界

ライブラリの3バッチAPIとHTTPアダプターは共通の`run_batch`を使います。件数（1〜10件）・ID形式と重複・キューの存在を先に検証し、送信ではデコード後の本文合計1 MiBも書き込み前に検証します。全体エラーは`EmptyBatchRequest`、`TooManyEntriesInBatchRequest`、`InvalidBatchEntryId`、`BatchEntryIdsNotDistinct`、`BatchRequestTooLong`などのHTTP 400です。

全体の検証後は入力順に単体APIを呼び、本文サイズ・遅延・FIFO識別子・ハンドル・可視性期限などの不正をエントリー別に記録して次へ進みます。送信はエントリーごとのImmediateトランザクション、削除・可視性変更はエントリーごとの単一SQL文の自動コミットです。バッチ全体を1トランザクションにはしません。送信中に重複排除キー登録が失敗しても、その送信のメッセージ作成を含めロールバックし、前後の成功分は永続化します。DB障害は`SenderFault=false`、入力エラーは`true`として返します。プロセス停止や応答消失が起きた場合にはコミット済みの一部が残り得ます。

HTTPはバッチ処理中に同一サーバーのMutexを保持し、他のHTTP操作がエントリー間に割り込みません。別プロセス・接続を含むバッチ全体の隔離は保証しません。JSONの`Entries`は配列順、Queryの`<Action>RequestEntry.N`はNの数値順で処理するため、10番目が2番目より先に入ることはありません。エントリーIDは応答との対応付け専用で、メッセージIDやFIFO重複排除キーとは独立です。

JSONでは`Successful`/`Failed`、Queryでは`<Action>ResultEntry`/`BatchResultErrorEntry`を返します。送信成功には本文MD5とMessageId、FIFOにはSequenceNumber、属性指定時には属性MD5を含めます。単体送信とバッチ送信でFIFOの順序・重複排除ロジックを共有します。バッチの合計サイズにも属性を含め、書き込み前に検証します。

仕様参照: [SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)、[DeleteMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html)、[ChangeMessageVisibilityBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibilityBatch.html)。

## DLQと再投入

`redrive_policies`にソース・DLQ・上限受信回数を、`dead_letter_origins`に移動したメッセージの元キューを保存します。既存DBには追加テーブルと索引のみを作成するため、既存キュー・メッセージを保持したまま更新できます。キュー作成とポリシー登録は同一トランザクションで行い、不正設定の場合はキューも残しません。

`receive`は候補選択・回数判定・DLQ移動・可視性更新を単一のImmediateトランザクションで行います。上限に達したメッセージは可視になるまで移動せず、その後のソース受信で移動します。候補はFIFOの先頭制約を通るため、in-flight中の先行メッセージを追い越しません。移動先では新しいsequenceを採番して末尾へ追加し、旧receipt handleを破棄し、受信回数をリセットします。本文・MessageId・グループIDは保持します。Standardの送信時刻は保持し、FIFOの送信時刻は移動時刻にします。

元キューへの再投入は`redrive_dead_letters`で同期実行できます。元キュー情報が一致する可視メッセージだけを、FIFOグループの先頭制約を維持して移動します。再投入は新しいMessageId・sequence・送信時刻・受信回数を使い、元の送信の重複排除キーには抑制されません。DLQへ直接送信されたメッセージや他ソース由来のメッセージは対象外です。ポリシー解除後も元キュー情報を保持するため再投入可能です。

DLQ移動・再投入の途中でDB操作が失敗すると、削除を含む全変更がロールバックされます。FIFOでは各キュー内の順序を維持しますが、失敗メッセージを別キューへ分離した後の業務処理全体の順序は保証しません。これは[AWSのDLQに関する注意](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)と同じ制約です。

HTTPはJSON/Query双方でRedrivePolicyのCreate/Set/GetとListDeadLetterSourceQueuesに対応します。ARNは`arn:aws:sqs:us-east-1:000000000000:<queue-name>`固定です。非同期のStartMessageMoveTask/Cancel/List操作とRedriveAllowPolicyはこの実装の対象外です。

## 配信遅延・保持期限・本文サイズ

`queues`に`delay_ms`、`message_retention_ms`、`maximum_message_size`を保存し、`messages.available_at_ms`に初回配信可能時刻を保存します。可視性タイムアウトとは独立した条件で候補を選択するため、受信後の再試行で初期遅延が再適用されません。既存DBへは列の有無を調べてトランザクション内で列を追加します。旧メッセージの配信可能時刻は0、旧キューは遅延0・保持4日・最大1 MiBになります。

送受信はImmediateトランザクション内で最新設定を読み、`created_at_ms + message_retention_ms <= now_ms`の行を先に削除します。期限切れのFIFO先行メッセージは後続を妨げず、期限切れメッセージをDLQへ送ることもありません。StandardのDLQ移動は元送信時刻を保持するため、DLQ側の保持期限で判定します。FIFO移動は移動時刻から数えます。再投入前にも期限を確認し、有効なメッセージだけ新しい送信時刻と対象キューの遅延で再登録します。FIFOの重複排除ウィンドウはメッセージ保持期限とは独立です。

本文は1バイト以上を必須とし、UTF-8本文と属性名・型名・属性値の合計がキューの設定上限以下か検証します。Binary属性は生バイト数で計算します。重複排除より先に属性とサイズを検証します。HTTP全体の上限は8 MiBとして、1 MiBメッセージのJSON・Queryエンコードの増加を許容します。

複数の配信属性とRedrivePolicyの変更は原子的に処理します。保持期間変更は既存メッセージにも即時適用します。FIFOの遅延変更は未受信メッセージへ遡及し、受信済みメッセージの可視性期限は変更しません。Standardの既存メッセージは変更しません。AWSの非同期設定伝播は再現せず、決定的なローカルテストのため即時反映とします。

## メッセージ属性の永続化・選択・MD5

`messages.message_attributes`に型付きマップをJSON保存します。論理型とカスタム接尾辞を`data_type`、文字列またはバイナリをenumで区別するため、Binaryが文字列へ変わることはありません。HTTPのBase64変換は境界だけで行います。属性名は予約接頭辞・文字種・長さ・ピリオド制約を検証し、最大10個・非空値・型との整合性を強制します。Numberは浮動小数点を使わず38桁精度と範囲を検証・正規化し、指数表記の展開後を含め大きい方のサイズで上限を確認します。

初回受信時刻、保存された重複排除ID、累計受信回数を別列で保持します。初回時刻のCOALESCE更新と回数加算は受信トランザクション内です。既存DBでは属性は空、累計回数は既存の受信回数から初期化し、重複排除IDは残存するキーから復元します。過去の初回受信時刻は推測せず移行後の最初の受信で記録します。

自動DLQ移動は属性・初回時刻・累計回数を引き継ぎます。従来の`receive_count`はキュー内のDLQ判定用としてリセットし、システム属性`ApproximateReceiveCount`は別の累計列から返します。手動再投入は新規ID・送信時刻・初回時刻・回数にし、ユーザー属性を保持します。SequenceNumberはMessageIdに対応する採番値で、再受信や自動移動で変えず、新規IDになる再投入で更新します。

同期ライブラリは全メタデータを返し、HTTP層が`MessageAttributeNames`と`MessageSystemAttributeNames`（旧`AttributeNames`も併用可）で絞ります。指定なしは属性なし、ユーザー属性は完全名・All・ワイルドカードによる前方一致、システム属性は列挙名・Allです。選択は返却データだけに適用し、DBの属性を削除しません。

MD5は属性名昇順に名前・型名・値のUTF-8/生バイトを4バイト長付きbig-endianで連結し、値の前に文字列/Number=1、Binary=2の1バイトを置きます。カスタム型名全体を含め、受信時には選択結果の属性集合で再計算します。FIFOの本文ベース重複排除には属性を使わず、既存LQSの安定ハッシュと重複排除ウィンドウを維持します。

仕様参照: [メッセージメタデータ](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html)、[MessageAttributeValue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_MessageAttributeValue.html)。

## ロングポーリングとin-flight管理

キューの既定待機時間`receive_wait_time_ms`（0〜20,000ms、既定0）と`max_in_flight`（1〜120,000、既定120,000）を永続化します。既存DBには既定値付きの列を追加し、受信済みメッセージの可視性期限や受信回数は保持します。HTTPは`ReceiveMessageWaitTimeSeconds`を秒で扱います。`LqsMaxInFlightMessages`はローカルの上限再現用拡張で、AWSの属性ではありません。

HTTPの受信だけを非同期処理にし、同期ライブラリの`receive`を最大100msごとに再実行します。キュー既定値はリクエスト開始時に読み、`WaitTimeSeconds`が指定されれば0を含め優先します。待機期限はTokioの単調時計で一度だけ設定し、再試行や設定変更で延長しません。受信成功は期限前に返し、利用可能なメッセージがない場合は期限到達後にのみ空を返します。未知のキュー・不正入力・DB障害は待機せずエラーを返します。

各再試行ではMutexとSQLiteトランザクションを解放してから`sleep_until`します。通知の取りこぼしを避けるため、ローカル向けの単純な定期ポーリングを採用します。新規送信・削除・バッチ操作だけでなく、遅延や可視性期限・保持期限の終了、別プロセスの更新にも追従します。HTTP受信futureを破棄すると次のポーリングは行われず、バックグラウンド受信タスクは残しません。待機数に応じた定期クエリの負荷は発生し、高負荷用途のイベント駆動最適化は対象外です。

in-flightは「保持期限内かつ`invisible_until_ms > now_ms`」の実件数です。独立したカウンターは持たず、`messages(queue_name, invisible_until_ms)`索引を使って計測します。`receive`のImmediateトランザクション内で期限切れ削除、件数確認、残り枠まで（かつ最大10件）の配信を行うため、別接続から競合しても上限を超えて受信できません。`ChangeMessageVisibility`は現在in-flightのメッセージだけを更新し、期限切れハンドルの復活による上限回避を防ぎます。

上限到達時のStandardの同期/短ポーリングは`OverLimit`、FIFOの同期/短ポーリングは空結果です。HTTPのロングポーリングでは両種とも上限を理由に早期終了せず、枠が空くか期限になるまで再試行します。削除、可視性終了、保持期限で枠が戻り、上限引き下げは既存のin-flightメッセージを取り消しません。設定変更後の再試行は最新上限を読みます。`ApproximateNumberOfMessagesNotVisible`はローカルで計測した実件数を返します。

仕様参照: [ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)、[可視性期限と上限](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)、[FIFO上限](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-fifo.html)。

## 時刻とテスト容易性

同期ライブラリAPIはホストの時計を直接読まず、呼び出し側から `now_ms` を渡します。これにより、可視性タイムアウトと重複排除の境界を sleep なしで決定的にテストできます。永続DBを利用するHTTP層は再起動をまたいで比較できるUnix時刻をメッセージ期限に使い、リクエスト待機時間だけは単調時計で管理します。

## 境界と将来の拡張

- SQLiteファイルへの永続化、DLQ転送、最大20秒のロングポーリングに対応。
- APIエラーは `LqsError` として返し、設定または送信・受信の誤りを明示する。
- FIFO のスループット分割は `MessageGroupId` が単位。独立した処理を並列化したい場合はグループを分ける。
