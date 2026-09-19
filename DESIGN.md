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

## 時刻とテスト容易性

API はホストの時計を直接読まず、呼び出し側から単調増加の `now_ms` を渡します。これにより、可視性タイムアウトと重複排除の境界を sleep なしで決定的にテストできます。実運用用のアダプターでは `Instant` などの単調時計をミリ秒へ変換して渡します。

## 境界と将来の拡張

- SQLiteファイルへの永続化は行う。ロングポーリングとDLQは未実装。
- APIエラーは `LqsError` として返し、設定または送信・受信の誤りを明示する。
- FIFO のスループット分割は `MessageGroupId` が単位。独立した処理を並列化したい場合はグループを分ける。
