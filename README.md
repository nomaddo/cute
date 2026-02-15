# Cute

KIFを解析するためのGoプロジェクト。


## Goals

- 評価値の関係を明らかにする
- 先に将棋AIで300点を取ったほうの勝率を24レート刻みで算出する

実験結果は[このファイルを参照](RESULT.md) 

## 仕組み

- config.json には将棋AIのパスなど、実行に必要な設定を書く
- test_kif/ にはテスト用のKIFファイルを置く

## Usage

以降のコマンドはリポジトリ直下で実行する。

### Requirements

- Go
- Ruby (tools/classify_kif_to_db.rb を使う場合)

devcontainer では Ruby をインストール済み。

### 1. config.json

`engine` に将棋AIの実行ファイル、`millis` に1手あたりの思考時間(ms)を指定する。

```json
{
  "engine": "/path/to/engine",
  "millis": 1000
}
```

### 2. KIF解析 (parquet生成)

```bash
go run ./cmd/cute -config config.json -input test_kif -output output.parquet
```

主なオプション:

- `-process-num` 並列数 (デフォルト: 20)
- `-resume` 既存のparquetから再開

### 3. 解析 (CSV出力)

```bash
go run ./cmd/analyze -input output.parquet -thresholds 300,600,1000
```

主なオプション:

- `-rating-diff-max` 先後のレート差の上限 (デフォルト: 100)
- `-player-bin-size` レート区間の幅 (デフォルト: 100)
- `-player-min`/`-player-max` レート範囲を固定 (0は自動)

出力は標準出力にCSVで表示される。

### 4. stats (ユーザー/レート分布)

KIFディレクトリから集計する場合:

```bash
go run ./cmd/stats -kif-dir test_kif -bin-size 100 -min-games 2
```

parquetから集計する場合:

```bash
go run ./cmd/stats -parquet output.parquet -bin-size 100 -min-games 2
```

主なオプション:

- `-bin-size` レート区間の幅 (デフォルト: 100)
- `-min-games` 1ユーザーに含める最小対局数 (デフォルト: 2)

## KIF Format

https://kakinoki.o.oo7.jp/kif_format.html
