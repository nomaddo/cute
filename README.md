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
- Ruby (戦型分類に使用)

### 1. config.json

`engine` に将棋AIの実行ファイル、`millis` に1手あたりの思考時間(ms)を指定する。

```json
{
  "engine": "/path/to/engine",
  "millis": 1000
}
```

### 2. KIF解析 (parquet生成)

KIF棋譜ファイルを将棋AIで解析し、各局面の評価値を含むparquetファイルを生成する。

```bash
go run ./cmd/graph -config config.json -input test_kif -output output.parquet
```

主なオプション:

- `-process-num` 並列数 (デフォルト: 20)
- `-resume` 既存のparquetから再開

### 3. 戦型分類 (opening DB 生成)

KIF棋譜を戦型別に分類し、parquetファイルに出力する。

```bash
ruby tools/classify_kif_to_db.rb -i test_kif -o out/senkei.parquet
```

### 4. 解析 (CSV出力)

#### すべてを対象にした解析

レート区間ごとに、先に評価値閾値を超えた側の勝率をCSV出力する。

```bash
go run ./cmd/analyze -input output.parquet -thresholds 300,500,1000
```

主なオプション:

- `-rating-diff-max` 先後のレート差の上限 (デフォルト: 100)
- `-player-bin-size` レート区間の幅 (デフォルト: 100)
- `-player-min`/`-player-max` レート範囲を固定 (0は自動)
- `-ignore-first-moves` 初手から無視する手数を指定(0は無効化)

#### 戦型を指定した解析

戦型DB (opening DB) を用いて、特定の戦型の棋譜のみを対象に解析する。
`-filter` で集計する棋譜の条件を指定し、`-crossing-side-filter` で特定の側のみを集計できる。

```bash
go run ./cmd/analyze -input output.parquet -opening-db out/senkei.parquet \
    -thresholds "200,300,500,1000" -ignore-first-moves 20 \
    -filter 'has(sente.note, "対抗形") && has(gote.note, "対抗形")' \
    -crossing-side-filter 'has(note, "振り飛車")'
```

主なオプション:

- `-opening-db` 戦型分類parquetファイル
- `-filter` 集計する棋譜の条件を指定 (expr式)
- `-crossing-side-filter` 集計対象のうち、この条件を満たした側だけを集計する (expr式)

フィルタ式で使える関数:

- `has(tags, "タグ名")` — 指定タグが含まれるか判定

フィルタ式で使えるフィールド:

- `sente.attack`, `sente.defense`, `sente.technique`, `sente.note` (先手の戦型タグ)
- `gote.attack`, `gote.defense`, `gote.technique`, `gote.note` (後手の戦型タグ)
- `-crossing-side-filter` では `attack`, `defense`, `technique`, `note` をプレイヤー単位で参照

出力は標準出力にCSVで表示される。

### 5. ユーザ別統計 (stats)

ユーザごとの作戦勝ち確率・勝率・よく使う作戦を分析する。

```bash
go run ./cmd/stats -parquet output.parquet -opening-db out/senkei.parquet \
    -threshold 500 -min-games 20 -sort crossing_rate
```

主なオプション:

- `-parquet` 評価値parquetファイル (必須)
- `-opening-db` 戦型分類parquetファイル (必須)
- `-threshold` crossing判定の評価値閾値 (デフォルト: 500)
- `-min-games` 最低対局数フィルタ (デフォルト: 20)
- `-ignore-first-moves` 序盤を無視する手数
- `-top-attacks` 表示する上位作戦数 (デフォルト: 3)
- `-sort` ソート列: `crossing_rate`, `win_rate`, `total_games`, `avg_rating`

出力CSV列:

| 列名 | 説明 |
|---|---|
| `name` | ユーザ名 |
| `avg_rating` | 平均レーティング |
| `games` | 全対局数 |
| `overall_win_rate` | 全体勝率 (評価値に依存しない) |
| `eval_games` | crossing解析対象の対局数 |
| `crossings` | 先にthresholdを超えた回数 |
| `crossing_rate` | 作戦勝ち確率 (crossings / eval_games) |
| `wins` | crossing後の勝利数 |
| `win_rate` | crossing後の勝率 |
| `top_attacks` | よく使う作戦 (attack tagの上位N件と回数) |

### 6. ロジスティック回帰分析 (logreg)

レート差と作戦勝ちが勝率に与える影響をロジスティック回帰で推定する。

```bash
go run ./cmd/logreg -input output.parquet -threshold 300
```

主なオプション:

- `-threshold` 評価値閾値 (デフォルト: 300)
- `-iter` 勾配降下の反復回数 (デフォルト: 300)
- `-lr` 学習率 (デフォルト: 0.05)
- `-max-abs-diff` レート差の上限 (0=無制限)
- `-ratings` 推定対象のレート値 (カンマ区切り)

### Makefile ターゲット

よく使うコマンドの組み合わせは `Makefile` にまとめている。

```bash
make graph              # KIF解析 → parquet生成
make results            # 全解析 (logreg + analyze + stats)
make shikenbisya-ibisya # 四間飛車 vs 居飛車 (居飛車側)
make ibisha             # 相居飛車
make taikoukei-ibisya   # 対抗形 (居飛車側)
make taikoukei-huribisya # 対抗形 (振り飛車側)
```

## KIF Format

https://kakinoki.o.oo7.jp/kif_format.html
