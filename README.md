# Cute

KIFを解析するためのgolangプロジェクト。


## Goals

- 評価値の関係を明らかにする。
  先に将棋AIで300点を取ったほうの勝率を24レートごとに算出する


## 仕組み

- config.json には将棋AIのパスなど、実行に必要な設定を書く
- test_kif/ には10個程度のテスト用のKIFファイルを置く

## KIF Format

https://kakinoki.o.oo7.jp/kif_format.html

## Parquest_schema.json

- result_code:
  - 0: sente win
  - 1: gote win
  - 2: draw
- win_reason_code
  - 0: mate
  - 1: time up
  - 2: illegal move
  - 3: resign
  - 4: draw
