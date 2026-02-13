# Result

## 実行コマンド

```
/workspaces/cute $ go run ./cmd/logreg -input out/games_22552_eval.parquet -threshold 300 -max-abs-diff 200 -workers 1
```

## 出力

```
data:
  input: out/games_22552_eval.parquet
  threshold: 300
  rating-scale: 100
  samples: 40226 (skipped=2439)
  max-abs-diff: 200
  workers: 1
model:
  features: intercept, rating_diff_scaled, first_crossed, rating_x_first, sente_flag
coefficients (log-odds):
  intercept = -0.230760
  rating_diff_scaled = 0.251512
  first_crossed = 0.147562
  rating_x_first = 0.036381
  sente_flag = -0.018096
odds ratios (1.0 = no change):
  rating_diff_scaled = 1.2860
  first_crossed = 1.1590
  rating_x_first = 1.0371
  sente_flag = 0.9821
predicted win rates (rating diff = 0):
  sente, first-cross=1: 0.475
  sente, first-cross=0: 0.438
  gote,  first-cross=1: 0.479
  gote,  first-cross=0: 0.443
expected win rates by rating (first-cross=1, rating diff = 0):
  rating=300: sente=0.502 gote=0.506
  rating=600: sente=0.529 gote=0.534
  rating=900: sente=0.556 gote=0.561
  rating=1200: sente=0.583 gote=0.587
  rating=1500: sente=0.609 gote=0.614
  rating=1800: sente=0.635 gote=0.639
  rating=2100: sente=0.660 gote=0.664
  rating=2400: sente=0.684 gote=0.688
```

## 解釈

- 先に300点を取った側は勝ちやすく、`first_crossed` のオッズ比は約 1.16。
- レート差100ごとの勝ちオッズは約 1.29 倍で、実力差の影響が強い。
- `rating_x_first` が正なので、レートが高いほど「有利局面を勝ち切る力」が少し強い傾向。
- 先手/後手の差 (`sente_flag`) は小さく、影響は軽微。
- レートが高いほど、先に300点を取ったときの期待勝率が上がる傾向が見える。

---

## 実行コマンド (threshold=500)

```
/workspaces/cute $ go run ./cmd/logreg -input out/games_22552_eval.parquet -threshold 500 -max-abs-diff 200 -workers 1
```

## 出力

```
data:
  input: out/games_22552_eval.parquet
  threshold: 500
  rating-scale: 100
  samples: 40018 (skipped=2543)
  max-abs-diff: 200
  workers: 1
model:
  features: intercept, rating_diff_scaled, first_crossed, rating_x_first, sente_flag
coefficients (log-odds):
  intercept = -0.365265
  rating_diff_scaled = 0.231001
  first_crossed = 0.244893
  rating_x_first = 0.060172
  sente_flag = -0.056112
odds ratios (1.0 = no change):
  rating_diff_scaled = 1.2599
  first_crossed = 1.2775
  rating_x_first = 1.0620
  sente_flag = 0.9454
predicted win rates (rating diff = 0):
  sente, first-cross=1: 0.456
  sente, first-cross=0: 0.396
  gote,  first-cross=1: 0.470
  gote,  first-cross=0: 0.410
expected win rates by rating (first-cross=1, rating diff = 0):
  rating=300: sente=0.501 gote=0.515
  rating=600: sente=0.546 gote=0.560
  rating=900: sente=0.590 gote=0.604
  rating=1200: sente=0.633 gote=0.646
  rating=1500: sente=0.674 gote=0.686
  rating=1800: sente=0.712 gote=0.724
  rating=2100: sente=0.748 gote=0.758
  rating=2400: sente=0.780 gote=0.790
```

## 解釈

- 閾値500でも、先に有利を取った側が勝ちやすい (`first_crossed` のオッズ比は約 1.28)。
- `rating_x_first` が 1.062 なので、レートが高いほど「有利局面を勝ち切る力」が300点より強くなる傾向。
- レート差100ごとの勝ちオッズは約 1.26 倍で、実力差の影響は継続して大きい。
- 先手/後手の差 (`sente_flag`) は小さく、影響は軽微。
- レートが高いほど、先に500点を取ったときの期待勝率が上がる傾向が見える。

---

## 実行コマンド (threshold=1000)

```
/workspaces/cute $ go run ./cmd/logreg/ -input out/games_22552_eval.parquet -threshold 1000 -max-abs-diff 200 -workers 1
```

## 出力

```
data:
  input: out/games_22552_eval.parquet
  threshold: 1000
  rating-scale: 100
  samples: 39332 (skipped=2886)
  max-abs-diff: 200
  workers: 1
model:
  features: intercept, rating_diff_scaled, first_crossed, rating_x_first, sente_flag
coefficients (log-odds):
  intercept = -0.582765
  rating_diff_scaled = 0.203337
  first_crossed = 0.381982
  rating_x_first = 0.105178
  sente_flag = -0.121358
odds ratios (1.0 = no change):
  rating_diff_scaled = 1.2255
  first_crossed = 1.4652
  rating_x_first = 1.1109
  sente_flag = 0.8857
predicted win rates (rating diff = 0):
  sente, first-cross=1: 0.420
  sente, first-cross=0: 0.331
  gote,  first-cross=1: 0.450
  gote,  first-cross=0: 0.358
expected win rates by rating (first-cross=1, rating diff = 0):
  rating=300: sente=0.498 gote=0.529
  rating=600: sente=0.577 gote=0.606
  rating=900: sente=0.651 gote=0.678
  rating=1200: sente=0.719 gote=0.743
  rating=1500: sente=0.778 gote=0.798
  rating=1800: sente=0.828 gote=0.845
  rating=2100: sente=0.868 gote=0.882
  rating=2400: sente=0.900 gote=0.911
```

## 解釈

- 閾値1000でも、先に有利を取った側が勝ちやすい (`first_crossed` のオッズ比は約 1.47)。
- `rating_x_first` が 1.111 なので、レートが高いほど「有利局面を勝ち切る力」がさらに強くなる傾向。
- レート差100ごとの勝ちオッズは約 1.23 倍で、実力差の影響は継続して大きい。
- 先手/後手の差 (`sente_flag`) は小さく、影響は軽微。
- レートが高いほど、先に1000点を取ったときの期待勝率が上がる傾向が見える。

---

## 実行コマンド (analyze)

```
/workspaces/cute $ go run ./cmd/analyze -input out/games_22552_eval.parquet -thresholds 300,500,1000
```

## 出力

```
read: 22552/22552 (100%)
threshold=300
player_rate,total_games,crossings,wins,win_rate
0-100,13,6,3,0.500000
100-200,391,170,105,0.617647
200-300,1346,617,356,0.576985
300-400,1705,787,456,0.579416
400-500,1950,929,534,0.574812
500-600,1831,846,483,0.570922
600-700,2026,943,541,0.573701
700-800,1938,904,545,0.602876
800-900,1904,901,535,0.593785
900-1000,1660,791,473,0.597977
1000-1100,1398,650,385,0.592308
1100-1200,1469,711,415,0.583685
1200-1300,1364,635,377,0.593701
1300-1400,1313,630,367,0.582540
1400-1500,1062,514,303,0.589494
1500-1600,879,412,251,0.609223
1600-1700,916,443,272,0.613995
1700-1800,783,368,227,0.616848
1800-1900,695,325,193,0.593846
1900-2000,513,254,168,0.661417
2000-2100,457,224,143,0.638393
2100-2200,278,133,79,0.593985
2200-2300,242,110,65,0.590909
2300-2400,212,101,60,0.594059
2400-2500,118,57,46,0.807018
2500-2600,88,39,26,0.666667
2600-2700,48,28,19,0.678571
2700-2800,38,18,11,0.611111
2800-2900,25,13,12,0.923077
2900-3000,2,1,0,0.000000
3000-3100,1,1,1,1.000000
3100-3200,1,0,0,0.000000

threshold=500
player_rate,total_games,crossings,wins,win_rate
0-100,13,5,4,0.800000
100-200,391,177,117,0.661017
200-300,1346,604,384,0.635762
300-400,1705,780,494,0.633333
400-500,1950,921,589,0.639522
500-600,1831,852,533,0.625587
600-700,2026,944,590,0.625000
700-800,1938,900,569,0.632222
800-900,1904,884,560,0.633484
900-1000,1660,784,521,0.664541
1000-1100,1398,663,421,0.634992
1100-1200,1469,701,452,0.644793
1200-1300,1364,638,391,0.612853
1300-1400,1313,622,408,0.655949
1400-1500,1062,504,345,0.684524
1500-1600,879,419,283,0.675418
1600-1700,916,435,277,0.636782
1700-1800,783,372,240,0.645161
1800-1900,695,318,218,0.685535
1900-2000,513,252,176,0.698413
2000-2100,457,225,147,0.653333
2100-2200,278,130,89,0.684615
2200-2300,242,108,70,0.648148
2300-2400,212,104,70,0.673077
2400-2500,118,55,45,0.818182
2500-2600,88,39,30,0.769231
2600-2700,48,28,19,0.678571
2700-2800,38,18,15,0.833333
2800-2900,25,13,11,0.846154
2900-3000,2,1,1,1.000000
3000-3100,1,1,1,1.000000
3100-3200,1,0,0,0.000000

threshold=1000
player_rate,total_games,crossings,wins,win_rate
0-100,13,5,5,1.000000
100-200,391,175,127,0.725714
200-300,1346,595,421,0.707563
300-400,1705,771,557,0.722438
400-500,1950,906,671,0.740618
500-600,1831,829,580,0.699638
600-700,2026,930,667,0.717204
700-800,1938,875,635,0.725714
800-900,1904,879,620,0.705347
900-1000,1660,766,553,0.721932
1000-1100,1398,660,488,0.739394
1100-1200,1469,698,526,0.753582
1200-1300,1364,628,440,0.700637
1300-1400,1313,612,455,0.743464
1400-1500,1062,495,387,0.781818
1500-1600,879,401,315,0.785536
1600-1700,916,430,329,0.765116
1700-1800,783,355,260,0.732394
1800-1900,695,320,259,0.809375
1900-2000,513,249,193,0.775100
2000-2100,457,216,158,0.731481
2100-2200,278,126,94,0.746032
2200-2300,242,114,86,0.754386
2300-2400,212,95,74,0.778947
2400-2500,118,57,50,0.877193
2500-2600,88,38,35,0.921053
2600-2700,48,26,19,0.730769
2700-2800,38,15,14,0.933333
2800-2900,25,15,13,0.866667
2900-3000,2,1,1,1.000000
3000-3100,1,1,1,1.000000
3100-3200,1,0,0,0.000000
```

---

## 実行コマンド (stats)

```
/workspaces/cute $ go run ./cmd/stats/ -parquet out/games_22552_eval.parquet
```

## 出力

```
input parquet: out/games_22552_eval.parquet
failed files: 0
unique users: 8908
ratings: known=8904 unknown=0 (users without rating=4)
users with >= 2 games: 6307
rating range: 10-3093
rating distribution (bin size=100):
0-99,29
100-199,145
200-299,459
300-399,536
400-499,572
500-599,501
600-699,562
700-799,576
800-899,580
900-999,472
1000-1099,393
1100-1199,439
1200-1299,457
1300-1399,434
1400-1499,361
1500-1599,318
1600-1699,316
1700-1799,273
1800-1899,267
1900-1999,249
2000-2099,215
2100-2199,168
2200-2299,151
2300-2399,124
2400-2499,87
2500-2599,78
2600-2699,57
2700-2799,47
2800-2899,24
2900-2999,11
3000-3099,3
```

## 解釈

- ユニークユーザーは 8908 人で、そのうち 2局以上のユーザーが 6307 人。
- レート分布は 300〜900 帯に多く、1000以上は徐々に減る。
- 最高レート帯(3000以上)は少数で、サンプルは薄い。
