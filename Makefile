PARQUET ?= out/checkpoint_66294.parquet
OPENING_DB ?= out/6_senkei.parquet
RESULTS_DIR ?= out/results
LOGREG_MAX_ABS_DIFF ?= 200
LOGREG_WORKERS ?= 1
KIFU_DIR ?= test_dir

.PHONY: results results-logreg results-analyze results-stats

results: results-logreg results-analyze results-stats

graph:
	go run ./cmd/graph -input $(KIFU_DIR) --resume --process-num 20 --output output.parquet

results-logreg:
	@mkdir -p $(RESULTS_DIR)
	go run ./cmd/logreg -input $(PARQUET) -threshold 300 -max-abs-diff $(LOGREG_MAX_ABS_DIFF) -workers $(LOGREG_WORKERS) > $(RESULTS_DIR)/logreg_threshold_300.txt
	go run ./cmd/logreg -input $(PARQUET) -threshold 500 -max-abs-diff $(LOGREG_MAX_ABS_DIFF) -workers $(LOGREG_WORKERS) > $(RESULTS_DIR)/logreg_threshold_500.txt
	go run ./cmd/logreg -input $(PARQUET) -threshold 1000 -max-abs-diff $(LOGREG_MAX_ABS_DIFF) -workers $(LOGREG_WORKERS) > $(RESULTS_DIR)/logreg_threshold_1000.txt

results-analyze:
	@mkdir -p $(RESULTS_DIR)
	go run ./cmd/analyze -input $(PARQUET) -thresholds 300,500,1000 > $(RESULTS_DIR)/analyze_thresholds_300_500_1000.txt

results-stats:
	@mkdir -p $(RESULTS_DIR)
	go run ./cmd/stats -parquet $(PARQUET) -opening-db $(OPENING_DB) -threshold 400  -min-games 20 > $(RESULTS_DIR)/stats.txt

shikenbisya-ibisya:
	go run ./cmd/analyze -input $(PARQUET) -opening-db $(OPENING_DB) -thresholds "200,300,500,1000" -ignore-first-moves 20 \
		-filter '(has(sente.attack, "四間飛車") && has (gote.note, "居飛車")) || (has(gote.attack, "四間飛車") && has(sente.note, "居飛車"))' \
		-crossing-side-filter 'has(note, "居飛車")'

shikenbisya-shikenbisya:
	go run ./cmd/analyze -input $(PARQUET) -opening-db $(OPENING_DB) -thresholds "200,300,500,1000" -ignore-first-moves 20 \
		-filter '(has(sente.attack, "四間飛車") && has (gote.note, "居飛車")) || (has(gote.attack, "四間飛車") && has(sente.note, "居飛車"))' \
		-crossing-side-filter 'has(attack, "四間飛車")'

ibisha:
	go run ./cmd/analyze -input $(PARQUET) -opening-db $(OPENING_DB) -thresholds "200,300,500,1000" -ignore-first-moves 20 \
		-filter 'has(sente.note, "居飛車") && has(gote.note, "居飛車")'

taikoukei-ibisya:
	go run ./cmd/analyze -input $(PARQUET) -opening-db $(OPENING_DB) -thresholds "200,300,500,1000" -ignore-first-moves 20 \
		-filter 'has(sente.note, "対抗形") && has(gote.note, "対抗形")' \
		-crossing-side-filter 'has(note, "居飛車")'

taikoukei-huribisya:
	go run ./cmd/analyze -input $(PARQUET) -opening-db $(OPENING_DB) -thresholds "200,300" -ignore-first-moves 20 \
		-filter 'has(sente.note, "対抗形") && has(gote.note, "対抗形")' \
		-crossing-side-filter 'has(note, "振り飛車")'

