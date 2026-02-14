PARQUET ?= out/games_34105_eval.parquet
RESULTS_DIR ?= out/results
LOGREG_MAX_ABS_DIFF ?= 200
LOGREG_WORKERS ?= 1

.PHONY: results results-logreg results-analyze results-stats

results: results-logreg results-analyze results-stats

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
	go run ./cmd/stats -parquet $(PARQUET) > $(RESULTS_DIR)/stats.txt
