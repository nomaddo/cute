package main

// This command estimates how much "early advantage" (first crossing a score threshold)
// increases the chance to win, while also accounting for player rating differences.
//
// It does this with logistic regression, a simple model for win/lose outcomes:
//   - Output is a probability between 0 and 1.
//   - Coefficients tell how each factor shifts the odds of winning.
//
// Key idea: we want to separate
//   (1) strength difference (rating_diff)
//   (2) early advantage (first_crossed)
//   (3) whether rating changes the "convert advantage into wins" effect
//
// The model uses these features:
//   intercept         : baseline win tendency
//   rating_diff_scaled: (player_rating - opponent_rating) / ratingScale
//   first_crossed     : 1 if this player first reached the eval threshold
//   rating_x_first    : rating_scaled * first_crossed (interaction term)
//   sente_flag        : 1 if player is sente, 0 if gote
//
// If rating_x_first is positive, higher-rated players convert early advantage more reliably.
// If rating_x_first is near 0, that "conversion power" does not depend on rating.

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	cute "cute/pkg/cute"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type sample struct {
	x []float64
	y float64
}

type counts struct {
	total   int
	skipped int
}

func main() {
	input := flag.String("input", "output.parquet", "input parquet file")
	threshold := flag.Int("threshold", 300, "eval threshold for first crossing")
	iter := flag.Int("iter", 300, "gradient descent iterations")
	lr := flag.Float64("lr", 0.05, "learning rate")
	ratingScale := flag.Float64("rating-scale", 100, "scale factor for rating diff")
	maxAbsDiff := flag.Int("max-abs-diff", 0, "max absolute rating diff (0=disabled)")
	parallel := flag.Int64("parallel", 4, "parquet read parallelism")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "number of gradient workers")
	ratingsArg := flag.String("ratings", "300,600,900,1200,1500,1800,2100,2400", "comma-separated rating values")
	flag.Parse()

	// Basic validation to avoid invalid model settings.

	if *iter <= 0 {
		fatal(fmt.Errorf("iter must be > 0"))
	}
	if *lr <= 0 {
		fatal(fmt.Errorf("lr must be > 0"))
	}
	if *ratingScale <= 0 {
		fatal(fmt.Errorf("rating-scale must be > 0"))
	}
	if *threshold <= 0 {
		fatal(fmt.Errorf("threshold must be > 0"))
	}
	if *workers <= 0 {
		fatal(fmt.Errorf("workers must be > 0"))
	}
	ratings, err := parseIntList(*ratingsArg)
	if err != nil {
		fatal(err)
	}
	records, err := readParquet(*input, *parallel)
	if err != nil {
		fatal(err)
	}

	// Convert game records into supervised learning samples.
	// Each game yields two samples: one from sente's perspective and one from gote's.
	// This keeps the model symmetric and focuses on "player vs opponent" features.
	samples, cts := buildSamples(records, *threshold, *ratingScale, *maxAbsDiff)
	if len(samples) == 0 {
		fatal(fmt.Errorf("no samples available after filtering (total=%d skipped=%d)", cts.total, cts.skipped))
	}

	// Fit logistic regression to model win probability given rating and advantage features.
	// We use batch gradient descent (simple, reliable for a small number of features).
	weights := fitLogReg(samples, *iter, *lr, *workers)

	fmt.Println("data:")
	fmt.Printf("  input: %s\n", *input)
	fmt.Printf("  threshold: %d\n", *threshold)
	fmt.Printf("  rating-scale: %.0f\n", *ratingScale)
	fmt.Printf("  samples: %d (skipped=%d)\n", len(samples), cts.skipped)
	fmt.Printf("  max-abs-diff: %d\n", *maxAbsDiff)
	fmt.Printf("  workers: %d\n", *workers)
	fmt.Println("model:")
	fmt.Println("  features: intercept, rating_diff_scaled, first_crossed, rating_x_first, sente_flag")
	printCoefficients(weights)
	printOddsRatios(weights)
	printPredictedRates(weights)
	printRatingFirstCross(weights, *ratingScale, ratings)
}

func buildSamples(records []cute.GameRecord, threshold int, ratingScale float64, maxAbsDiff int) ([]sample, counts) {
	var samples []sample
	cts := counts{total: len(records)}
	for _, record := range records {
		crossingSide := firstCrossingSide(record.MoveEvals, threshold)
		resultSide := winnerSide(record.Result)
		// Skip games that do not have a clear threshold crossing or winner.
		if crossingSide == "none" || resultSide == "none" {
			cts.skipped++
			continue
		}
		ratingDiff := int(record.SenteRating - record.GoteRating)
		// Optional filter: remove games with too large rating gaps.
		if maxAbsDiff > 0 && absInt(ratingDiff) > maxAbsDiff {
			cts.skipped++
			continue
		}

		// Add one sample per player perspective to avoid locking to a side.
		// (sente's sample)
		samples = append(samples, makeSample(
			float64(record.SenteRating),
			float64(record.GoteRating),
			crossingSide == "sente",
			resultSide == "sente",
			true,
			ratingScale,
		))
		// (gote's sample)
		samples = append(samples, makeSample(
			float64(record.GoteRating),
			float64(record.SenteRating),
			crossingSide == "gote",
			resultSide == "gote",
			false,
			ratingScale,
		))
	}
	return samples, cts
}

func makeSample(playerRating, opponentRating float64, playerFirstCross bool, playerWin bool, isSente bool, ratingScale float64) sample {
	first := 0.0
	if playerFirstCross {
		first = 1.0
	}
	sente := 0.0
	if isSente {
		sente = 1.0
	}
	label := 0.0
	if playerWin {
		label = 1.0
	}
	// ratingDiff says "how much stronger is the player than the opponent".
	// Scaling keeps numbers small and makes training stable.
	ratingDiff := (playerRating - opponentRating) / ratingScale
	// ratingScaled is the player's rating on the same scale.
	ratingScaled := playerRating / ratingScale
	// Interaction term: only active when the player first crossed the threshold.
	// If its coefficient is positive, higher rating means better conversion of advantage.
	ratingFirst := ratingScaled * first
	return sample{
		x: []float64{1.0, ratingDiff, first, ratingFirst, sente},
		y: label,
	}
}

func fitLogReg(samples []sample, iter int, lr float64, workers int) []float64 {
	// Initialize weights to zero. This corresponds to 50% predicted win rate.
	weights := make([]float64, len(samples[0].x))
	if workers > len(samples) {
		workers = len(samples)
	}
	// Model:
	//   p = sigmoid(w · x) = 1 / (1 + exp(-w · x))
	// Loss (average negative log-likelihood):
	//   L = (1/N) * sum_i [ -y_i * log(p_i) - (1 - y_i) * log(1 - p_i) ]
	// Gradient:
	//   dL/dw = (1/N) * sum_i (p_i - y_i) * x_i
	// We update w by gradient descent: w = w - lr * dL/dw
	// Symbols:
	//   x   : feature vector for one sample (intercept, rating diff, etc.)
	//   w   : model weights (one weight per feature)
	//   p   : predicted win probability for a sample
	//   y   : true label (win=1, lose=0)
	//   N   : number of samples
	for i := 0; i < iter; i++ {
		grad := make([]float64, len(weights))
		if workers <= 1 {
			for _, s := range samples {
				p := sigmoid(dot(weights, s.x))
				err := p - s.y
				for j := range grad {
					grad[j] += err * s.x[j]
				}
			}
		} else {
			partials := make([][]float64, workers)
			for w := 0; w < workers; w++ {
				partials[w] = make([]float64, len(weights))
			}
			var wg sync.WaitGroup
			chunk := (len(samples) + workers - 1) / workers
			for w := 0; w < workers; w++ {
				start := w * chunk
				end := start + chunk
				if start >= len(samples) {
					break
				}
				if end > len(samples) {
					end = len(samples)
				}
				wg.Add(1)
				go func(idx, from, to int) {
					defer wg.Done()
					localGrad := partials[idx]
					for _, s := range samples[from:to] {
						p := sigmoid(dot(weights, s.x))
						err := p - s.y
						for j := range localGrad {
							localGrad[j] += err * s.x[j]
						}
					}
				}(w, start, end)
			}
			wg.Wait()
			for w := 0; w < workers; w++ {
				localGrad := partials[w]
				for j := range grad {
					grad[j] += localGrad[j]
				}
			}
		}
		// Average gradient and update weights.
		scale := lr / float64(len(samples))
		for j := range weights {
			weights[j] -= grad[j] * scale
		}
	}
	return weights
}

func printCoefficients(weights []float64) {
	labels := []string{"intercept", "rating_diff_scaled", "first_crossed", "rating_x_first", "sente_flag"}
	fmt.Println("coefficients (log-odds):")
	// Coefficients are in log-odds units; positive values increase win probability.
	for i, w := range weights {
		fmt.Printf("  %s = %.6f\n", labels[i], w)
	}
}

func printOddsRatios(weights []float64) {
	labels := []string{"rating_diff_scaled", "first_crossed", "rating_x_first", "sente_flag"}
	fmt.Println("odds ratios (1.0 = no change):")
	// Odds ratios are easier to read: 1.0 means no change, 1.5 means 50% higher odds.
	for i := 1; i < len(weights); i++ {
		fmt.Printf("  %s = %.4f\n", labels[i-1], math.Exp(weights[i]))
	}
}

func printPredictedRates(weights []float64) {
	// These predictions use ratingDiff=0 to show the pure effect of first_crossed/sente.
	fmt.Println("predicted win rates (rating diff = 0):")
	fmt.Printf("  sente, first-cross=1: %.3f\n", predict(weights, 0, 1, 1, 0))
	fmt.Printf("  sente, first-cross=0: %.3f\n", predict(weights, 0, 0, 1, 0))
	fmt.Printf("  gote,  first-cross=1: %.3f\n", predict(weights, 0, 1, 0, 0))
	fmt.Printf("  gote,  first-cross=0: %.3f\n", predict(weights, 0, 0, 0, 0))
}

func printRatingFirstCross(weights []float64, ratingScale float64, ratings []int) {
	if len(ratings) == 0 {
		return
	}
	fmt.Println("expected win rates by rating (first-cross=1, rating diff = 0):")
	for _, rating := range ratings {
		ratingScaled := float64(rating) / ratingScale
		sente := predict(weights, 0, 1, 1, ratingScaled)
		gote := predict(weights, 0, 1, 0, ratingScaled)
		fmt.Printf("  rating=%d: sente=%.3f gote=%.3f\n", rating, sente, gote)
	}
}

func predict(weights []float64, ratingDiff float64, firstCross float64, sente float64, ratingScaled float64) float64 {
	// ratingScaled should match the same scale as ratingScale. It affects only the interaction.
	x := []float64{1.0, ratingDiff, firstCross, ratingScaled * firstCross, sente}
	return sigmoid(dot(weights, x))
}

func sigmoid(z float64) float64 {
	if z >= 0 {
		return 1 / (1 + math.Exp(-z))
	}
	ez := math.Exp(z)
	return ez / (1 + ez)
}

func dot(a []float64, b []float64) float64 {
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func firstCrossingSide(evals []cute.MoveEval, threshold int) string {
	for _, eval := range evals {
		if eval.ScoreType == "mate" {
			if eval.ScoreValue >= 0 {
				return "sente"
			}
			return "gote"
		}
		if eval.ScoreValue >= int32(threshold) {
			return "sente"
		}
		if eval.ScoreValue <= -int32(threshold) {
			return "gote"
		}
	}
	return "none"
}

func winnerSide(result string) string {
	switch result {
	case "sente_win":
		return "sente"
	case "gote_win":
		return "gote"
	default:
		return "none"
	}
}

func readParquet(path string, parallel int64) ([]cute.GameRecord, error) {
	absPath := path
	if !filepath.IsAbs(path) {
		if resolved, err := filepath.Abs(path); err == nil {
			absPath = resolved
		}
	}
	fileReader, err := local.NewLocalFileReader(absPath)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	parquetReader, err := reader.NewParquetReader(fileReader, new(cute.GameRecord), parallel)
	if err != nil {
		return nil, err
	}
	defer parquetReader.ReadStop()

	num := int(parquetReader.GetNumRows())
	records := make([]cute.GameRecord, 0, num)
	batchSize := 1024
	for offset := 0; offset < num; offset += batchSize {
		remain := num - offset
		if remain < batchSize {
			batchSize = remain
		}
		batch := make([]cute.GameRecord, batchSize)
		if err := parquetReader.Read(&batch); err != nil {
			return nil, err
		}
		records = append(records, batch...)
	}
	return records, nil
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func parseIntList(raw string) ([]int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		value, err := strconv.Atoi(segment)
		if err != nil {
			return nil, fmt.Errorf("invalid ratings entry: %s", segment)
		}
		values = append(values, value)
	}
	return values, nil
}
