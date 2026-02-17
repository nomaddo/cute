package main

// This command estimates how much "early advantage" (first crossing a score threshold)
// increases the chance to win, while also accounting for player rating differences.
//
// It uses logistic regression with one sample per game from sente's perspective.
// The output is a probability between 0 and 1, and coefficients tell how each
// factor shifts the odds of sente winning.
//
// Key ideas:
//   (1) strength difference (rating_diff)
//   (2) early advantage (first_crossed)
//   (3) whether absolute skill changes the "convert advantage into wins" effect
//
// Features:
//   intercept         : baseline sente win tendency (at mean rating, no first-cross)
//   rating_diff_scaled: (sente_rating - gote_rating) / ratingScale
//   first_crossed     : 1 if sente first reached the eval threshold, 0 if gote did
//   rating_x_first    : centered_rating * first_crossed (interaction term)
//                        where centered_rating = (sente_rating - mean_rating) / ratingScale
//
// Centering the rating makes the first_crossed coefficient represent the
// effect at the mean rating of the dataset, not at an arbitrary rating = 0.
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

	// Build one sample per game (sente perspective) and fit a single model.
	// We use batch gradient descent (simple, reliable for a small number of features).
	samples, cts, meanRating := buildSamples(records, *threshold, *ratingScale, *maxAbsDiff)
	if len(samples) == 0 {
		fatal(fmt.Errorf("no samples available after filtering (total=%d skipped=%d)", cts.total, cts.skipped))
	}
	weights, loss := fitLogReg(samples, *iter, *lr, *workers)

	fmt.Println("data:")
	fmt.Printf("  input: %s\n", *input)
	fmt.Printf("  threshold: %d\n", *threshold)
	fmt.Printf("  rating-scale: %.0f\n", *ratingScale)
	fmt.Printf("  games: %d (skipped=%d)\n", len(samples), cts.skipped)
	fmt.Printf("  max-abs-diff: %d\n", *maxAbsDiff)
	fmt.Printf("  mean-sente-rating: %.0f\n", meanRating)
	fmt.Printf("  workers: %d\n", *workers)
	fmt.Println("model:")
	fmt.Println("  features: intercept, rating_diff_scaled, first_crossed, rating_x_first")
	fmt.Printf("  final-loss: %.6f\n", loss)

	printSection("all", weights, *ratingScale, meanRating, ratings)
}

func buildSamples(records []cute.GameRecord, threshold int, ratingScale float64, maxAbsDiff int) ([]sample, counts, float64) {
	// First pass: filter games and compute mean sente rating for centering.
	type accepted struct {
		senteRating     float64
		goteRating      float64
		senteFirstCross bool
		senteWin        bool
	}
	var games []accepted
	cts := counts{total: len(records)}
	var sumRating float64
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
		games = append(games, accepted{
			senteRating:     float64(record.SenteRating),
			goteRating:      float64(record.GoteRating),
			senteFirstCross: crossingSide == "sente",
			senteWin:        resultSide == "sente",
		})
		sumRating += float64(record.SenteRating)
	}
	meanRating := 0.0
	if len(games) > 0 {
		meanRating = sumRating / float64(len(games))
	}
	// Second pass: build one sample per game (sente perspective) with centered rating.
	samples := make([]sample, 0, len(games))
	for _, g := range games {
		samples = append(samples, makeSample(g.senteRating, g.goteRating, g.senteFirstCross, g.senteWin, ratingScale, meanRating))
	}
	return samples, cts, meanRating
}

func makeSample(senteRating, goteRating float64, senteFirstCross bool, senteWin bool, ratingScale float64, meanRating float64) sample {
	first := 0.0
	if senteFirstCross {
		first = 1.0
	}
	label := 0.0
	if senteWin {
		label = 1.0
	}
	// ratingDiff: how much stronger sente is than gote.
	ratingDiff := (senteRating - goteRating) / ratingScale
	// Centered rating: sente's rating relative to the dataset mean.
	// Centering makes the intercept and first_crossed coefficients
	// represent effects at the mean rating, not at rating=0.
	ratingCentered := (senteRating - meanRating) / ratingScale
	// Interaction term: only active when sente first crossed the threshold.
	// If its coefficient is positive, higher rating means better conversion of advantage.
	ratingFirst := ratingCentered * first
	return sample{
		x: []float64{1.0, ratingDiff, first, ratingFirst},
		y: label,
	}
}

func fitLogReg(samples []sample, iter int, lr float64, workers int) ([]float64, float64) {
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
	// Compute final loss (negative log-likelihood) for convergence check.
	var totalLoss float64
	for _, s := range samples {
		p := sigmoid(dot(weights, s.x))
		// Clamp to avoid log(0).
		if p < 1e-15 {
			p = 1e-15
		}
		if p > 1-1e-15 {
			p = 1 - 1e-15
		}
		totalLoss += -s.y*math.Log(p) - (1-s.y)*math.Log(1-p)
	}
	finalLoss := totalLoss / float64(len(samples))
	return weights, finalLoss
}

func printCoefficients(weights []float64) {
	labels := []string{"intercept", "rating_diff_scaled", "first_crossed", "rating_x_first"}
	fmt.Println("coefficients (log-odds):")
	// Coefficients are in log-odds units; positive values increase win probability.
	for i, w := range weights {
		fmt.Printf("  %s = %.6f\n", labels[i], w)
	}
}

func printOddsRatios(weights []float64) {
	labels := []string{"rating_diff_scaled", "first_crossed", "rating_x_first"}
	fmt.Println("odds ratios (1.0 = no change):")
	// Odds ratios are easier to read: 1.0 means no change, 1.5 means 50% higher odds.
	for i := 1; i < len(weights); i++ {
		fmt.Printf("  %s = %.4f\n", labels[i-1], math.Exp(weights[i]))
	}
}

func printPredictedRates(weights []float64) {
	// Predictions at ratingDiff=0, ratingCentered=0 (mean-rated player).
	fmt.Println("predicted win rates (rating diff = 0, at mean rating):")
	fmt.Printf("  first-cross=1: %.3f\n", predict(weights, 0, 1, 0))
	fmt.Printf("  first-cross=0: %.3f\n", predict(weights, 0, 0, 0))
}

func printRatingFirstCross(weights []float64, ratingScale float64, meanRating float64, ratings []int) {
	if len(ratings) == 0 {
		return
	}
	fmt.Println("expected win rates by rating (first-cross=1, rating diff = 0):")
	for _, rating := range ratings {
		ratingCentered := (float64(rating) - meanRating) / ratingScale
		winRate := predict(weights, 0, 1, ratingCentered)
		fmt.Printf("  rating=%d: win_rate=%.3f\n", rating, winRate)
	}
}

func predict(weights []float64, ratingDiff float64, firstCross float64, ratingCentered float64) float64 {
	// ratingCentered is (playerRating - meanRating) / ratingScale; affects only the interaction.
	x := []float64{1.0, ratingDiff, firstCross, ratingCentered * firstCross}
	return sigmoid(dot(weights, x))
}

func printSection(label string, weights []float64, ratingScale float64, meanRating float64, ratings []int) {
	fmt.Printf("%s model:\n", label)
	printCoefficients(weights)
	printOddsRatios(weights)
	printPredictedRates(weights)
	printRatingFirstCross(weights, ratingScale, meanRating, ratings)
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
