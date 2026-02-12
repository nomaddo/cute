package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	cute "cute/src"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type scenario struct {
	threshold  int
	bucketFrom int
	bucketTo   int
}

type stats struct {
	totalGames    int
	crossings     int
	wins          int
	excludedGames int
}

// main parses CLI flags and prints CSV stats for eval threshold crossings.
func main() {
	inputPath := flag.String("input", "output.parquet", "input parquet file")
	thresholdsArg := flag.String("thresholds", "1000", "comma-separated eval thresholds")
	ratingDiffMax := flag.Int("rating-diff-max", 100, "max rating difference between players")
	binSize := flag.Int("player-bin-size", 100, "player rating bucket size")
	playerMin := flag.Int("player-min", 0, "minimum player rating (0 to auto-detect)")
	playerMax := flag.Int("player-max", 0, "maximum player rating (0 to auto-detect)")
	parallel := flag.Int64("parallel", 4, "parquet read parallelism")
	flag.Parse()

	thresholds, err := parseIntList(*thresholdsArg)
	if err != nil {
		fatal(err)
	}
	if len(thresholds) == 0 {
		fatal(fmt.Errorf("thresholds must be non-empty"))
	}
	if *binSize <= 0 {
		fatal(fmt.Errorf("player-bin-size must be > 0"))
	}
	if *ratingDiffMax < 0 {
		fatal(fmt.Errorf("rating-diff-max must be >= 0"))
	}

	records, err := readParquet(*inputPath, *parallel)
	if err != nil {
		fatal(err)
	}

	minRating, maxRating := ratingMinMax(records)
	if *playerMin > 0 {
		minRating = *playerMin
	}
	if *playerMax > 0 {
		maxRating = *playerMax
	}
	scenarios := buildScenarios(thresholds, minRating, maxRating, *binSize)
	results := make(map[scenario]*stats, len(scenarios))
	for _, sc := range scenarios {
		results[sc] = &stats{}
	}

	for _, record := range records {
		ratingDiff := int(math.Abs(float64(record.SenteRating - record.GoteRating)))
		if ratingDiff > *ratingDiffMax {
			continue
		}
		for _, sc := range scenarios {
			crossingSide := firstCrossingSide(record.MoveEvals, sc.threshold)
			resultSide := winnerSide(record.Result)
			if inBucket(int(record.SenteRating), sc) {
				st := results[sc]
				st.totalGames++
				if crossingSide == "none" || resultSide == "none" {
					st.excludedGames++
				} else if crossingSide == "sente" {
					st.crossings++
					if resultSide == "sente" {
						st.wins++
					}
				}
			}
			if inBucket(int(record.GoteRating), sc) {
				st := results[sc]
				st.totalGames++
				if crossingSide == "none" || resultSide == "none" {
					st.excludedGames++
				} else if crossingSide == "gote" {
					st.crossings++
					if resultSide == "gote" {
						st.wins++
					}
				}
			}
		}
	}

	printCSV(scenarios, results)
}

// readParquet loads all GameRecord rows from a parquet file.
// path: parquet file path; parallel: number of reader goroutines.
func readParquet(path string, parallel int64) ([]cute.GameRecord, error) {
	fileReader, err := local.NewLocalFileReader(path)
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
	done := make(chan struct{})
	var processed int64
	go func(total int) {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				fmt.Fprintf(os.Stderr, "\rread: %d/%d (100%%)\n", total, total)
				return
			case <-ticker.C:
				count := int(atomic.LoadInt64(&processed))
				percent := 0
				if total > 0 {
					percent = int(float64(count) / float64(total) * 100)
				}
				fmt.Fprintf(os.Stderr, "\rread: %d/%d (%d%%)", count, total, percent)
			}
		}
	}(num)
	batchSize := 1024
	for offset := 0; offset < num; offset += batchSize {
		remain := num - offset
		if remain < batchSize {
			batchSize = remain
		}
		batch := make([]cute.GameRecord, batchSize)
		if err := parquetReader.Read(&batch); err != nil {
			close(done)
			return nil, err
		}
		records = append(records, batch...)
		atomic.AddInt64(&processed, int64(len(batch)))
	}
	close(done)
	return records, nil
}

// buildScenarios creates per-bucket scenarios for each eval threshold.
// thresholds: eval thresholds to test; minRating/maxRating: inclusive bounds for buckets; binSize: bucket width.
func buildScenarios(thresholds []int, minRating, maxRating, binSize int) []scenario {
	var scenarios []scenario
	for bucketStart := minRating; bucketStart <= maxRating; bucketStart += binSize {
		bucketEnd := bucketStart + binSize
		for _, threshold := range thresholds {
			scenarios = append(scenarios, scenario{
				threshold:  threshold,
				bucketFrom: bucketStart,
				bucketTo:   bucketEnd,
			})
		}
	}
	sort.Slice(scenarios, func(i, j int) bool {
		if scenarios[i].bucketFrom == scenarios[j].bucketFrom {
			return scenarios[i].threshold < scenarios[j].threshold
		}
		return scenarios[i].bucketFrom < scenarios[j].bucketFrom
	})
	return scenarios
}

// ratingMinMax returns the minimum and maximum player rating observed in records.
func ratingMinMax(records []cute.GameRecord) (int, int) {
	min := 0
	max := 0
	initialized := false
	for _, record := range records {
		values := []int{int(record.SenteRating), int(record.GoteRating)}
		for _, value := range values {
			if !initialized {
				min = value
				max = value
				initialized = true
				continue
			}
			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
		}
	}
	if !initialized {
		return 0, 0
	}
	return min, max
}

// inBucket reports whether rating falls within scenario's [bucketFrom, bucketTo) range.
func inBucket(rating int, sc scenario) bool {
	return rating >= sc.bucketFrom && rating < sc.bucketTo
}

// firstCrossingSide returns which side first crosses the eval threshold.
// evals: per-move evaluations; threshold: centipawn threshold to detect.
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

// winnerSide maps result string to "sente", "gote", or "none".
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

// parseIntList parses comma-separated integers with optional whitespace.
// raw: input string like "100,200"; returns empty slice when raw is blank.
func parseIntList(raw string) ([]int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// printCSV writes CSV to stdout for all scenarios.
func printCSV(scenarios []scenario, results map[scenario]*stats) {
	fmt.Println("player_bucket_from,player_bucket_to,threshold,total_games,crossings,wins,win_rate,excluded")
	for _, sc := range scenarios {
		st := results[sc]
		winRate := 0.0
		if st.crossings > 0 {
			winRate = float64(st.wins) / float64(st.crossings)
		}
		fmt.Printf("%d,%d,%d,%d,%d,%d,%.6f,%d\n",
			sc.bucketFrom,
			sc.bucketTo,
			sc.threshold,
			st.totalGames,
			st.crossings,
			st.wins,
			winRate,
			st.excludedGames,
		)
	}
}

// fatal prints an error to stderr and exits with status 1.
func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
