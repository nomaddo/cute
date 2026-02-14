package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	cute "cute/pkg/cute"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type stats struct {
	totalGames int
	crossings  int
	wins       int
}

type userStats struct {
	games       int
	ratingSum   int64
	ratingCount int
	byThreshold map[int]*stats
}

func main() {
	input := flag.String("input", "output.parquet", "input parquet file")
	thresholdsArg := flag.String("thresholds", "300,500,1000", "comma-separated eval thresholds")
	minGames := flag.Int("min-games", 10, "minimum games per user")
	parallel := flag.Int64("parallel", 4, "parquet read parallelism")
	flag.Parse()

	if *minGames <= 0 {
		fatal(fmt.Errorf("min-games must be > 0"))
	}
	thresholds, err := parseIntList(*thresholdsArg)
	if err != nil {
		fatal(err)
	}
	if len(thresholds) == 0 {
		fatal(fmt.Errorf("thresholds must be non-empty"))
	}
	sort.Ints(thresholds)

	records, err := readParquet(*input, *parallel)
	if err != nil {
		fatal(err)
	}

	userCounts := make(map[string]int)
	for _, record := range records {
		if record.SenteName != "" {
			userCounts[record.SenteName]++
		}
		if record.GoteName != "" {
			userCounts[record.GoteName]++
		}
	}
	eligible := make(map[string]struct{})
	for name, count := range userCounts {
		if count >= *minGames {
			eligible[name] = struct{}{}
		}
	}

	users := make(map[string]*userStats, len(eligible))
	for name := range eligible {
		perThreshold := make(map[int]*stats, len(thresholds))
		for _, th := range thresholds {
			perThreshold[th] = &stats{}
		}
		users[name] = &userStats{byThreshold: perThreshold}
	}

	for _, record := range records {
		crossingSide := firstCrossingSide(record.MoveEvals, thresholds)
		resultSide := winnerSide(record.Result)

		if record.SenteName != "" {
			if user, ok := users[record.SenteName]; ok {
				user.games++
				user.ratingSum += int64(record.SenteRating)
				user.ratingCount++
				for _, th := range thresholds {
					st := user.byThreshold[th]
					if crossingSide[th] == "sente" {
						st.totalGames++
						st.crossings++
						if resultSide == "sente" {
							st.wins++
						}
					}
				}
			}
		}

		if record.GoteName != "" {
			if user, ok := users[record.GoteName]; ok {
				user.games++
				user.ratingSum += int64(record.GoteRating)
				user.ratingCount++
				for _, th := range thresholds {
					st := user.byThreshold[th]
					if crossingSide[th] == "gote" {
						st.totalGames++
						st.crossings++
						if resultSide == "gote" {
							st.wins++
						}
					}
				}
			}
		}
	}

	headers := []string{"user", "avg_rating"}
	for _, th := range thresholds {
		headers = append(headers, fmt.Sprintf("win_rate_%d", th))
	}
	fmt.Println(strings.Join(headers, ","))

	userOrder := make([]string, 0, len(users))
	for name := range users {
		userOrder = append(userOrder, name)
	}
	sort.Slice(userOrder, func(i, j int) bool {
		left := users[userOrder[i]]
		right := users[userOrder[j]]
		leftAvg := 0.0
		if left.ratingCount > 0 {
			leftAvg = float64(left.ratingSum) / float64(left.ratingCount)
		}
		rightAvg := 0.0
		if right.ratingCount > 0 {
			rightAvg = float64(right.ratingSum) / float64(right.ratingCount)
		}
		if leftAvg == rightAvg {
			return userOrder[i] < userOrder[j]
		}
		return leftAvg > rightAvg
	})
	for _, name := range userOrder {
		user := users[name]
		avgRating := 0.0
		if user.ratingCount > 0 {
			avgRating = float64(user.ratingSum) / float64(user.ratingCount)
		}
		row := []string{name, fmt.Sprintf("%.1f", avgRating)}
		for _, th := range thresholds {
			st := user.byThreshold[th]
			winRate := 0.0
			if st.crossings > 0 {
				winRate = float64(st.wins) / float64(st.crossings)
			}
			row = append(row, fmt.Sprintf("%.6f", winRate))
		}
		fmt.Println(strings.Join(row, ","))
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
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func firstCrossingSide(evals []cute.MoveEval, thresholds []int) map[int]string {
	result := make(map[int]string, len(thresholds))
	remaining := make(map[int]struct{}, len(thresholds))
	for _, th := range thresholds {
		remaining[th] = struct{}{}
		result[th] = "none"
	}
	for _, eval := range evals {
		if len(remaining) == 0 {
			break
		}
		for th := range remaining {
			if eval.ScoreType == "mate" {
				if eval.ScoreValue >= 0 {
					result[th] = "sente"
				} else {
					result[th] = "gote"
				}
				delete(remaining, th)
				continue
			}
			if eval.ScoreValue >= int32(th) {
				result[th] = "sente"
				delete(remaining, th)
				continue
			}
			if eval.ScoreValue <= -int32(th) {
				result[th] = "gote"
				delete(remaining, th)
			}
		}
	}
	return result
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
