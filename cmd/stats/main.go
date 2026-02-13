package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	cute "cute/pkg/cute"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ratingStats struct {
	binSize     int
	known       int
	unknown     int
	min         int
	max         int
	initialized bool
	bins        map[int]int
}

type userRatingAgg struct {
	sum   int64
	count int
}

func newRatingStats(binSize int) *ratingStats {
	return &ratingStats{
		binSize: binSize,
		bins:    make(map[int]int),
	}
}

func (rs *ratingStats) Add(rating int32) {
	if rating <= 0 {
		rs.unknown++
		return
	}
	value := int(rating)
	rs.known++
	if !rs.initialized {
		rs.min = value
		rs.max = value
		rs.initialized = true
	} else {
		if value < rs.min {
			rs.min = value
		}
		if value > rs.max {
			rs.max = value
		}
	}
	binStart := (value / rs.binSize) * rs.binSize
	rs.bins[binStart]++
}

func main() {
	kifDir := flag.String("kif-dir", "", "input directory for KIF files")
	parquetPath := flag.String("parquet", "", "input parquet file")
	binSize := flag.Int("bin-size", 100, "rating bin size")
	minGames := flag.Int("min-games", 2, "minimum games per user to count")
	flag.Parse()

	if *binSize <= 0 {
		fatal(fmt.Errorf("bin-size must be > 0"))
	}
	if *minGames <= 0 {
		fatal(fmt.Errorf("min-games must be > 0"))
	}
	if (*kifDir == "") == (*parquetPath == "") {
		fatal(fmt.Errorf("specify exactly one of -kif-dir or -parquet"))
	}

	uniqueUsers := make(map[string]struct{})
	userAgg := make(map[string]*userRatingAgg)
	ratings := newRatingStats(*binSize)
	failed := 0

	inputIsParquet := *parquetPath != ""
	if inputIsParquet {
		records, err := readParquet(*parquetPath, 4)
		if err != nil {
			fatal(err)
		}
		for _, record := range records {
			if record.SenteName != "" {
				uniqueUsers[record.SenteName] = struct{}{}
				addUserRating(userAgg, record.SenteName, record.SenteRating)
			}
			if record.GoteName != "" {
				uniqueUsers[record.GoteName] = struct{}{}
				addUserRating(userAgg, record.GoteName, record.GoteRating)
			}
		}
	} else {
		files, err := cute.CollectKIF(*kifDir)
		if err != nil {
			fatal(err)
		}
		if len(files) == 0 {
			fatal(fmt.Errorf("no .kif files found in %s", *kifDir))
		}
		for _, path := range files {
			players, err := cute.LoadKIFPlayers(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to parse %s: %v\n", path, err)
				failed++
				continue
			}
			if players.SenteName != "" {
				uniqueUsers[players.SenteName] = struct{}{}
				addUserRating(userAgg, players.SenteName, players.SenteRating)
			}
			if players.GoteName != "" {
				uniqueUsers[players.GoteName] = struct{}{}
				addUserRating(userAgg, players.GoteName, players.GoteRating)
			}
		}
	}

	unknownUsers := 0
	usersAtLeast := 0
	for name := range uniqueUsers {
		agg, ok := userAgg[name]
		if !ok || agg.count == 0 {
			unknownUsers++
			continue
		}
		if agg.count >= *minGames {
			usersAtLeast++
		}
		avg := int32(agg.sum / int64(agg.count))
		ratings.Add(avg)
	}

	if inputIsParquet {
		fmt.Printf("input parquet: %s\n", *parquetPath)
	} else {
		fmt.Printf("kif dir: %s\n", *kifDir)
	}
	fmt.Printf("failed files: %d\n", failed)
	fmt.Printf("unique users: %d\n", len(uniqueUsers))
	fmt.Printf("ratings: known=%d unknown=%d (users without rating=%d)\n", ratings.known, ratings.unknown, unknownUsers)
	fmt.Printf("users with >= %d games: %d\n", *minGames, usersAtLeast)
	if ratings.known > 0 {
		fmt.Printf("rating range: %d-%d\n", ratings.min, ratings.max)
	}
	fmt.Printf("rating distribution (bin size=%d):\n", ratings.binSize)
	keys := make([]int, 0, len(ratings.bins))
	for key := range ratings.bins {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, start := range keys {
		end := start + ratings.binSize - 1
		fmt.Printf("%d-%d,%d\n", start, end, ratings.bins[start])
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func addUserRating(agg map[string]*userRatingAgg, name string, rating int32) {
	if rating <= 0 {
		return
	}
	entry, ok := agg[name]
	if !ok {
		entry = &userRatingAgg{}
		agg[name] = entry
	}
	entry.sum += int64(rating)
	entry.count++
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
