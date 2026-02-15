package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	cute "cute/pkg/cute"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// userStats aggregates per-user crossing and strategy statistics.
type userStats struct {
	parquetGames int            // total games in eval parquet (used for min-games filter)
	totalWins    int            // total wins regardless of crossing
	totalGames   int            // games included in crossing analysis (excludes draws/none)
	crossings    int            // times the user's side crossed first
	wins         int            // wins when user crossed first
	attackCounts map[string]int // attack tag → number of games
	ratingSum    int64
	ratingCount  int
}

// openingRecord matches the strategy classification parquet schema.
// All fields are OPTIONAL because the Ruby parquet gem writes nullable columns.
type openingRecord struct {
	GameID             *string `parquet:"name=game_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GameType           *string `parquet:"name=game_type, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SenteName          *string `parquet:"name=sente_name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SenteRating        *int32  `parquet:"name=sente_rating, type=INT32, repetitiontype=OPTIONAL"`
	GoteName           *string `parquet:"name=gote_name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GoteRating         *int32  `parquet:"name=gote_rating, type=INT32, repetitiontype=OPTIONAL"`
	TurnMax            *int32  `parquet:"name=turn_max, type=INT32, repetitiontype=OPTIONAL"`
	SenteAttackTags    *string `parquet:"name=sente_attack_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SenteDefenseTags   *string `parquet:"name=sente_defense_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SenteTechniqueTags *string `parquet:"name=sente_technique_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SenteNoteTags      *string `parquet:"name=sente_note_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GoteAttackTags     *string `parquet:"name=gote_attack_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GoteDefenseTags    *string `parquet:"name=gote_defense_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GoteTechniqueTags  *string `parquet:"name=gote_technique_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	GoteNoteTags       *string `parquet:"name=gote_note_tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

// openingInfo stores per-game opening information indexed by game_id.
type openingInfo struct {
	senteAttackTags []string
	goteAttackTags  []string
}

func main() {
	parquetPath := flag.String("parquet", "", "input eval parquet file")
	openingDBPath := flag.String("opening-db", "", "strategy classification parquet file")
	threshold := flag.Int("threshold", 500, "eval threshold for crossing detection")
	minGames := flag.Int("min-games", 20, "minimum games per user (in opening DB)")
	ignoreFirstMoves := flag.Int("ignore-first-moves", 0, "ignore evals up to this move number")
	topN := flag.Int("top-attacks", 3, "number of top attack strategies to show per user")
	sortBy := flag.String("sort", "crossing_rate", "sort column: crossing_rate, win_rate, total_games, avg_rating")
	flag.Parse()

	if *parquetPath == "" || *openingDBPath == "" {
		fatal(fmt.Errorf("both -parquet and -opening-db are required"))
	}

	// 1. Load opening DB.
	fmt.Fprintf(os.Stderr, "loading opening DB: %s\n", *openingDBPath)
	openings, err := loadOpeningDB(*openingDBPath, 4)
	if err != nil {
		fatal(fmt.Errorf("opening-db: %w", err))
	}
	fmt.Fprintf(os.Stderr, "opening DB: %d games\n", len(openings))

	// 2. Load eval parquet.
	fmt.Fprintf(os.Stderr, "loading eval parquet: %s\n", *parquetPath)
	records, err := readEvalParquet(*parquetPath, 4)
	if err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "eval parquet: %d games\n", len(records))

	// 3. Build per-user stats from eval parquet, joining with opening DB for attack tags.
	users := make(map[string]*userStats)
	joined := 0

	for _, record := range records {
		gid := normalizeGameID(record.GameID)
		opening, hasOpening := openings[gid]

		crossingSide := firstCrossingSide(record.MoveEvals, *threshold, *ignoreFirstMoves)
		resultSide := winnerSide(record.Result)

		if hasOpening {
			joined++
		}

		// Process sente player.
		if record.SenteName != "" {
			u := getOrCreateUser(users, record.SenteName)
			u.parquetGames++
			if resultSide == "sente" {
				u.totalWins++
			}
			if record.SenteRating > 0 {
				u.ratingSum += int64(record.SenteRating)
				u.ratingCount++
			}
			if hasOpening {
				for _, tag := range opening.senteAttackTags {
					u.attackCounts[tag]++
				}
			}
			if crossingSide != "none" && resultSide != "none" {
				u.totalGames++
				if crossingSide == "sente" {
					u.crossings++
					if resultSide == "sente" {
						u.wins++
					}
				}
			}
		}

		// Process gote player.
		if record.GoteName != "" {
			u := getOrCreateUser(users, record.GoteName)
			u.parquetGames++
			if resultSide == "gote" {
				u.totalWins++
			}
			if record.GoteRating > 0 {
				u.ratingSum += int64(record.GoteRating)
				u.ratingCount++
			}
			if hasOpening {
				for _, tag := range opening.goteAttackTags {
					u.attackCounts[tag]++
				}
			}
			if crossingSide != "none" && resultSide != "none" {
				u.totalGames++
				if crossingSide == "gote" {
					u.crossings++
					if resultSide == "gote" {
						u.wins++
					}
				}
			}
		}
	}

	fmt.Fprintf(os.Stderr, "joined games: %d\n", joined)

	// 4. Filter by min-games, compute rates, sort.
	type userResult struct {
		name           string
		avgRating      float64
		parquetGames   int
		overallWinRate float64
		totalGames     int
		crossings      int
		crossingRate   float64
		wins           int
		winRate        float64
		topAttacks     string
	}

	var results []userResult
	for name, u := range users {
		if u.parquetGames < *minGames {
			continue
		}
		avgRating := 0.0
		if u.ratingCount > 0 {
			avgRating = float64(u.ratingSum) / float64(u.ratingCount)
		}
		crossingRate := 0.0
		if u.totalGames > 0 {
			crossingRate = float64(u.crossings) / float64(u.totalGames)
		}
		winRate := 0.0
		if u.crossings > 0 {
			winRate = float64(u.wins) / float64(u.crossings)
		}
		overallWinRate := 0.0
		if u.parquetGames > 0 {
			overallWinRate = float64(u.totalWins) / float64(u.parquetGames)
		}
		results = append(results, userResult{
			name:           name,
			avgRating:      avgRating,
			parquetGames:   u.parquetGames,
			overallWinRate: overallWinRate,
			totalGames:     u.totalGames,
			crossings:      u.crossings,
			crossingRate:   crossingRate,
			wins:           u.wins,
			winRate:        winRate,
			topAttacks:     formatTopAttacks(u.attackCounts, *topN),
		})
	}

	sort.Slice(results, func(i, j int) bool {
		switch *sortBy {
		case "win_rate":
			if results[i].winRate != results[j].winRate {
				return results[i].winRate > results[j].winRate
			}
			return results[i].totalGames > results[j].totalGames
		case "total_games":
			return results[i].totalGames > results[j].totalGames
		case "avg_rating":
			return results[i].avgRating > results[j].avgRating
		default: // crossing_rate
			if results[i].crossingRate != results[j].crossingRate {
				return results[i].crossingRate > results[j].crossingRate
			}
			return results[i].totalGames > results[j].totalGames
		}
	})

	// 5. Print CSV.
	fmt.Fprintf(os.Stderr, "users with >= %d games: %d (threshold=%d)\n",
		*minGames, len(results), *threshold)
	fmt.Println("name,avg_rating,games,overall_win_rate,eval_games,crossings,crossing_rate,wins,win_rate,top_attacks")
	for _, r := range results {
		fmt.Printf("%s,%.0f,%d,%.4f,%d,%d,%.4f,%d,%.4f,%s\n",
			r.name,
			r.avgRating,
			r.parquetGames,
			r.overallWinRate,
			r.totalGames,
			r.crossings,
			r.crossingRate,
			r.wins,
			r.winRate,
			r.topAttacks,
		)
	}
}

func getOrCreateUser(users map[string]*userStats, name string) *userStats {
	u, ok := users[name]
	if !ok {
		u = &userStats{attackCounts: make(map[string]int)}
		users[name] = u
	}
	return u
}

// formatTopAttacks returns the top-N attack tags as "tag1(count1) tag2(count2) ...".
func formatTopAttacks(counts map[string]int, top int) string {
	type kv struct {
		tag   string
		count int
	}
	var pairs []kv
	for tag, count := range counts {
		pairs = append(pairs, kv{tag, count})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].tag < pairs[j].tag
		}
		return pairs[i].count > pairs[j].count
	})
	if len(pairs) > top {
		pairs = pairs[:top]
	}
	var parts []string
	for _, p := range pairs {
		parts = append(parts, fmt.Sprintf("%s(%d)", p.tag, p.count))
	}
	return strings.Join(parts, " ")
}

// firstCrossingSide returns which side first crosses the eval threshold.
func firstCrossingSide(evals []cute.MoveEval, threshold int, ignoreFirstMoves int) string {
	for _, eval := range evals {
		if ignoreFirstMoves > 0 && int(eval.Ply) <= ignoreFirstMoves {
			continue
		}
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

// loadOpeningDB reads the strategy classification parquet into a map keyed by game_id.
func loadOpeningDB(path string, parallel int64) (map[string]openingInfo, error) {
	fileReader, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	parquetReader, err := reader.NewParquetReader(fileReader, new(openingRecord), parallel)
	if err != nil {
		return nil, err
	}
	defer parquetReader.ReadStop()

	num := int(parquetReader.GetNumRows())
	result := make(map[string]openingInfo, num)
	batchSize := 1024
	for offset := 0; offset < num; offset += batchSize {
		remain := num - offset
		if remain < batchSize {
			batchSize = remain
		}
		batch := make([]openingRecord, batchSize)
		if err := parquetReader.Read(&batch); err != nil {
			return nil, err
		}
		for _, rec := range batch {
			gid := normalizeGameID(derefStr(rec.GameID))
			result[gid] = openingInfo{
				senteAttackTags: splitTags(derefStr(rec.SenteAttackTags)),
				goteAttackTags:  splitTags(derefStr(rec.GoteAttackTags)),
			}
		}
	}
	return result, nil
}

// readEvalParquet loads all GameRecord rows from a parquet file.
func readEvalParquet(path string, parallel int64) ([]cute.GameRecord, error) {
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

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// splitTags splits a comma-separated tag string into trimmed non-empty strings.
func splitTags(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// normalizeGameID strips the .kif extension for consistent game_id matching.
func normalizeGameID(id string) string {
	return strings.TrimSuffix(id, ".kif")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
