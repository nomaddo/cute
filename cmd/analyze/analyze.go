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

	cute "cute/pkg/cute"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
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
	thresholdsArg := flag.String("thresholds", "300,500,1000", "comma-separated eval thresholds")
	ratingDiffMax := flag.Int("rating-diff-max", 50, "max rating difference between players")
	ignoreFirstMoves := flag.Int("ignore-first-moves", 0, "ignore evals up to this move number (0=disabled)")
	binSize := flag.Int("player-bin-size", 100, "player rating bucket size")
	playerMin := flag.Int("player-min", 0, "minimum player rating (0 to auto-detect)")
	playerMax := flag.Int("player-max", 0, "maximum player rating (0 to auto-detect)")
	parallel := flag.Int64("parallel", 4, "parquet read parallelism")
	openingDB := flag.String("opening-db", "", "strategy classification parquet file for opening filter")
	filterExpr := flag.String("filter", "", `expr filter on opening DB (e.g. 'has(sente.attack, "四間飛車") && has(gote.note, "居飛車")')`)
	crossingSideFilter := flag.String("crossing-side-filter", "", `expr per-player filter to restrict which side's crossings to count (e.g. 'has(attack, "四間飛車")')`)
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
	if *ignoreFirstMoves < 0 {
		fatal(fmt.Errorf("ignore-first-moves must be >= 0"))
	}

	filter := *filterExpr
	allowedIDs := make(map[string]bool)
	// crossingSideMap: game_id -> which side's crossings to count.
	// "sente", "gote", or "both". Empty map means count all sides.
	crossingSides := make(map[string]string)

	if *openingDB != "" {
		// Build filter from shorthand flags if --filter is not set.
		if filter == "" {
			var parts []string
			if len(parts) > 0 {
				filter = strings.Join(parts, " && ")
			}
		}
		if filter == "" {
			fatal(fmt.Errorf("--opening-db requires --filter"))
		}

		fmt.Fprintf(os.Stderr, "filter: %s\n", filter)
		var err2 error
		allowedIDs, crossingSides, err2 = loadOpeningFilter(*openingDB, filter, *crossingSideFilter, *parallel)
		if err2 != nil {
			fatal(fmt.Errorf("opening-db: %w", err2))
		}
	}

	records, err := readParquet(*inputPath, *parallel)
	if err != nil {
		fatal(err)
	}

	// Filter by opening tags if specified.
	if *openingDB != "" {
		filtered := records[:0]
		for _, r := range records {
			if allowedIDs[normalizeGameID(r.GameID)] {
				filtered = append(filtered, r)
			}
		}
		fmt.Fprintf(os.Stderr, "opening filter: %d/%d games match\n",
			len(filtered), len(records))
		records = filtered
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

	hasCrossingSideFilter := len(crossingSides) > 0

	for _, record := range records {
		ratingDiff := int(math.Abs(float64(record.SenteRating - record.GoteRating)))
		if ratingDiff > *ratingDiffMax {
			continue
		}
		// Determine which sides to count crossings for.
		countSente := true
		countGote := true
		if hasCrossingSideFilter {
			side := crossingSides[normalizeGameID(record.GameID)]
			countSente = side == "sente" || side == "both"
			countGote = side == "gote" || side == "both"
		}
		for _, sc := range scenarios {
			crossingSide := firstCrossingSide(record.MoveEvals, sc.threshold, *ignoreFirstMoves)
			resultSide := winnerSide(record.Result)
			if countSente && inBucket(int(record.SenteRating), sc) {
				st := results[sc]
				if crossingSide == "none" || resultSide == "none" {
					st.excludedGames++
				} else if crossingSide == "sente" {
					st.totalGames++
					st.crossings++
					if resultSide == "sente" {
						st.wins++
					}
				} else if hasCrossingSideFilter {
					// Count games where the filtered side didn't cross first.
					st.totalGames++
				}
			}
			if countGote && inBucket(int(record.GoteRating), sc) {
				st := results[sc]
				if crossingSide == "none" || resultSide == "none" {
					st.excludedGames++
				} else if crossingSide == "gote" {
					st.totalGames++
					st.crossings++
					if resultSide == "gote" {
						st.wins++
					}
				} else if hasCrossingSideFilter {
					// Count games where the filtered side didn't cross first.
					st.totalGames++
				}
			}
		}
	}

	printCSV(scenarios, results, hasCrossingSideFilter)
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
		if scenarios[i].threshold == scenarios[j].threshold {
			return scenarios[i].bucketFrom < scenarios[j].bucketFrom
		}
		return scenarios[i].threshold < scenarios[j].threshold
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
// ignoreFirstMoves: ignore evals up to this move number (0=disabled).
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
// showCrossingRate: when true, adds total_games and crossing_rate columns.
func printCSV(scenarios []scenario, results map[scenario]*stats, showCrossingRate bool) {
	currentThreshold := 0
	first := true
	for _, sc := range scenarios {
		if first || sc.threshold != currentThreshold {
			if !first {
				fmt.Println()
			}
			currentThreshold = sc.threshold
			fmt.Printf("threshold=%d\n", currentThreshold)
			if showCrossingRate {
				fmt.Println("player_rate,total_games,crossings,crossing_rate,wins,win_rate")
			} else {
				fmt.Println("player_rate,crossings,wins,win_rate")
			}
			first = false
		}
		st := results[sc]
		winRate := 0.0
		if st.crossings > 0 {
			winRate = float64(st.wins) / float64(st.crossings)
		}
		playerRate := fmt.Sprintf("%d-%d", sc.bucketFrom, sc.bucketTo)
		if showCrossingRate {
			crossingRate := 0.0
			if st.totalGames > 0 {
				crossingRate = float64(st.crossings) / float64(st.totalGames)
			}
			fmt.Printf("%s,%d,%d,%.6f,%d,%.6f\n",
				playerRate,
				st.totalGames,
				st.crossings,
				crossingRate,
				st.wins,
				winRate,
			)
		} else {
			fmt.Printf("%s,%d,%d,%.6f\n",
				playerRate,
				st.crossings,
				st.wins,
				winRate,
			)
		}
	}
}

// fatal prints an error to stderr and exits with status 1.
func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

// openingRecord matches the strategy classification parquet schema from classify_kif_to_db.rb.
// All fields are OPTIONAL because the Ruby parquet gem writes nullable columns.
// Supports both kif_tags.parquet (11 cols) and 6_senkei.parquet (15 cols) layouts.
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

// loadOpeningFilter reads the opening DB parquet and returns:
// - allowedIDs: set of game_ids matching the filter expression
// - crossingSides: game_id -> "sente"/"gote"/"both" for crossing-side-filter
//
// crossingSideExpr is evaluated per-player using playerTags env.
// If empty, crossingSides is returned empty (meaning count all sides).
func loadOpeningFilter(path, filterExpr, crossingSideExpr string, parallel int64) (map[string]bool, map[string]string, error) {
	// Compile the game filter expression.
	program, err := expr.Compile(filterExpr,
		expr.Env(gameEnv{}),
		expr.AsBool(),
		expr.Function("has", hasFunc,
			new(func([]string, string) bool),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid filter expression: %w", err)
	}

	// Compile the per-player crossing-side filter if provided.
	var crossingProgram *vm.Program
	if crossingSideExpr != "" {
		fmt.Fprintf(os.Stderr, "crossing-side-filter: %s\n", crossingSideExpr)
		var err2 error
		crossingProgram, err2 = expr.Compile(crossingSideExpr,
			expr.Env(playerTags{}),
			expr.AsBool(),
			expr.Function("has", hasFunc,
				new(func([]string, string) bool),
			),
		)
		if err2 != nil {
			return nil, nil, fmt.Errorf("invalid crossing-side-filter expression: %w", err2)
		}
	}

	fileReader, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, nil, err
	}
	defer fileReader.Close()

	parquetReader, err := reader.NewParquetReader(fileReader, new(openingRecord), parallel)
	if err != nil {
		return nil, nil, err
	}
	defer parquetReader.ReadStop()

	num := int(parquetReader.GetNumRows())
	allowedIDs := make(map[string]bool)
	crossingSides := make(map[string]string)
	batchSize := 1024
	for offset := 0; offset < num; offset += batchSize {
		remain := num - offset
		if remain < batchSize {
			batchSize = remain
		}
		batch := make([]openingRecord, batchSize)
		if err := parquetReader.Read(&batch); err != nil {
			return nil, nil, err
		}
		for _, rec := range batch {
			env := rec.toGameEnv()
			out, err := expr.Run(program, env)
			if err != nil {
				continue
			}
			matched, ok := out.(bool)
			if !ok || !matched {
				continue
			}
			gid := normalizeGameID(env.GameID)
			allowedIDs[gid] = true

			// Evaluate crossing-side filter per player.
			if crossingProgram != nil {
				senteMatch := evalPlayerFilter(crossingProgram, env.Sente)
				goteMatch := evalPlayerFilter(crossingProgram, env.Gote)
				switch {
				case senteMatch && goteMatch:
					crossingSides[gid] = "both"
				case senteMatch:
					crossingSides[gid] = "sente"
				case goteMatch:
					crossingSides[gid] = "gote"
				default:
					// Neither side matches crossing filter; exclude from counting.
					delete(allowedIDs, gid)
				}
			}
		}
	}
	return allowedIDs, crossingSides, nil
}

// evalPlayerFilter runs a compiled per-player expr against playerTags.
func evalPlayerFilter(program *vm.Program, tags playerTags) bool {
	out, err := expr.Run(program, tags)
	if err != nil {
		return false
	}
	matched, ok := out.(bool)
	return ok && matched
}

// playerTags holds the parsed tag lists for one player.
type playerTags struct {
	Attack    []string `expr:"attack"`
	Defense   []string `expr:"defense"`
	Technique []string `expr:"technique"`
	Note      []string `expr:"note"`
}

// gameEnv is the environment exposed to filter expressions.
//
// Available fields:
//
//	game_id        string
//	sente.attack   []string    sente.defense  []string
//	sente.technique []string   sente.note     []string
//	gote.attack    []string    gote.defense   []string
//	gote.technique []string    gote.note      []string
//
// Built-in function:
//
//	has(tags, "タグ名") bool  — tags にタグが含まれるか判定
//
// Examples:
//
//	has(sente.attack, "四間飛車") && has(gote.attack, "居飛車")
//	has(sente.attack, "中飛車") || has(gote.attack, "中飛車")
//	has(sente.defense, "美濃囲い") && !has(gote.defense, "穴熊")
type gameEnv struct {
	GameID string     `expr:"game_id"`
	Sente  playerTags `expr:"sente"`
	Gote   playerTags `expr:"gote"`
}

// hasFunc implements the has(tags, tag) function for expr.
func hasFunc(params ...any) (any, error) {
	tags, ok1 := params[0].([]string)
	tag, ok2 := params[1].(string)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("has() expects ([]string, string), got (%T, %T)", params[0], params[1])
	}
	for _, t := range tags {
		if t == tag {
			return true, nil
		}
	}
	return false, nil
}

// toGameEnv converts an openingRecord into a gameEnv for expr evaluation.
func (r *openingRecord) toGameEnv() gameEnv {
	return gameEnv{
		GameID: derefStr(r.GameID),
		Sente: playerTags{
			Attack:    splitTags(derefStr(r.SenteAttackTags)),
			Defense:   splitTags(derefStr(r.SenteDefenseTags)),
			Technique: splitTags(derefStr(r.SenteTechniqueTags)),
			Note:      splitTags(derefStr(r.SenteNoteTags)),
		},
		Gote: playerTags{
			Attack:    splitTags(derefStr(r.GoteAttackTags)),
			Defense:   splitTags(derefStr(r.GoteDefenseTags)),
			Technique: splitTags(derefStr(r.GoteTechniqueTags)),
			Note:      splitTags(derefStr(r.GoteNoteTags)),
		},
	}
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

// normalizeGameID strips the .kif extension for consistent game_id matching
// between the eval parquet (e.g. "35586426.kif") and the opening DB (e.g. "35586426").
func normalizeGameID(id string) string {
	return strings.TrimSuffix(id, ".kif")
}
