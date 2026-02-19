package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	cute "cute/pkg/cute"
)

// posInfo holds the SFEN string and move counts for a qualified position.
type posInfo struct {
	sfen  string
	moves map[string]uint32
}

func main() {
	inputDir := flag.String("input", "test_kif", "input directory for KIF files")
	outputPath := flag.String("output", "book.db", "output book file")
	threshold := flag.Int("threshold", 3, "minimum occurrence count to include in book")
	maxPly := flag.Int("max-ply", 60, "maximum ply to process per game")
	maxFiles := flag.Int("max-files", 0, "maximum number of files to process (0=all)")
	workers := flag.Int("workers", 0, "number of parallel workers (0=NumCPU)")
	flag.Parse()

	if *workers <= 0 {
		*workers = runtime.NumCPU()
	}

	start := time.Now()

	// Count files without building a full path list (saves memory with
	// millions of files).
	totalFiles, err := cute.CountKIF(*inputDir)
	if err != nil {
		fatal(err)
	}
	if totalFiles == 0 {
		fatal(fmt.Errorf("no .kif files found in %s", *inputDir))
	}
	if *maxFiles > 0 && totalFiles > *maxFiles {
		totalFiles = *maxFiles
	}
	fmt.Fprintf(os.Stderr, "files: %d, workers: %d, max-ply: %d, threshold: %d\n",
		totalFiles, *workers, *maxPly, *threshold)

	// ---- Pass 1: count position occurrences (memory-efficient) ----
	// Only stores Packed256 -> uint32, avoiding SFEN string allocations.
	// Files are streamed via WalkKIF – no []string allocation.
	fmt.Fprintf(os.Stderr, "pass 1: counting positions...\n")
	counts, errFiles := runPass1(*inputDir, *maxFiles, *maxPly, *workers, totalFiles)

	total := 0
	for _, c := range counts {
		total += int(c)
	}
	fmt.Fprintf(os.Stderr, "  unique positions: %d, total occurrences: %d, file errors: %d\n",
		len(counts), total, errFiles)

	// Filter: keep only positions meeting the threshold.
	qual := make(map[cute.Packed256]bool)
	for k, c := range counts {
		if c >= uint32(*threshold) {
			qual[k] = true
		}
	}

	// Free pass-1 memory before pass 2.
	counts = nil
	runtime.GC()

	fmt.Fprintf(os.Stderr, "  qualified positions (>=%d): %d\n", *threshold, len(qual))
	if len(qual) == 0 {
		fmt.Fprintln(os.Stderr, "no positions meet the threshold; nothing to write")
		return
	}

	// ---- Pass 2: collect moves for qualified positions ----
	// Re-reads files but only allocates SFEN strings for qualified positions.
	fmt.Fprintf(os.Stderr, "pass 2: collecting moves...\n")
	data := runPass2(*inputDir, *maxFiles, *maxPly, qual, *workers, totalFiles)
	fmt.Fprintf(os.Stderr, "  book entries: %d\n", len(data))

	// ---- Write book file ----
	if err := writeBook(*outputPath, data); err != nil {
		fatal(err)
	}

	fmt.Fprintf(os.Stderr, "wrote %s (%d positions) in %v\n",
		*outputPath, len(data), time.Since(start).Round(time.Millisecond))
}

// ---------------------------------------------------------------------------
// Core iteration: replay a game and invoke a callback for every position
// from which a move was played.
// ---------------------------------------------------------------------------

// iteratePositions loads a KIF file, replays moves up to maxPly, and calls fn
// for each position that has a following move.
//
// Parameters passed to fn:
//   - packed : 256-bit packed position (suitable as map key, 32 bytes)
//   - pos    : borrowed pointer to the current position – do NOT store
//   - ply    : SFEN move number for this position
//   - move   : USI-format move played from this position
func iteratePositions(
	path string,
	maxPly int,
	fn func(packed cute.Packed256, pos *cute.Position, ply int, move string),
) error {
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		return err
	}
	pos := board.InitialPosition()
	moves := board.Moves()
	if len(moves) == 0 {
		return nil
	}

	// Emit the initial position (ply 1) with the first move.
	if packed, err := cute.PackPosition256(pos); err == nil {
		fn(packed, &pos, 1, moves[0])
	}

	limit := maxPly
	if limit > len(moves) {
		limit = len(moves)
	}

	for i := 0; i < limit; i++ {
		if err := pos.ApplyMove(moves[i]); err != nil {
			break
		}
		if !pos.IsLegalPosition() {
			break
		}
		// Only emit if there is a next move from this position.
		if i+1 >= len(moves) || i+1 >= maxPly {
			break
		}
		packed, err := cute.PackPosition256(pos)
		if err != nil {
			break
		}
		fn(packed, &pos, i+2, moves[i+1])
	}
	return nil
}

// ---------------------------------------------------------------------------
// File feeder – streams paths from WalkKIF into a channel, respecting
// maxFiles. Runs in its own goroutine and closes ch when done.
// ---------------------------------------------------------------------------

func feedFiles(inputDir string, maxFiles int, ch chan<- string) {
	sent := 0
	_ = cute.WalkKIF(inputDir, func(path string) error {
		if maxFiles > 0 && sent >= maxFiles {
			return filepath.SkipAll
		}
		ch <- path
		sent++
		return nil
	})
	close(ch)
}

// ---------------------------------------------------------------------------
// Pass 1 – count occurrences (Packed256 → uint32)
// ---------------------------------------------------------------------------

func runPass1(inputDir string, maxFiles, maxPly, workers, totalFiles int) (map[cute.Packed256]uint32, int) {
	counts := make(map[cute.Packed256]uint32)
	var mu sync.Mutex
	var processed, errCount atomic.Int64

	ch := make(chan string, workers*4)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]cute.Packed256, 0, 64)
			for path := range ch {
				batch = batch[:0]
				err := iteratePositions(path, maxPly,
					func(packed cute.Packed256, _ *cute.Position, _ int, _ string) {
						batch = append(batch, packed)
					})
				if err != nil {
					errCount.Add(1)
				}
				if len(batch) > 0 {
					mu.Lock()
					for _, p := range batch {
						counts[p]++
					}
					mu.Unlock()
				}
				if n := processed.Add(1); n%10000 == 0 {
					fmt.Fprintf(os.Stderr, "\r  %d/%d", n, totalFiles)
				}
			}
		}()
	}

	feedFiles(inputDir, maxFiles, ch)
	wg.Wait()
	fmt.Fprintf(os.Stderr, "\r  %d/%d\n", processed.Load(), totalFiles)

	return counts, int(errCount.Load())
}

// ---------------------------------------------------------------------------
// Pass 2 – collect moves for qualified positions
// ---------------------------------------------------------------------------

func runPass2(inputDir string, maxFiles, maxPly int, qual map[cute.Packed256]bool, workers, totalFiles int) map[cute.Packed256]*posInfo {
	data := make(map[cute.Packed256]*posInfo)
	var mu sync.Mutex
	var processed atomic.Int64

	type localEntry struct {
		packed cute.Packed256
		sfen   string
		move   string
	}

	ch := make(chan string, workers*4)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]localEntry, 0, 16)
			for path := range ch {
				batch = batch[:0]
				_ = iteratePositions(path, maxPly,
					func(packed cute.Packed256, pos *cute.Position, ply int, move string) {
						if !qual[packed] {
							return
						}
						// Only compute SFEN for qualified positions.
						batch = append(batch, localEntry{packed, pos.ToSFEN(ply), move})
					})
				if len(batch) > 0 {
					mu.Lock()
					for _, e := range batch {
						info := data[e.packed]
						if info == nil {
							info = &posInfo{sfen: e.sfen, moves: make(map[string]uint32)}
							data[e.packed] = info
						}
						info.moves[e.move]++
					}
					mu.Unlock()
				}
				if n := processed.Add(1); n%10000 == 0 {
					fmt.Fprintf(os.Stderr, "\r  %d/%d", n, totalFiles)
				}
			}
		}()
	}

	feedFiles(inputDir, maxFiles, ch)
	wg.Wait()
	fmt.Fprintf(os.Stderr, "\r  %d/%d\n", processed.Load(), totalFiles)

	return data
}

// ---------------------------------------------------------------------------
// Book writer – YaneuraOu DB format
// ---------------------------------------------------------------------------

func writeBook(path string, data map[cute.Packed256]*posInfo) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	// Header required by the format.
	fmt.Fprintln(w, "#YANEURAOU-DB2016 1.00")

	// Collect entries sorted by SFEN for deterministic / sortable output.
	entries := make([]*posInfo, 0, len(data))
	for _, info := range data {
		entries = append(entries, info)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].sfen < entries[j].sfen
	})

	for _, e := range entries {
		fmt.Fprintf(w, "sfen %s\n", e.sfen)

		// Sort moves by count descending (highest frequency = best move),
		// then alphabetically for stability.
		type mc struct {
			move  string
			count uint32
		}
		ms := make([]mc, 0, len(e.moves))
		for m, c := range e.moves {
			ms = append(ms, mc{m, c})
		}
		sort.Slice(ms, func(i, j int) bool {
			if ms[i].count != ms[j].count {
				return ms[i].count > ms[j].count
			}
			return ms[i].move < ms[j].move
		})

		// Format: <move> <response> <eval> <depth> <count>
		// response=none (no tracking), eval=0, depth=0
		for _, m := range ms {
			fmt.Fprintf(w, "%s none 0 0 %d\n", m.move, m.count)
		}
	}

	return w.Flush()
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
