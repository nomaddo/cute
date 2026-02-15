package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cute "cute/pkg/cute"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	startTime := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configPath := flag.String("config", "config.json", "path to config.json")
	inputDir := flag.String("input", "test_kif", "input directory for KIF files")
	outputPath := flag.String("output", "output.parquet", "output parquet file")
	processNum := flag.Int("process-num", 20, "number of parallel workers")
	resume := flag.Bool("resume", false, "resume from existing output parquet")
	flag.Parse()

	cfgPath, repoRoot, err := resolveConfigPath(*configPath)
	if err != nil {
		fatal(err)
	}
	cfg, err := cute.LoadConfig(cfgPath)
	if err != nil {
		fatal(err)
	}
	enginePath, err := resolveEnginePath(cfg.Engine, repoRoot)
	if err != nil {
		fatal(err)
	}
	if _, err := os.Stat(enginePath); err != nil {
		fatal(fmt.Errorf("engine binary not found at %s: %w", enginePath, err))
	}
	files, err := cute.CollectKIF(*inputDir)
	if err != nil {
		fatal(err)
	}
	if len(files) == 0 {
		fatal(fmt.Errorf("no .kif files found in %s", *inputDir))
	}

	moveTimeMs := cfg.Millis
	if moveTimeMs <= 0 {
		moveTimeMs = 1000
	}

	workers := *processNum
	if workers <= 0 {
		workers = 1
	}
	if workers > len(files) {
		workers = len(files)
	}
	if workers == 0 {
		return
	}
	if dir := filepath.Dir(*outputPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fatal(err)
		}
	}

	outputTarget := *outputPath
	processedIDs := make(map[string]struct{})
	resumeFromExisting := false
	if *resume {
		if _, err := os.Stat(*outputPath); err == nil {
			resumeFromExisting = true
			outputTarget = *outputPath + ".tmp"
		}
	}

	jobs := make(chan string)
	errCh := make(chan error, workers)
	results := make(chan cute.GameRecord, workers)
	writeErr := make(chan error, 1)
	done := make(chan struct{})
	var processed int64
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		writeErr <- cute.WriteParquet(outputTarget, results, int64(workers))
	}()
	if resumeFromExisting {
		if err := readExistingRecords(*outputPath, int64(workers), processedIDs, results); err != nil {
			fatal(err)
		}
	}
	go func(total int) {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				fmt.Fprintf(os.Stderr, "\rprogress: %d/%d (100%%)\n", total, total)
				return
			case <-ticker.C:
				count := int(atomic.LoadInt64(&processed))
				percent := 0
				if total > 0 {
					percent = int(float64(count) / float64(total) * 100)
				}
				fmt.Fprintf(os.Stderr, "\rprogress: %d/%d (%d%%)", count, total, percent)
			}
		}
	}(len(files))

	var wg sync.WaitGroup
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	stopRequested := make(chan struct{})
	go func() {
		<-stopCh
		cancel()
		close(stopRequested)
	}()
	defer signal.Stop(stopCh)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if isStopRequested(stopRequested) {
				return
			}
			session, err := startSession(ctx, enginePath)
			if err != nil {
				errCh <- err
				return
			}
			defer session.Close()
			evalCache := make(map[string]cute.Score)
			for path := range jobs {
				if isStopRequested(stopRequested) {
					return
				}
				fileStart := time.Now()
				record, err := cute.BuildGameRecord(ctx, path, session, moveTimeMs, evalCache)
				if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					return
				}
				if err != nil && isEngineFailure(err) {
					if isStopRequested(stopRequested) {
						return
					}
					_ = session.Close()
					session, err = startSession(ctx, enginePath)
					if err != nil {
						errCh <- err
						return
					}
					if isStopRequested(stopRequested) {
						return
					}
					record, err = cute.BuildGameRecord(ctx, path, session, moveTimeMs, evalCache)
					if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
						return
					}
				}
				if isStopRequested(stopRequested) {
					return
				}
				elapsed := time.Since(fileStart).Round(time.Millisecond)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to process %s (%s): %v\n", path, elapsed, err)
					atomic.AddInt64(&processed, 1)
					continue
				}
				results <- record
				fmt.Fprintf(os.Stderr, "processed %s (%s)\n", path, elapsed)
				atomic.AddInt64(&processed, 1)
			}
		}()
	}

enqueue:
	for _, path := range files {
		if _, ok := processedIDs[filepath.Base(path)]; ok {
			atomic.AddInt64(&processed, 1)
			continue
		}
		select {
		case <-stopRequested:
			break enqueue
		case jobs <- path:
		}
	}
	close(jobs)
	wg.Wait()
	close(done)
	close(results)
	writeWg.Wait()
	if err := <-writeErr; err != nil {
		fatal(err)
	}
	if resumeFromExisting {
		if err := os.Rename(outputTarget, *outputPath); err != nil {
			fatal(err)
		}
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			fatal(err)
		}
	}
	elapsed := time.Since(startTime).Round(time.Second)
	fmt.Fprintf(os.Stderr, "elapsed: %s, processed: %d\n", elapsed, atomic.LoadInt64(&processed))
}

func readExistingRecords(path string, parallel int64, ids map[string]struct{}, out chan<- cute.GameRecord) error {
	fileReader, err := local.NewLocalFileReader(path)
	if err != nil {
		return err
	}
	defer fileReader.Close()

	parquetReader, err := reader.NewParquetReader(fileReader, new(cute.GameRecord), parallel)
	if err != nil {
		return err
	}
	defer parquetReader.ReadStop()

	rows := int(parquetReader.GetNumRows())
	batchSize := 1024
	for offset := 0; offset < rows; offset += batchSize {
		remain := rows - offset
		if remain < batchSize {
			batchSize = remain
		}
		batch := make([]cute.GameRecord, batchSize)
		if err := parquetReader.Read(&batch); err != nil {
			return err
		}
		for i := range batch {
			ids[batch[i].GameID] = struct{}{}
			out <- batch[i]
		}
	}
	return nil
}

func startSession(ctx context.Context, enginePath string) (*cute.Session, error) {
	session, err := cute.StartSession(ctx, enginePath)
	if err != nil {
		return nil, err
	}
	if err := session.Handshake(ctx); err != nil {
		session.Close()
		return nil, err
	}
	return session, nil
}

func isEngineFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "EOF") || strings.Contains(msg, "engine stdout closed")
}

func resolveConfigPath(arg string) (string, string, error) {
	if arg != "" {
		abs, err := filepath.Abs(arg)
		if err != nil {
			return "", "", err
		}
		return abs, filepath.Dir(abs), nil
	}
	return cute.FindConfigPath()
}

func resolveEnginePath(cfgEngine, repoRoot string) (string, error) {
	if cfgEngine == "" {
		return "", errors.New("engine path is required")
	}
	if filepath.IsAbs(cfgEngine) {
		return cfgEngine, nil
	}
	return filepath.Join(repoRoot, cfgEngine), nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func isStopRequested(stopRequested <-chan struct{}) bool {
	select {
	case <-stopRequested:
		return true
	default:
		return false
	}
}
