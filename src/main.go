// main.go
//
// このプログラムでは、指定されたディレクトリにあるすべてのkifファイルを読み込み、各局面を解析して評価値を計算します。
// 評価値をコメントとして書き込んだkifファイルを出力ディレクトリに保存します。

package cute

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Engine string `json:"engine"`
	Nodes  int    `json:"nodes"`
}


func Run() {
	configPath := flag.String("config", "config.json", "path to config.json")
	inputDir := flag.String("input", "test_kif", "input directory for KIF files")
	outputPath := flag.String("output", "output.parquet", "output parquet file")
	processNum := flag.Int("process-num", 1, "number of parallel workers")
	perEvalTimeout := flag.Duration("timeout", 10 * time.Second, "timeout per evaluation")
	flag.Parse()

	cfgPath, repoRoot, err := resolveConfigPath(*configPath)
	if err != nil {
		fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
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
	files, err := collectKIF(*inputDir)
	if err != nil {
		fatal(err)
	}
	if len(files) == 0 {
		fatal(fmt.Errorf("no .kif files found in %s", *inputDir))
	}

	nodes := cfg.Nodes
	if nodes <= 0 {
		nodes = 1000
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

	jobs := make(chan string)
	errCh := make(chan error, workers)
	results := make(chan GameRecord, workers)
	writeErr := make(chan error, 1)
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		writeErr <- writeParquet(*outputPath, results, int64(workers))
	}()

	sessions := make([]*Session, 0, workers)
	for i := 0; i < workers; i++ {
		session, err := StartSession(context.Background(), enginePath)
		if err != nil {
			errCh <- err
			break
		}
		if err := session.Handshake(context.Background()); err != nil {
			errCh <- err
			session.Close()
			break
		}
		sessions = append(sessions, session)
	}
	for _, session := range sessions {
		defer session.Close()
	}
	var wg sync.WaitGroup
	for i := 0; i < len(sessions); i++ {
		session := sessions[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				record, err := buildGameRecord(path, session, nodes, *perEvalTimeout)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to process %s: %v\n", path, err)
					continue
				}
				results <- record
			}
		}()
	}

	for _, path := range files {
		jobs <- path
	}
	close(jobs)
	wg.Wait()
	close(results)
	writeWg.Wait()
	if err := <-writeErr; err != nil {
		fatal(err)
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			fatal(err)
		}
	}
}

func resolveConfigPath(arg string) (string, string, error) {
	if arg != "" {
		abs, err := filepath.Abs(arg)
		if err != nil {
			return "", "", err
		}
		return abs, filepath.Dir(abs), nil
	}
	return FindConfigPath()
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


func FindConfigPath() (string, string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	paths := []string{
		filepath.Join(cwd, "config.json"),
		filepath.Join(cwd, "..", "config.json"),
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path, filepath.Dir(path), nil
		}
	}
	return "", "", fmt.Errorf("config.json not found from %s", cwd)
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

