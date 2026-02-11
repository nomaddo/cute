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
	"strconv"
	"time"
)

type Config struct {
	Engine string          `json:"engine"`
	Nodes  json.RawMessage `json:"nodes"`
}

func Run() {
	configPath := flag.String("config", "", "path to config.json")
	inputDir := flag.String("input", "test_kif", "input directory for KIF files")
	outputDir := flag.String("output", "output_kif", "output directory for annotated KIF files")
	perEvalTimeout := flag.Duration("timeout", 30*time.Second, "timeout per evaluation")
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
	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fatal(err)
	}
	files, err := collectKIF(*inputDir)
	if err != nil {
		fatal(err)
	}
	if len(files) == 0 {
		fatal(fmt.Errorf("no .kif files found in %s", *inputDir))
	}

	nodes := ParseNodes(cfg.Nodes, 10000)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	session, err := StartSession(ctx, enginePath)
	if err != nil {
		fatal(err)
	}
	defer session.Close()
	if err := session.Handshake(ctx); err != nil {
		fatal(err)
	}

	for _, path := range files {
		if err := processKIF(path, *outputDir, session, nodes, *perEvalTimeout); err != nil {
			fmt.Fprintf(os.Stderr, "failed to process %s: %v\n", path, err)
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

func processKIF(path, outputDir string, session *Session, nodes int, perEvalTimeout time.Duration) error {
	lines, err := readKIFLines(path)
	if err != nil {
		return err
	}
	moves, moveLines, err := parseKIFMoves(lines)
	if err != nil {
		return err
	}
	if len(moves) == 0 {
		return fmt.Errorf("no moves found in %s", path)
	}
	scores := make([]Score, len(moves))
	for i := range moves {
		evalCtx, cancel := context.WithTimeout(context.Background(), perEvalTimeout)
		score, _, err := session.Evaluate(evalCtx, moves[:i+1], nodes)
		cancel()
		if err != nil {
			return fmt.Errorf("move %d: %w", i+1, err)
		}
		scores[i] = score
	}
	annotated := annotateLines(lines, moveLines, scores)
	base := filepath.Base(path)
	outPath := filepath.Join(outputDir, base)
	return writeLines(outPath, annotated)
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

func ParseNodes(raw json.RawMessage, fallback int) int {
	if len(raw) == 0 {
		return fallback
	}
	var asInt int
	if err := json.Unmarshal(raw, &asInt); err == nil && asInt > 0 {
		return asInt
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		if value, err := strconv.Atoi(asString); err == nil && value > 0 {
			return value
		}
	}
	return fallback
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

