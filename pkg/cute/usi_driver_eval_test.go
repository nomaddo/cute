package cute_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cute "cute/pkg/cute"
)

func TestUSIEvaluateTestdataKIFs(t *testing.T) {
	cfgPath, repoRoot, err := cute.FindConfigPath()
	if err != nil {
		t.Fatalf("failed to locate config.json: %v", err)
	}
	cfg, err := cute.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("failed to load config.json: %v", err)
	}
	if cfg.Engine == "" {
		t.Fatal("config.json is missing engine path")
	}

	enginePath := cfg.Engine
	if !filepath.IsAbs(enginePath) {
		enginePath = filepath.Join(repoRoot, enginePath)
	}
	if _, err := os.Stat(enginePath); err != nil {
		t.Skipf("engine binary not found at %s: %v", enginePath, err)
	}

	testdataDir := filepath.Join(repoRoot, "pkg", "cute", "testdata")
	files, err := cute.CollectKIF(testdataDir)
	if err != nil {
		t.Fatalf("failed to collect kifs: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no .kif files found in testdata")
	}

	moveTimeMs := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	session, err := cute.StartSession(ctx, enginePath)
	if err != nil {
		t.Fatalf("failed to start engine session: %v", err)
	}
	defer session.Close()

	stderrBuf := &bytes.Buffer{}
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(stderrBuf, session.Stderr())
		close(stderrDone)
	}()

	if err := session.Handshake(ctx); err != nil {
		if shouldSkipForMissingLibs(stderrBuf, stderrDone) {
			t.Skipf("engine cannot start due to missing runtime libraries: %s", strings.TrimSpace(stderrBuf.String()))
		}
		t.Fatalf("usi handshake failed: %v", err)
	}

	for _, path := range files {
		board, err := cute.LoadBoardFromKIF(path)
		if err != nil {
			t.Fatalf("failed to load board from %s: %v", path, err)
		}
		moveCount := board.MoveCount()
		// When the game ended with a foul, the last move produced an
		// illegal position that the engine cannot evaluate.
		evalCount := moveCount
		if board.IsFoulEnd() && evalCount > 0 {
			evalCount--
		}
		for i := 0; i <= evalCount; i++ {
			sfen, err := board.SFENAt(i)
			if err != nil {
				t.Fatalf("failed to build sfen at move %d for %s: %v", i, path, err)
			}
			if _, _, err := session.Evaluate(ctx, sfen, moveTimeMs); err != nil {
				t.Fatalf("failed to evaluate %s move %d: %v", path, i, err)
			}
		}
	}
}
