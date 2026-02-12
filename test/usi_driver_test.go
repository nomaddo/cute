package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	usi "cute/src"
)

func TestUSIEngineBestMove(t *testing.T) {
	cfgPath, repoRoot, err := usi.FindConfigPath()
	if err != nil {
		t.Fatalf("failed to locate config.json: %v", err)
	}
	cfg, err := usi.LoadConfig(cfgPath)
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

	moveTimeMs := cfg.Millis
	if moveTimeMs <= 0 {
		moveTimeMs = 1000
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng, err := usi.Start(ctx, enginePath)
	if err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}
	defer eng.Close()

	stderrBuf := &bytes.Buffer{}
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(stderrBuf, eng.Stderr())
		close(stderrDone)
	}()

	reader := eng.Reader()
	events := make(chan usi.Event, 32)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		for {
			event, err := reader.Next()
			if err != nil {
				errCh <- err
				return
			}
			events <- event
		}
	}()

	if err := eng.Send("usi"); err != nil {
		t.Fatalf("failed to send usi: %v", err)
	}
	if _, err := waitForEvent(ctx, events, errCh, usi.EventUSIOK); err != nil {
		if shouldSkipForMissingLibs(stderrBuf, stderrDone) {
			t.Skipf("engine cannot start due to missing runtime libraries: %s", strings.TrimSpace(stderrBuf.String()))
		}
		t.Fatalf("usi handshake failed: %v", err)
	}

	if err := eng.Send("isready"); err != nil {
		t.Fatalf("failed to send isready: %v", err)
	}
	if _, err := waitForEvent(ctx, events, errCh, usi.EventReadyOK); err != nil {
		if shouldSkipForMissingLibs(stderrBuf, stderrDone) {
			t.Skipf("engine cannot start due to missing runtime libraries: %s", strings.TrimSpace(stderrBuf.String()))
		}
		t.Fatalf("ready handshake failed: %v", err)
	}

	if err := eng.Send("position startpos"); err != nil {
		t.Fatalf("failed to send position: %v", err)
	}
	if err := eng.Send(fmt.Sprintf("go movetime %d", moveTimeMs)); err != nil {
		t.Fatalf("failed to send go: %v", err)
	}
	best, err := waitForEvent(ctx, events, errCh, usi.EventBestMove)
	if err != nil {
		t.Fatalf("failed to get bestmove: %v", err)
	}
	if best.Move == "" || best.Move == "none" {
		t.Fatalf("bestmove is empty: %+v", best)
	}
}

func shouldSkipForMissingLibs(stderrBuf *bytes.Buffer, stderrDone <-chan struct{}) bool {
	select {
	case <-stderrDone:
	case <-time.After(500 * time.Millisecond):
	}

	msg := stderrBuf.String()
	return strings.Contains(msg, "GLIBC") || strings.Contains(msg, "GLIBCXX")
}

func waitForEvent(ctx context.Context, events <-chan usi.Event, errCh <-chan error, want usi.EventType) (usi.Event, error) {
	for {
		select {
		case <-ctx.Done():
			return usi.Event{}, ctx.Err()
		case err := <-errCh:
			if err == nil {
				return usi.Event{}, errors.New("engine stdout closed")
			}
			return usi.Event{}, err
		case event, ok := <-events:
			if !ok {
				return usi.Event{}, errors.New("engine stdout closed")
			}
			if event.Type == want {
				return event, nil
			}
		}
	}
}

