package cute

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Engine manages a USI engine process.
type Engine struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	mu     sync.Mutex
	closed bool
}

// Start launches an external USI engine process.
func Start(ctx context.Context, path string, args ...string) (*Engine, error) {
	if path == "" {
		return nil, errors.New("engine path is required")
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = filepath.Dir(path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Engine{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr}, nil
}

// Reader returns a protocol reader for engine stdout.
func (e *Engine) Reader() *Reader {
	return NewReader(e.stdout)
}

// Stderr returns the stderr stream for the engine process.
func (e *Engine) Stderr() io.Reader {
	return e.stderr
}

// Send sends a single command line to the engine.
func (e *Engine) Send(line string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("engine is closed")
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	_, err := io.WriteString(e.stdin, line)
	return err
}

// Close terminates the engine process.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	_ = e.Send("quit")
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- e.cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		_ = e.cmd.Process.Kill()
		return errors.New("engine did not exit in time")
	}
}

// Reader reads and parses USI protocol lines from the engine.
type Reader struct {
	scanner *bufio.Scanner
}

// NewReader creates a Reader for engine stdout.
func NewReader(r io.Reader) *Reader {
	return &Reader{scanner: bufio.NewScanner(r)}
}

// ParseLine converts a raw line into a protocol event.
func ParseLine(line string) (Event, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Event{}, errors.New("empty line")
	}
	fields := strings.Fields(line)
	switch fields[0] {
	case "id":
		if len(fields) < 3 {
			return Event{}, fmt.Errorf("invalid id: %q", line)
		}
		return Event{Type: EventID, Key: fields[1], Value: strings.Join(fields[2:], " ")}, nil
	case "usiok":
		return Event{Type: EventUSIOK}, nil
	case "readyok":
		return Event{Type: EventReadyOK}, nil
	case "bestmove":
		if len(fields) < 2 {
			return Event{}, fmt.Errorf("invalid bestmove: %q", line)
		}
		e := Event{Type: EventBestMove, Move: fields[1]}
		if len(fields) >= 4 && fields[2] == "ponder" {
			e.Ponder = fields[3]
		}
		return e, nil
	case "info":
		return Event{Type: EventInfo, Raw: line}, nil
	default:
		return Event{Type: EventUnknown, Raw: line}, nil
	}
}

// Next blocks until a line is available or EOF occurs.
func (r *Reader) Next() (Event, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return Event{}, err
		}
		return Event{}, io.EOF
	}
	return ParseLine(r.scanner.Text())
}

// EventType represents a USI protocol event type.
type EventType int

const (
	EventUnknown EventType = iota
	EventID
	EventUSIOK
	EventReadyOK
	EventInfo
	EventBestMove
)

// Event is a parsed USI protocol line.
type Event struct {
	Type   EventType
	Key    string
	Value  string
	Move   string
	Ponder string
	Raw    string
}

// Score represents a USI evaluation score.
type Score struct {
	Kind  string
	Value int
}

// String returns a stable text representation for comments/logging.
func (s Score) String() string {
	if s.Kind == "cp" {
		return fmt.Sprintf("cp %d", s.Value)
	}
	if s.Kind == "mate" {
		return fmt.Sprintf("mate %d", s.Value)
	}
	return "unknown"
}

// Session manages a USI engine session and event stream.
type Session struct {
	engine *Engine
	reader *Reader
	events chan Event
	errCh  chan error
}

// StartSession launches a USI engine and starts a reader goroutine.
func StartSession(ctx context.Context, path string, args ...string) (*Session, error) {
	engine, err := Start(ctx, path, args...)
	if err != nil {
		return nil, err
	}
	reader := engine.Reader()
	events := make(chan Event, 64)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		for {
			event, err := reader.Next()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			events <- event
		}
	}()
	return &Session{engine: engine, reader: reader, events: events, errCh: errCh}, nil
}

// Close terminates the engine process.
func (s *Session) Close() error {
	if s == nil || s.engine == nil {
		return nil
	}
	return s.engine.Close()
}

// Stderr returns the engine's stderr reader for diagnostics.
func (s *Session) Stderr() io.Reader {
	if s == nil || s.engine == nil {
		return nil
	}
	return s.engine.Stderr()
}

// Handshake runs the standard USI handshake.
func (s *Session) Handshake(ctx context.Context) error {
	if err := s.engine.Send("usi"); err != nil {
		return err
	}
	if _, err := s.waitForEvent(ctx, EventUSIOK); err != nil {
		return err
	}
	if err := s.engine.Send("setoption name FV_SCALE value 36"); err != nil {
		return err
	}
	if err := s.engine.Send("setoption name Threads value 1"); err != nil {
		return err
	}
	if err := s.engine.Send("setoption name USI_Hash value 700"); err != nil {
		return err
	}
	if err := s.engine.Send("isready"); err != nil {
		return err
	}
	_, err := s.waitForEvent(ctx, EventReadyOK)
	return err
}

// Evaluate runs a bounded search for the given SFEN position and returns the last score.
func (s *Session) Evaluate(ctx context.Context, sfen string, moveTimeMs int) (Score, string, error) {
	cmd := "position sfen " + sfen
	if err := s.engine.Send(cmd); err != nil {
		return Score{}, "", err
	}
	if moveTimeMs <= 0 {
		moveTimeMs = 1
	}
	if err := s.engine.Send(fmt.Sprintf("go movetime %d", moveTimeMs)); err != nil {
		return Score{}, "", err
	}
	turn := "b"
	if fields := strings.Fields(sfen); len(fields) >= 2 {
		turn = fields[1]
	}

	var score Score
	haveScore := false
	for {
		event, err := s.nextEvent(ctx)
		if err != nil {
			return Score{}, "", err
		}
		switch event.Type {
		case EventInfo:
			if parsed, ok := parseInfoScore(event.Raw); ok {
				score = parsed
				haveScore = true
			}
		case EventBestMove:
			if !haveScore {
				return Score{}, event.Move, errors.New("no score in engine output")
			}
			if turn == "w" {
				score = flipScore(score)
			}
			return score, event.Move, nil
		}
	}
}

func flipScore(score Score) Score {
	score.Value = -score.Value
	return score
}

func (s *Session) waitForEvent(ctx context.Context, want EventType) (Event, error) {
	for {
		event, err := s.nextEvent(ctx)
		if err != nil {
			return Event{}, err
		}
		if event.Type == want {
			return event, nil
		}
	}
}

func (s *Session) nextEvent(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	case err := <-s.errCh:
		if err == nil {
			return Event{}, errors.New("engine stdout closed")
		}
		return Event{}, err
	case event, ok := <-s.events:
		if !ok {
			return Event{}, errors.New("engine stdout closed")
		}
		return event, nil
	}
}

func parseInfoScore(line string) (Score, bool) {
	fields := strings.Fields(line)
	for i := 0; i+2 < len(fields); i++ {
		if fields[i] != "score" {
			continue
		}
		kind := fields[i+1]
		value, err := strconv.Atoi(fields[i+2])
		if err != nil {
			return Score{}, false
		}
		if kind != "cp" && kind != "mate" {
			return Score{}, false
		}
		return Score{Kind: kind, Value: value}, true
	}
	return Score{}, false
}
