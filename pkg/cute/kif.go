package cute

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

type square struct {
	file int
	rank int
}

type Color int

const (
	Black Color = iota
	White
)

type Piece struct {
	kind     string
	color    Color
	promoted bool
}

type Position struct {
	board [9][9]*Piece
	hands map[Color]map[string]int
	turn  Color
}

type Board struct {
	initial Position
	moves   []string
	foulEnd bool
}

type KIFPlayers struct {
	SenteName   string
	SenteRating int32
	GoteName    string
	GoteRating  int32
}

var moveLineRe = regexp.MustCompile(`^\s*(\d+)\s+(.+?)\s+\(`)
var terminalLineRe = regexp.MustCompile(`^\s*(\d+)\s+(.+?)\s*$`)
var fromSquareRe = regexp.MustCompile(`\((\d)(\d)\)`)
var nameRatingRe = regexp.MustCompile(`^(.+?)\((\d+)\)$`)

func readKIFLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text, err := decodeKIF(data)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(text, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], "\r")
	}
	return lines, nil
}

func decodeKIF(data []byte) (string, error) {
	if bytes.HasPrefix(data, []byte{0xEF, 0xBB, 0xBF}) {
		data = data[3:]
	}
	if utf8.Valid(data) {
		return string(data), nil
	}
	reader := transform.NewReader(bytes.NewReader(data), japanese.ShiftJIS.NewDecoder())
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(decoded) {
		return "", errors.New("failed to decode Shift-JIS KIF")
	}
	return string(decoded), nil
}

func parseKIFMoves(lines []string) ([]string, []int, error) {
	var moves []string
	var lineIdx []int
	var prevDest *square
	for i, line := range lines {
		match := moveLineRe.FindStringSubmatch(line)
		if len(match) == 0 {
			continue
		}
		moveText := strings.TrimSpace(match[2])
		if moveText == "" {
			continue
		}
		move, dest, end, err := parseKIFMoveToken(moveText, prevDest)
		if err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if end {
			break
		}
		moves = append(moves, move)
		lineIdx = append(lineIdx, i)
		prevDest = dest
	}
	return moves, lineIdx, nil
}

func parseKIFMoveToken(token string, prevDest *square) (string, *square, bool, error) {
	if isTerminalMove(token) {
		return "", nil, true, nil
	}
	work := strings.TrimSpace(token)
	var dest square
	if strings.HasPrefix(work, "同") {
		if prevDest == nil {
			return "", nil, false, errors.New("same-square move without previous destination")
		}
		dest = *prevDest
		work = strings.TrimSpace(strings.TrimLeft(strings.TrimPrefix(work, "同"), " \u3000"))
	} else {
		runes := []rune(work)
		if len(runes) < 2 {
			return "", nil, false, fmt.Errorf("invalid move token: %s", token)
		}
		file, ok := parseFileRune(runes[0])
		if !ok {
			return "", nil, false, fmt.Errorf("invalid destination file in %s", token)
		}
		rank, ok := parseRankRune(runes[1])
		if !ok {
			return "", nil, false, fmt.Errorf("invalid destination rank in %s", token)
		}
		dest = square{file: file, rank: rank}
		work = strings.TrimSpace(string(runes[2:]))
	}

	fromFile, fromRank, hasFrom := parseFromSquare(work)
	if hasFrom {
		work = fromSquareRe.ReplaceAllString(work, "")
	}

	noPromote := strings.Contains(work, "不成")
	if noPromote {
		work = strings.Replace(work, "不成", "", 1)
	}
	promote := false
	if strings.Contains(work, "成") {
		promote = true
		work = strings.Replace(work, "成", "", 1)
	}

	drop := strings.Contains(work, "打")
	if drop {
		work = strings.Replace(work, "打", "", 1)
	}

	piece, promotedPiece, forcePromote, err := parsePiece(work)
	if err != nil {
		return "", nil, false, err
	}
	if forcePromote {
		promote = true
	}
	if noPromote {
		promote = false
	}
	if drop {
		if promotedPiece {
			return "", nil, false, errors.New("cannot drop promoted piece")
		}
		usi := fmt.Sprintf("%s*%s", piece, formatSquare(dest))
		return usi, &dest, false, nil
	}
	if !hasFrom {
		return "", nil, false, errors.New("missing source square")
	}
	from := square{file: fromFile, rank: fromRank}
	usi := fmt.Sprintf("%s%s", formatSquare(from), formatSquare(dest))
	if promote {
		usi += "+"
	}
	return usi, &dest, false, nil
}

func isTerminalMove(token string) bool {
	switch token {
	case "投了", "中断", "持将棋", "千日手", "詰み", "切れ負け", "反則勝ち", "反則負け", "入玉勝ち", "勝ち宣言":
		return true
	default:
		return false
	}
}

// isFoulEnd returns true if the game ended with 反則勝ち or 反則負け.
// The last move before the terminal marker is an illegal move and
// produces an invalid position that engines cannot evaluate.
func isFoulEnd(lines []string) bool {
	terminal, _ := findTerminalMove(lines)
	return terminal == "反則勝ち" || terminal == "反則負け"
}

func parseFromSquare(text string) (int, int, bool) {
	match := fromSquareRe.FindStringSubmatch(text)
	if len(match) != 3 {
		return 0, 0, false
	}
	file := int(match[1][0] - '0')
	rank := int(match[2][0] - '0')
	if file < 1 || file > 9 || rank < 1 || rank > 9 {
		return 0, 0, false
	}
	return file, rank, true
}

func parseFileRune(r rune) (int, bool) {
	if r >= '1' && r <= '9' {
		return int(r - '0'), true
	}
	if r >= '１' && r <= '９' {
		return int(r-'１') + 1, true
	}
	return 0, false
}

func parseRankRune(r rune) (int, bool) {
	switch r {
	case '一':
		return 1, true
	case '二':
		return 2, true
	case '三':
		return 3, true
	case '四':
		return 4, true
	case '五':
		return 5, true
	case '六':
		return 6, true
	case '七':
		return 7, true
	case '八':
		return 8, true
	case '九':
		return 9, true
	default:
		return 0, false
	}
}

func formatSquare(s square) string {
	return fmt.Sprintf("%d%c", s.file, rankToLetter(s.rank))
}

func rankToLetter(rank int) byte {
	return byte('a' + rank - 1)
}

type pieceDef struct {
	name         string
	letter       string
	promoted     bool
	forcePromote bool
}

var pieceDefs = []pieceDef{
	{name: "成銀", letter: "S", forcePromote: true},
	{name: "成桂", letter: "N", forcePromote: true},
	{name: "成香", letter: "L", forcePromote: true},
	{name: "成歩", letter: "P", forcePromote: true},
	{name: "と", letter: "P", promoted: true},
	{name: "馬", letter: "B", promoted: true},
	{name: "龍", letter: "R", promoted: true},
	{name: "竜", letter: "R", promoted: true},
	{name: "王", letter: "K"},
	{name: "玉", letter: "K"},
	{name: "飛", letter: "R"},
	{name: "角", letter: "B"},
	{name: "金", letter: "G"},
	{name: "銀", letter: "S"},
	{name: "桂", letter: "N"},
	{name: "香", letter: "L"},
	{name: "歩", letter: "P"},
}

func parsePiece(text string) (string, bool, bool, error) {
	clean := strings.TrimSpace(text)
	for _, def := range pieceDefs {
		if strings.HasPrefix(clean, def.name) {
			return def.letter, def.promoted, def.forcePromote, nil
		}
	}
	return "", false, false, fmt.Errorf("unknown piece in %s", text)
}

func annotateLines(lines []string, moveLines []int, scores []Score) []string {
	out := make([]string, 0, len(lines))
	moveIdx := 0
	for i, line := range lines {
		if moveIdx < len(moveLines) && i == moveLines[moveIdx] {
			line = fmt.Sprintf("%s * eval %s", line, scores[moveIdx].String())
			moveIdx++
		}
		out = append(out, line)
	}
	return out
}

func BuildGameRecord(ctx context.Context, path string, session *Session, moveTimeMs int, cache map[string]Score) (GameRecord, error) {
	lines, err := readKIFLines(path)
	if err != nil {
		return GameRecord{}, err
	}
	moves, _, err := parseKIFMoves(lines)
	if err != nil {
		return GameRecord{}, err
	}
	if len(moves) == 0 {
		return GameRecord{}, fmt.Errorf("no moves found in %s", path)
	}

	// When the game ended with a foul (反則勝ち/反則負け), the last move
	// produced an illegal position that engines cannot evaluate.
	// Exclude it from the move list.
	foul := isFoulEnd(lines)
	if foul && len(moves) > 0 {
		moves = moves[:len(moves)-1]
	}

	pos, err := initialPositionFromKIF(lines)
	if err != nil {
		return GameRecord{}, err
	}
	if cache == nil {
		cache = make(map[string]Score)
	}
	scores := make([]Score, len(moves))
	for i := range moves {
		if err := ctx.Err(); err != nil {
			return GameRecord{}, err
		}
		if err := pos.ApplyMove(moves[i]); err != nil {
			return GameRecord{}, fmt.Errorf("move %d: %w", i+1, err)
		}
		sfen := pos.ToSFEN(i + 1)
		key := sfen
		if fields := strings.Fields(sfen); len(fields) >= 3 {
			key = strings.Join(fields[:3], " ")
		}
		if cached, ok := cache[key]; ok {
			scores[i] = cached
			continue
		}
		score, _, err := session.Evaluate(ctx, sfen, moveTimeMs)
		if err != nil {
			return GameRecord{}, fmt.Errorf("move %d: %w", i+1, err)
		}
		scores[i] = score

		// Cache only up to first 30 moves to limit memory usage.
		if i < 30 {
			cache[key] = score
		}
	}

	senteName, senteRating, goteName, goteRating := parsePlayers(lines)
	result, winReason := parseResult(lines)
	evals := make([]MoveEval, 0, len(scores))
	for i, score := range scores {
		evals = append(evals, MoveEval{
			Ply:        int32(i + 1),
			ScoreType:  score.Kind,
			ScoreValue: int32(score.Value),
		})
	}

	record := GameRecord{
		GameID:      filepath.Base(path),
		SenteName:   senteName,
		SenteRating: senteRating,
		GoteName:    goteName,
		GoteRating:  goteRating,
		Result:      result,
		WinReason:   winReason,
		MoveCount:   int32(len(moves)),
		MoveEvals:   evals,
	}
	return record, nil
}

func parsePlayers(lines []string) (string, int32, string, int32) {
	sente := headerValue(lines, "先手")
	gote := headerValue(lines, "後手")
	senteName, senteRating := parseNameRating(sente)
	goteName, goteRating := parseNameRating(gote)
	return senteName, senteRating, goteName, goteRating
}

func LoadKIFPlayers(path string) (KIFPlayers, error) {
	lines, err := readKIFLines(path)
	if err != nil {
		return KIFPlayers{}, err
	}
	return PlayersFromKIFLines(lines), nil
}

func PlayersFromKIFLines(lines []string) KIFPlayers {
	senteName, senteRating, goteName, goteRating := parsePlayers(lines)
	return KIFPlayers{
		SenteName:   senteName,
		SenteRating: senteRating,
		GoteName:    goteName,
		GoteRating:  goteRating,
	}
}

func headerValue(lines []string, key string) string {
	prefixes := []string{key + "：", key + ":"}
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		for _, prefix := range prefixes {
			if strings.HasPrefix(trim, prefix) {
				return strings.TrimSpace(strings.TrimPrefix(trim, prefix))
			}
		}
	}
	return ""
}

func parseNameRating(raw string) (string, int32) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0
	}
	match := nameRatingRe.FindStringSubmatch(raw)
	if len(match) == 3 {
		return strings.TrimSpace(match[1]), parseInt32(match[2])
	}
	return raw, 0
}

func parseInt32(raw string) int32 {
	var value int
	_, _ = fmt.Sscanf(raw, "%d", &value)
	return int32(value)
}

func parseResult(lines []string) (string, string) {
	terminal, ply := findTerminalMove(lines)
	if terminal == "" {
		return "unknown", ""
	}
	result, reason := resultFromTerminal(terminal, ply)
	return result, reason
}

func findTerminalMove(lines []string) (string, int) {
	ply := 0
	for _, line := range lines {
		// Try the standard move line pattern first (has clock info).
		match := moveLineRe.FindStringSubmatch(line)
		if len(match) == 0 {
			// Terminal markers like "反則勝ち" have no clock parenthesis.
			match = terminalLineRe.FindStringSubmatch(line)
		}
		if len(match) == 0 {
			continue
		}
		moveText := strings.TrimSpace(match[2])
		if moveText == "" {
			continue
		}
		ply++
		if isTerminalMove(moveText) {
			return moveText, ply
		}
	}
	return "", 0
}

func resultFromTerminal(token string, ply int) (string, string) {
	switch token {
	case "中断":
		return "abort", token
	case "持将棋", "千日手":
		return "draw", token
	case "反則勝ち", "詰み":
		return winnerFromPly(ply), token
	case "投了", "切れ負け", "反則負け":
		return winnerFromPly(ply + 1), token
	default:
		return "unknown", token
	}
}

func winnerFromPly(ply int) string {
	if ply%2 == 1 {
		return "sente_win"
	}
	return "gote_win"
}

func CollectKIF(root string) ([]string, error) {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".kif") {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func LoadBoardFromKIF(path string) (*Board, error) {
	lines, err := readKIFLines(path)
	if err != nil {
		return nil, err
	}
	return BoardFromKIF(lines)
}

func BoardFromKIF(lines []string) (*Board, error) {
	pos, err := initialPositionFromKIF(lines)
	if err != nil {
		return nil, err
	}
	moves, _, err := parseKIFMoves(lines)
	if err != nil {
		return nil, err
	}
	return &Board{initial: pos, moves: moves, foulEnd: isFoulEnd(lines)}, nil
}

func (b *Board) MoveCount() int {
	if b == nil {
		return 0
	}
	return len(b.moves)
}

// IsFoulEnd returns true if the game ended with an illegal move (反則).
// When true, the last move in the move list produced an illegal position
// and should not be evaluated by an engine.
func (b *Board) IsFoulEnd() bool {
	if b == nil {
		return false
	}
	return b.foulEnd
}

func (b *Board) SFENAt(move int) (string, error) {
	if b == nil {
		return "", errors.New("board is nil")
	}
	if move < 0 || move > len(b.moves) {
		return "", fmt.Errorf("move out of range: %d", move)
	}
	pos := b.initial.Clone()
	for i := 0; i < move; i++ {
		if err := pos.ApplyMove(b.moves[i]); err != nil {
			return "", fmt.Errorf("move %d: %w", i+1, err)
		}
	}
	return pos.ToSFEN(move + 1), nil
}

func KIFFileToSFEN(path string) (string, error) {
	board, err := LoadBoardFromKIF(path)
	if err != nil {
		return "", err
	}
	return board.SFENAt(0)
}

func KIFToSFEN(lines []string) (string, error) {
	board, err := BoardFromKIF(lines)
	if err != nil {
		return "", err
	}
	return board.SFENAt(0)
}

func standardSFEN() string {
	return "lnsgkgsnl/1r5b1/ppppppppp/9/9/9/PPPPPPPPP/1B5R1/LNSGKGSNL b - 1"
}

func initialPositionFromKIF(lines []string) (Position, error) {
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "手合割") {
			if strings.Contains(trim, "平手") {
				return parseSFENPosition(standardSFEN())
			}
		}
	}

	boardLines := collectBoardLines(lines)
	if len(boardLines) == 0 {
		return Position{}, errors.New("no board definition found")
	}
	board, err := parseBoardLines(boardLines)
	if err != nil {
		return Position{}, err
	}
	turn := parseTurn(lines)
	black, white, err := parseHandsCounts(lines)
	if err != nil {
		return Position{}, err
	}
	hand := buildHands(black, white)
	if hand == "" {
		hand = "-"
	}
	sfen := fmt.Sprintf("%s %s %s 1", board, turn, hand)
	return parseSFENPosition(sfen)
}

func parseSFENPosition(sfen string) (Position, error) {
	fields := strings.Fields(sfen)
	if len(fields) < 3 {
		return Position{}, fmt.Errorf("invalid sfen: %s", sfen)
	}
	pos := Position{
		board: [9][9]*Piece{},
		hands: map[Color]map[string]int{
			Black: {},
			White: {},
		},
	}
	if fields[1] == "w" {
		pos.turn = White
	} else {
		pos.turn = Black
	}
	if err := parseBoardSFEN(fields[0], &pos); err != nil {
		return Position{}, err
	}
	if err := parseHandsSFEN(fields[2], &pos); err != nil {
		return Position{}, err
	}
	return pos, nil
}

func parseBoardSFEN(board string, pos *Position) error {
	ranks := strings.Split(board, "/")
	if len(ranks) != 9 {
		return fmt.Errorf("invalid board ranks: %d", len(ranks))
	}
	for rankIndex, rankText := range ranks {
		file := 9
		runes := []rune(rankText)
		for i := 0; i < len(runes); i++ {
			r := runes[i]
			if r >= '1' && r <= '9' {
				empty := int(r - '0')
				file -= empty
				continue
			}
			promoted := false
			if r == '+' {
				promoted = true
				i++
				if i >= len(runes) {
					return errors.New("dangling promotion marker")
				}
				r = runes[i]
			}
			color := Black
			if r >= 'a' && r <= 'z' {
				color = White
				r = rune(strings.ToUpper(string(r))[0])
			}
			kind, ok := sfenPiece(r)
			if !ok {
				return fmt.Errorf("unknown sfen piece %c", r)
			}
			if file < 1 {
				return errors.New("too many files in rank")
			}
			rank := rankIndex + 1
			pos.board[rank-1][file-1] = &Piece{kind: kind, color: color, promoted: promoted}
			file--
		}
		if file != 0 {
			return fmt.Errorf("rank %d does not have 9 files", rankIndex+1)
		}
	}
	return nil
}

func sfenPiece(r rune) (string, bool) {
	switch r {
	case 'P':
		return "P", true
	case 'L':
		return "L", true
	case 'N':
		return "N", true
	case 'S':
		return "S", true
	case 'G':
		return "G", true
	case 'B':
		return "B", true
	case 'R':
		return "R", true
	case 'K':
		return "K", true
	default:
		return "", false
	}
}

func parseHandsSFEN(hand string, pos *Position) error {
	if hand == "-" {
		return nil
	}
	count := 0
	for _, r := range hand {
		if r >= '0' && r <= '9' {
			count = count*10 + int(r-'0')
			continue
		}
		if count == 0 {
			count = 1
		}
		color := Black
		if r >= 'a' && r <= 'z' {
			color = White
			r = rune(strings.ToUpper(string(r))[0])
		}
		piece, ok := sfenPiece(r)
		if !ok {
			return fmt.Errorf("unknown hand piece %c", r)
		}
		pos.hands[color][piece] += count
		count = 0
	}
	if count != 0 {
		return errors.New("trailing hand count")
	}
	return nil
}

func (p Position) Clone() Position {
	clone := Position{
		board: [9][9]*Piece{},
		hands: map[Color]map[string]int{
			Black: {},
			White: {},
		},
		turn: p.turn,
	}
	for r := 0; r < 9; r++ {
		for f := 0; f < 9; f++ {
			if p.board[r][f] == nil {
				continue
			}
			piece := *p.board[r][f]
			clone.board[r][f] = &piece
		}
	}
	for color, hand := range p.hands {
		for key, val := range hand {
			clone.hands[color][key] = val
		}
	}
	return clone
}

func (p *Position) ToSFEN(moveNumber int) string {
	var rows []string
	for rank := 1; rank <= 9; rank++ {
		rows = append(rows, p.rankToSFEN(rank))
	}
	board := strings.Join(rows, "/")
	turn := "b"
	if p.turn == White {
		turn = "w"
	}
	black := p.hands[Black]
	white := p.hands[White]
	hand := buildHands(black, white)
	if hand == "" {
		hand = "-"
	}
	return fmt.Sprintf("%s %s %s %d", board, turn, hand, moveNumber)
}

func (p *Position) rankToSFEN(rank int) string {
	var b strings.Builder
	empty := 0
	flushEmpty := func() {
		if empty > 0 {
			b.WriteString(fmt.Sprintf("%d", empty))
			empty = 0
		}
	}
	for file := 9; file >= 1; file-- {
		piece := p.board[rank-1][file-1]
		if piece == nil {
			empty++
			continue
		}
		flushEmpty()
		text := piece.kind
		if piece.promoted {
			text = "+" + text
		}
		if piece.color == White {
			text = strings.ToLower(text)
		}
		b.WriteString(text)
	}
	flushEmpty()
	return b.String()
}

func (p *Position) ApplyMove(move string) error {
	parsed, err := parseUSIMove(move)
	if err != nil {
		return err
	}
	if parsed.drop {
		return p.applyDrop(parsed)
	}
	return p.applyMove(parsed)
}

type usiMove struct {
	from    square
	to      square
	drop    bool
	piece   string
	promote bool
}

func parseUSIMove(move string) (usiMove, error) {
	if strings.Contains(move, "*") {
		parts := strings.SplitN(move, "*", 2)
		if len(parts) != 2 || len(parts[0]) != 1 {
			return usiMove{}, fmt.Errorf("invalid drop move: %s", move)
		}
		piece := strings.ToUpper(parts[0])
		to, err := parseUSISquare(parts[1])
		if err != nil {
			return usiMove{}, err
		}
		return usiMove{drop: true, piece: piece, to: to}, nil
	}
	if len(move) < 4 {
		return usiMove{}, fmt.Errorf("invalid move: %s", move)
	}
	from, err := parseUSISquare(move[0:2])
	if err != nil {
		return usiMove{}, err
	}
	to, err := parseUSISquare(move[2:4])
	if err != nil {
		return usiMove{}, err
	}
	promote := false
	if len(move) > 4 {
		if move[4] != '+' {
			return usiMove{}, fmt.Errorf("invalid promotion marker: %s", move)
		}
		promote = true
	}
	return usiMove{from: from, to: to, promote: promote}, nil
}

func parseUSISquare(text string) (square, error) {
	if len(text) != 2 {
		return square{}, fmt.Errorf("invalid square: %s", text)
	}
	file := int(text[0] - '0')
	if file < 1 || file > 9 {
		return square{}, fmt.Errorf("invalid file: %s", text)
	}
	rank := int(text[1]-'a') + 1
	if rank < 1 || rank > 9 {
		return square{}, fmt.Errorf("invalid rank: %s", text)
	}
	return square{file: file, rank: rank}, nil
}

func (p *Position) applyDrop(move usiMove) error {
	hand := p.hands[p.turn]
	if hand[move.piece] == 0 {
		return fmt.Errorf("no %s in hand", move.piece)
	}
	if p.pieceAt(move.to) != nil {
		return errors.New("drop destination occupied")
	}
	hand[move.piece]--
	if hand[move.piece] == 0 {
		delete(hand, move.piece)
	}
	p.setPiece(move.to, &Piece{kind: move.piece, color: p.turn})
	p.toggleTurn()
	return nil
}

func (p *Position) applyMove(move usiMove) error {
	piece := p.pieceAt(move.from)
	if piece == nil {
		return fmt.Errorf("no piece at %d%c", move.from.file, rankToLetter(move.from.rank))
	}
	if piece.color != p.turn {
		return errors.New("moving opponent piece")
	}
	captured := p.pieceAt(move.to)
	if captured != nil {
		if captured.color == p.turn {
			return errors.New("capturing own piece")
		}
		captureKind := captured.kind
		p.hands[p.turn][captureKind]++
	}
	p.setPiece(move.from, nil)
	moved := *piece
	if move.promote {
		if moved.kind == "K" || moved.kind == "G" {
			return errors.New("cannot promote king or gold")
		}
		moved.promoted = true
	}
	p.setPiece(move.to, &moved)
	p.toggleTurn()
	return nil
}

func (p *Position) pieceAt(s square) *Piece {
	if s.file < 1 || s.file > 9 || s.rank < 1 || s.rank > 9 {
		return nil
	}
	return p.board[s.rank-1][s.file-1]
}

func (p *Position) setPiece(s square, piece *Piece) {
	if s.file < 1 || s.file > 9 || s.rank < 1 || s.rank > 9 {
		return
	}
	if piece == nil {
		p.board[s.rank-1][s.file-1] = nil
		return
	}
	copy := *piece
	p.board[s.rank-1][s.file-1] = &copy
}

func (p *Position) toggleTurn() {
	if p.turn == Black {
		p.turn = White
	} else {
		p.turn = Black
	}
}

func collectBoardLines(lines []string) []string {
	var board []string
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "|") && strings.HasSuffix(trim, "|") {
			board = append(board, trim)
		}
	}
	return board
}

func parseBoardLines(lines []string) (string, error) {
	if len(lines) < 9 {
		return "", fmt.Errorf("board lines must be 9 rows, got %d", len(lines))
	}
	rows := make([]string, 0, 9)
	for i := 0; i < 9; i++ {
		row, err := parseBoardRow(lines[i])
		if err != nil {
			return "", fmt.Errorf("row %d: %w", i+1, err)
		}
		rows = append(rows, row)
	}
	return strings.Join(rows, "/"), nil
}

func parseBoardRow(line string) (string, error) {
	trim := strings.TrimSpace(line)
	trim = strings.TrimPrefix(trim, "|")
	trim = strings.TrimSuffix(trim, "|")
	runes := []rune(trim)
	var cells []string
	for i := 0; i < len(runes); {
		r := runes[i]
		if r == ' ' || r == '\t' || r == '　' {
			i++
			continue
		}
		if r == '・' {
			cells = append(cells, "")
			i++
			continue
		}
		isGote := false
		if r == 'v' {
			isGote = true
			i++
			if i >= len(runes) {
				return "", errors.New("dangling gote marker")
			}
			r = runes[i]
		}
		piece, consumed, err := parseBoardPiece(runes[i:])
		if err != nil {
			return "", err
		}
		if isGote {
			piece = strings.ToLower(piece)
		}
		cells = append(cells, piece)
		i += consumed
	}
	if len(cells) != 9 {
		return "", fmt.Errorf("expected 9 cells, got %d", len(cells))
	}
	return compressEmpty(cells), nil
}

func parseBoardPiece(runes []rune) (string, int, error) {
	if len(runes) == 0 {
		return "", 0, errors.New("missing piece")
	}
	switch runes[0] {
	case 'と':
		return "+P", 1, nil
	case '馬':
		return "+B", 1, nil
	case '龍', '竜':
		return "+R", 1, nil
	case '成':
		if len(runes) < 2 {
			return "", 0, errors.New("missing promoted piece")
		}
		promoted, ok := promotedBase(runes[1])
		if !ok {
			return "", 0, fmt.Errorf("unknown promoted piece %c", runes[1])
		}
		return "+" + promoted, 2, nil
	default:
		base, ok := basePiece(runes[0])
		if !ok {
			return "", 0, fmt.Errorf("unknown piece %c", runes[0])
		}
		return base, 1, nil
	}
}

func promotedBase(r rune) (string, bool) {
	switch r {
	case '銀':
		return "S", true
	case '桂':
		return "N", true
	case '香':
		return "L", true
	case '歩':
		return "P", true
	default:
		return "", false
	}
}

func basePiece(r rune) (string, bool) {
	switch r {
	case '歩':
		return "P", true
	case '香':
		return "L", true
	case '桂':
		return "N", true
	case '銀':
		return "S", true
	case '金':
		return "G", true
	case '角':
		return "B", true
	case '飛':
		return "R", true
	case '玉', '王':
		return "K", true
	default:
		return "", false
	}
}

func compressEmpty(cells []string) string {
	var b strings.Builder
	empty := 0
	flushEmpty := func() {
		if empty > 0 {
			b.WriteString(fmt.Sprintf("%d", empty))
			empty = 0
		}
	}
	for _, cell := range cells {
		if cell == "" {
			empty++
			continue
		}
		flushEmpty()
		b.WriteString(cell)
	}
	flushEmpty()
	return b.String()
}

func parseTurn(lines []string) string {
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "手番") {
			if strings.Contains(trim, "後手") {
				return "w"
			}
			if strings.Contains(trim, "先手") {
				return "b"
			}
		}
	}
	return "b"
}

func parseHands(lines []string) (string, error) {
	black, white, err := parseHandsCounts(lines)
	if err != nil {
		return "", err
	}
	hand := buildHands(black, white)
	if hand == "" {
		return "-", nil
	}
	return hand, nil
}

func parseHandsCounts(lines []string) (map[string]int, map[string]int, error) {
	black := make(map[string]int)
	white := make(map[string]int)
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "先手の持駒") {
			counts, err := parseHandLine(trim)
			if err != nil {
				return nil, nil, err
			}
			mergeCounts(black, counts)
		}
		if strings.HasPrefix(trim, "後手の持駒") {
			counts, err := parseHandLine(trim)
			if err != nil {
				return nil, nil, err
			}
			mergeCounts(white, counts)
		}
	}
	return black, white, nil
}

func mergeCounts(dst, src map[string]int) {
	for key, val := range src {
		dst[key] += val
	}
}

func parseHandLine(line string) (map[string]int, error) {
	parts := strings.SplitN(line, "：", 2)
	if len(parts) != 2 {
		parts = strings.SplitN(line, ":", 2)
	}
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid hand line: %s", line)
	}
	text := strings.TrimSpace(parts[1])
	if text == "なし" {
		return map[string]int{}, nil
	}
	counts := make(map[string]int)
	for len(text) > 0 {
		piece, rest, err := nextHandToken(text)
		if err != nil {
			return nil, err
		}
		counts[piece.name] += piece.count
		text = strings.TrimSpace(rest)
	}
	return counts, nil
}

type handToken struct {
	name  string
	count int
}

func nextHandToken(text string) (handToken, string, error) {
	if text == "" {
		return handToken{}, "", errors.New("empty hand token")
	}
	runes := []rune(text)
	name := string(runes[0])
	piece, ok := basePiece(runes[0])
	if !ok {
		return handToken{}, "", fmt.Errorf("unknown hand piece %s", name)
	}
	count, consumed := parseCount(runes[1:])
	if consumed == 0 {
		return handToken{name: piece, count: 1}, string(runes[1:]), nil
	}
	return handToken{name: piece, count: count}, string(runes[1+consumed:]), nil
}

func parseCount(runes []rune) (int, int) {
	if len(runes) == 0 {
		return 0, 0
	}
	if runes[0] >= '0' && runes[0] <= '9' {
		val := 0
		i := 0
		for i < len(runes) && runes[i] >= '0' && runes[i] <= '9' {
			val = val*10 + int(runes[i]-'0')
			i++
		}
		return val, i
	}
	value := 0
	consumed := 0
	for consumed < len(runes) {
		n, ok := japaneseNumber(runes[consumed])
		if !ok {
			break
		}
		value = value*10 + n
		consumed++
	}
	if value == 0 {
		return 0, 0
	}
	return value, consumed
}

func japaneseNumber(r rune) (int, bool) {
	switch r {
	case '一':
		return 1, true
	case '二':
		return 2, true
	case '三':
		return 3, true
	case '四':
		return 4, true
	case '五':
		return 5, true
	case '六':
		return 6, true
	case '七':
		return 7, true
	case '八':
		return 8, true
	case '九':
		return 9, true
	case '十':
		return 10, true
	default:
		return 0, false
	}
}

func buildHands(black, white map[string]int) string {
	order := []string{"R", "B", "G", "S", "N", "L", "P"}
	var b strings.Builder
	for _, piece := range order {
		count := black[piece]
		if count > 0 {
			if count > 1 {
				b.WriteString(fmt.Sprintf("%d", count))
			}
			b.WriteString(piece)
		}
	}
	for _, piece := range order {
		count := white[piece]
		if count > 0 {
			if count > 1 {
				b.WriteString(fmt.Sprintf("%d", count))
			}
			b.WriteString(strings.ToLower(piece))
		}
	}
	return b.String()
}
