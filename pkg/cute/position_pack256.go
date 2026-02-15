package cute

import "fmt"

type Packed256 struct {
	Words [4]uint64
}

type bitWriter256 struct {
	words [4]uint64
	pos   int
}

type bitReader256 struct {
	words [4]uint64
	pos   int
}

type codeSpec struct {
	kind    string
	bits    uint64
	bitLen  int
	isEmpty bool
}

type codeBook struct {
	byLen  map[int]map[uint64]codeSpec
	maxLen int
}

var boardCodes = []codeSpec{
	{kind: "", bits: 0b0, bitLen: 1, isEmpty: true},
	{kind: "P", bits: 0b01, bitLen: 2},
	{kind: "L", bits: 0b0011, bitLen: 4},
	{kind: "N", bits: 0b1011, bitLen: 4},
	{kind: "S", bits: 0b0111, bitLen: 4},
	{kind: "G", bits: 0b01111, bitLen: 5},
	{kind: "B", bits: 0b011111, bitLen: 6},
	{kind: "R", bits: 0b111111, bitLen: 6},
}

var handCodes = []codeSpec{
	{kind: "P", bits: 0b0, bitLen: 1},
	{kind: "L", bits: 0b001, bitLen: 3},
	{kind: "N", bits: 0b101, bitLen: 3},
	{kind: "S", bits: 0b011, bitLen: 3},
	{kind: "G", bits: 0b0111, bitLen: 4},
	{kind: "B", bits: 0b01111, bitLen: 5},
	{kind: "R", bits: 0b11111, bitLen: 5},
}

var boardCodeBook = buildCodeBook(boardCodes)
var handCodeBook = buildCodeBook(handCodes)

func PackPosition256(pos Position) (Packed256, error) {
	writer := &bitWriter256{}

	turnBit := uint64(0)
	if pos.turn == White {
		turnBit = 1
	}
	if err := writer.writeBit(turnBit); err != nil {
		return Packed256{}, err
	}

	blackKing, whiteKing, err := kingSquares(pos)
	if err != nil {
		return Packed256{}, err
	}
	if err := writer.writeBits(uint64(blackKing), 7); err != nil {
		return Packed256{}, err
	}
	if err := writer.writeBits(uint64(whiteKing), 7); err != nil {
		return Packed256{}, err
	}

	for sq := 0; sq < 81; sq++ {
		if sq == blackKing || sq == whiteKing {
			continue
		}
		piece := pieceAtIndex(pos, sq)
		if piece == nil {
			if err := writer.writeCode(boardCodeBook, "", false); err != nil {
				return Packed256{}, err
			}
			continue
		}
		if piece.kind == "K" {
			return Packed256{}, fmt.Errorf("unexpected king at square %d", sq)
		}
		if err := writer.writeCode(boardCodeBook, piece.kind, false); err != nil {
			return Packed256{}, err
		}
		if err := writer.writeColor(piece.color); err != nil {
			return Packed256{}, err
		}
		if isPromotable(piece.kind) {
			promoBit := uint64(0)
			if piece.promoted {
				promoBit = 1
			}
			if err := writer.writeBit(promoBit); err != nil {
				return Packed256{}, err
			}
		}
	}

	for _, color := range []Color{Black, White} {
		for _, kind := range []string{"P", "L", "N", "S", "G", "B", "R"} {
			count := pos.hands[color][kind]
			for i := 0; i < count; i++ {
				if err := writer.writeCode(handCodeBook, kind, true); err != nil {
					return Packed256{}, err
				}
				if err := writer.writeColor(color); err != nil {
					return Packed256{}, err
				}
				if isPromotable(kind) {
					if err := writer.writeBit(0); err != nil {
						return Packed256{}, err
					}
				}
			}
		}
	}

	if writer.pos != 256 {
		return Packed256{}, fmt.Errorf("packed length is %d bits, expected 256", writer.pos)
	}

	return Packed256{Words: writer.words}, nil
}

func UnpackPosition256(p Packed256) (Position, error) {
	reader := &bitReader256{words: p.Words}

	turnBit, err := reader.readBit()
	if err != nil {
		return Position{}, err
	}
	turn := Black
	if turnBit == 1 {
		turn = White
	}

	blackKing, err := reader.readBits(7)
	if err != nil {
		return Position{}, err
	}
	whiteKing, err := reader.readBits(7)
	if err != nil {
		return Position{}, err
	}
	if blackKing == whiteKing {
		return Position{}, fmt.Errorf("kings share square %d", blackKing)
	}

	pos := Position{
		board: [9][9]*Piece{},
		hands: map[Color]map[string]int{
			Black: {},
			White: {},
		},
		turn: turn,
	}
	setPieceAtIndex(&pos, int(blackKing), &Piece{kind: "K", color: Black})
	setPieceAtIndex(&pos, int(whiteKing), &Piece{kind: "K", color: White})

	for sq := 0; sq < 81; sq++ {
		if sq == int(blackKing) || sq == int(whiteKing) {
			continue
		}
		code, err := reader.readCode(boardCodeBook)
		if err != nil {
			return Position{}, err
		}
		if code.isEmpty {
			continue
		}
		color, err := reader.readColor()
		if err != nil {
			return Position{}, err
		}
		promoted := false
		if isPromotable(code.kind) {
			promoBit, err := reader.readBit()
			if err != nil {
				return Position{}, err
			}
			promoted = promoBit == 1
		}
		setPieceAtIndex(&pos, sq, &Piece{kind: code.kind, color: color, promoted: promoted})
	}

	for reader.pos < 256 {
		code, err := reader.readCode(handCodeBook)
		if err != nil {
			return Position{}, err
		}
		color, err := reader.readColor()
		if err != nil {
			return Position{}, err
		}
		if isPromotable(code.kind) {
			promoBit, err := reader.readBit()
			if err != nil {
				return Position{}, err
			}
			if promoBit != 0 {
				return Position{}, fmt.Errorf("promoted piece in hand: %s", code.kind)
			}
		}
		pos.hands[color][code.kind]++
	}

	return pos, nil
}

func buildCodeBook(codes []codeSpec) codeBook {
	book := codeBook{byLen: map[int]map[uint64]codeSpec{}}
	for _, code := range codes {
		if book.byLen[code.bitLen] == nil {
			book.byLen[code.bitLen] = map[uint64]codeSpec{}
		}
		book.byLen[code.bitLen][code.bits] = code
		if code.bitLen > book.maxLen {
			book.maxLen = code.bitLen
		}
	}
	return book
}

func (w *bitWriter256) writeBit(bit uint64) error {
	if w.pos >= 256 {
		return fmt.Errorf("bitstream overflow")
	}
	word := w.pos / 64
	offset := uint(w.pos % 64)
	if bit != 0 {
		w.words[word] |= 1 << offset
	}
	w.pos++
	return nil
}

func (w *bitWriter256) writeBits(value uint64, bitLen int) error {
	for i := 0; i < bitLen; i++ {
		bit := (value >> i) & 1
		if err := w.writeBit(bit); err != nil {
			return err
		}
	}
	return nil
}

func (w *bitWriter256) writeCode(book codeBook, kind string, isHand bool) error {
	code, ok := findCode(book, kind, isHand)
	if !ok {
		return fmt.Errorf("unknown piece code: %s", kind)
	}
	return w.writeBits(code.bits, code.bitLen)
}

func (w *bitWriter256) writeColor(color Color) error {
	bit := uint64(0)
	if color == White {
		bit = 1
	}
	return w.writeBit(bit)
}

func (r *bitReader256) readBit() (uint64, error) {
	if r.pos >= 256 {
		return 0, fmt.Errorf("bitstream underflow")
	}
	word := r.pos / 64
	offset := uint(r.pos % 64)
	bit := (r.words[word] >> offset) & 1
	r.pos++
	return bit, nil
}

func (r *bitReader256) readBits(bitLen int) (uint64, error) {
	var value uint64
	for i := 0; i < bitLen; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		value |= bit << i
	}
	return value, nil
}

func (r *bitReader256) readCode(book codeBook) (codeSpec, error) {
	var value uint64
	for length := 1; length <= book.maxLen; length++ {
		bit, err := r.readBit()
		if err != nil {
			return codeSpec{}, err
		}
		value |= bit << (length - 1)
		if entry, ok := book.byLen[length][value]; ok {
			return entry, nil
		}
	}
	return codeSpec{}, fmt.Errorf("invalid code")
}

func (r *bitReader256) readColor() (Color, error) {
	bit, err := r.readBit()
	if err != nil {
		return Black, err
	}
	if bit == 1 {
		return White, nil
	}
	return Black, nil
}

func findCode(book codeBook, kind string, isHand bool) (codeSpec, bool) {
	for _, entries := range book.byLen {
		for _, code := range entries {
			if code.kind == kind {
				return code, true
			}
			if !isHand && code.isEmpty && kind == "" {
				return code, true
			}
		}
	}
	return codeSpec{}, false
}

func kingSquares(pos Position) (int, int, error) {
	black := -1
	white := -1
	for r := 0; r < 9; r++ {
		for f := 0; f < 9; f++ {
			piece := pos.board[r][f]
			if piece == nil || piece.kind != "K" {
				continue
			}
			idx := r*9 + f
			if piece.color == Black {
				if black != -1 {
					return 0, 0, fmt.Errorf("multiple black kings")
				}
				black = idx
			} else {
				if white != -1 {
					return 0, 0, fmt.Errorf("multiple white kings")
				}
				white = idx
			}
		}
	}
	if black == -1 || white == -1 {
		return 0, 0, fmt.Errorf("missing king")
	}
	return black, white, nil
}

func pieceAtIndex(pos Position, idx int) *Piece {
	r := idx / 9
	f := idx % 9
	return pos.board[r][f]
}

func setPieceAtIndex(pos *Position, idx int, piece *Piece) {
	r := idx / 9
	f := idx % 9
	pos.board[r][f] = piece
}

func isPromotable(kind string) bool {
	switch kind {
	case "P", "L", "N", "S", "B", "R":
		return true
	default:
		return false
	}
}
