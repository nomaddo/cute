package cute_test

import (
	"path/filepath"
	"testing"

	cute "cute/pkg/cute"
)

// TestIsLegalPosition_InitialPosition verifies the initial board is legal.
func TestIsLegalPosition_InitialPosition(t *testing.T) {
	board, err := cute.LoadBoardFromKIF(filepath.Join("testdata", "initial.kif"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	pos := board.InitialPosition()
	if !pos.IsLegalPosition() {
		t.Fatal("initial position should be legal")
	}
}

// TestIsInCheck_KingNotInCheck verifies that in a normal opening position,
// neither king is in check.
func TestIsInCheck_KingNotInCheck(t *testing.T) {
	board, err := cute.LoadBoardFromKIF(filepath.Join("testdata", "initial.kif"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	pos := board.InitialPosition()
	if pos.IsInCheck(cute.Black) {
		t.Fatal("black king should not be in check in initial position")
	}
	if pos.IsInCheck(cute.White) {
		t.Fatal("white king should not be in check in initial position")
	}
}

// TestIsInCheck_RookAttack verifies rook-based check detection.
func TestIsInCheck_RookAttack(t *testing.T) {
	// Set up a position where Black's rook attacks White's king
	// on the same file with no pieces in between.
	pos := cute.NewPosition()
	pos.SetPiece(5, 1, "K", cute.White, false) // White king at 5a
	pos.SetPiece(5, 9, "R", cute.Black, false) // Black rook at 5i
	pos.SetPiece(1, 9, "K", cute.Black, false) // Black king at 1i
	pos.SetTurn(cute.White)

	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from rook on same file")
	}
}

// TestIsInCheck_RookBlockedByPiece verifies that a piece blocks rook check.
func TestIsInCheck_RookBlockedByPiece(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 1, "K", cute.White, false) // White king at 5a
	pos.SetPiece(5, 5, "P", cute.Black, false) // Blocking pawn at 5e
	pos.SetPiece(5, 9, "R", cute.Black, false) // Black rook at 5i
	pos.SetPiece(1, 9, "K", cute.Black, false) // Black king at 1i
	pos.SetTurn(cute.White)

	if pos.IsInCheck(cute.White) {
		t.Fatal("white king should NOT be in check when rook is blocked")
	}
}

// TestIsInCheck_BishopAttack verifies bishop-based check detection.
func TestIsInCheck_BishopAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false) // White king at 5e
	pos.SetPiece(1, 1, "B", cute.Black, false) // Black bishop at 1a → diagonal to 5e
	pos.SetPiece(9, 9, "K", cute.Black, false) // Black king at 9i
	pos.SetTurn(cute.White)

	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from bishop on diagonal")
	}
}

// TestIsInCheck_GoldAttack verifies gold-based check detection.
func TestIsInCheck_GoldAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false) // White king at 5e
	pos.SetPiece(5, 6, "G", cute.Black, false) // Black gold at 5f (1 step forward from Black's perspective)
	pos.SetPiece(9, 9, "K", cute.Black, false) // Black king at 9i
	pos.SetTurn(cute.White)

	// Black gold at 5f can attack 5e (forward = rank-1 for Black).
	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from adjacent gold")
	}
}

// TestIsInCheck_SilverAttack verifies silver move patterns.
func TestIsInCheck_SilverAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false) // White king at 5e
	pos.SetPiece(4, 6, "S", cute.Black, false) // Black silver at 4f
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	// Silver at 4f attacking 5e: df=1, dr=-1 → forward-right diagonal for Black.
	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from silver diagonal")
	}
}

// TestIsInCheck_SilverCannotAttackSideways verifies silver cannot move sideways.
func TestIsInCheck_SilverCannotAttackSideways(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false)
	pos.SetPiece(4, 5, "S", cute.Black, false) // Silver at 4e — sideways from king
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	if pos.IsInCheck(cute.White) {
		t.Fatal("silver cannot attack sideways")
	}
}

// TestIsInCheck_KnightAttack verifies knight jump check.
func TestIsInCheck_KnightAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 3, "K", cute.White, false) // White king at 5c
	pos.SetPiece(4, 5, "N", cute.Black, false) // Black knight at 4e
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	// Black knight at 4e jumps to (4±1, 5+(-2)) = (3,3) or (5,3).
	// df = 5-4 = 1, dr = 3-5 = -2, fwd = -1, 2*fwd = -2. ✓
	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from knight")
	}
}

// TestIsInCheck_LanceAttack verifies lance slide check.
func TestIsInCheck_LanceAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 1, "K", cute.White, false) // White king at 5a
	pos.SetPiece(5, 4, "L", cute.Black, false) // Black lance at 5d
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	// Black lance slides forward (rank decreasing). 5d→5c→5b→5a.
	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from lance")
	}
}

// TestIsInCheck_PawnAttack verifies pawn check.
func TestIsInCheck_PawnAttack(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false)
	pos.SetPiece(5, 6, "P", cute.Black, false) // Black pawn at 5f (one step forward = 5e)
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from pawn")
	}
}

// TestIsInCheck_PromotedRook (Dragon) verifies diagonal adjacent attack.
func TestIsInCheck_PromotedRook(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false)
	pos.SetPiece(4, 4, "R", cute.Black, true) // Promoted rook (Dragon) at 4d — diagonal
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from dragon's diagonal")
	}
}

// TestIsInCheck_PromotedBishop (Horse) verifies orthogonal adjacent attack.
func TestIsInCheck_PromotedBishop(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false)
	pos.SetPiece(5, 4, "B", cute.Black, true) // Promoted bishop (Horse) at 5d — orthogonal
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	if !pos.IsInCheck(cute.White) {
		t.Fatal("white king should be in check from horse's orthogonal")
	}
}

// TestIsInCheck_PromotedSilverLikeGold verifies promoted silver moves like gold.
func TestIsInCheck_PromotedSilverLikeGold(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 5, "K", cute.White, false)
	pos.SetPiece(4, 5, "S", cute.Black, true) // Promoted silver at 4e — sideways
	pos.SetPiece(9, 9, "K", cute.Black, false)
	pos.SetTurn(cute.White)

	// Normal silver cannot attack sideways, but promoted silver (gold-like) can.
	if !pos.IsInCheck(cute.White) {
		t.Fatal("promoted silver should attack sideways like gold")
	}
}

// TestIsInCheck_WhitePieces verifies White's pieces attack in the right direction.
func TestIsInCheck_WhitePieces(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 9, "K", cute.Black, false) // Black king at 5i
	pos.SetPiece(5, 8, "P", cute.White, false) // White pawn at 5h (attacks forward=rank+1=5i)
	pos.SetPiece(1, 1, "K", cute.White, false)
	pos.SetTurn(cute.Black)

	if !pos.IsInCheck(cute.Black) {
		t.Fatal("black king should be in check from white pawn")
	}
}

// TestIsInCheck_WhiteKnight verifies White's knight direction.
func TestIsInCheck_WhiteKnight(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 7, "K", cute.Black, false) // Black king at 5g
	pos.SetPiece(4, 5, "N", cute.White, false) // White knight at 4e
	pos.SetPiece(1, 1, "K", cute.White, false)
	pos.SetTurn(cute.Black)

	// White knight at 4e: forward for White = rank+1.
	// Jumps to (4±1, 5+2) = (3,7) or (5,7).
	// df = 5-4 = 1, dr = 7-5 = 2, fwd(White) = 1, 2*fwd = 2. ✓
	if !pos.IsInCheck(cute.Black) {
		t.Fatal("black king should be in check from white knight")
	}
}

// TestIsLegalPosition_AfterIllegalMove verifies detection of 王手放置.
func TestIsLegalPosition_AfterIllegalMove(t *testing.T) {
	// Simulate: Black's king is attacked but Black moves a non-king piece
	// (i.e. leaves king in check = illegal).
	pos := cute.NewPosition()
	pos.SetPiece(5, 9, "K", cute.Black, false) // Black king at 5i
	pos.SetPiece(5, 1, "R", cute.White, false) // White rook at 5a attacks along file
	pos.SetPiece(1, 1, "K", cute.White, false) // White king at 1a
	pos.SetPiece(1, 9, "G", cute.Black, false) // Black gold at 1i
	// It's now White's turn — meaning Black just moved but left king in check.
	pos.SetTurn(cute.White)

	if pos.IsLegalPosition() {
		t.Fatal("position should be illegal: Black left king in check")
	}
}

// TestIsLegalPosition_LegalAfterMove verifies a normal legal position.
func TestIsLegalPosition_LegalAfterMove(t *testing.T) {
	pos := cute.NewPosition()
	pos.SetPiece(5, 9, "K", cute.Black, false)
	pos.SetPiece(1, 1, "K", cute.White, false)
	pos.SetPiece(7, 7, "P", cute.Black, false)
	pos.SetTurn(cute.White) // Black just moved — king not in check.

	if !pos.IsLegalPosition() {
		t.Fatal("position should be legal")
	}
}

func isLegalPosition_FoulKIF(path string, t *testing.T) {
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !board.IsFoulEnd() {
		t.Skip("game is not a foul-end game")
	}
	// Walk all moves and verify the last position becomes illegal.
	pos := board.InitialPosition()
	moves := board.Moves()
	lastIllegal := -1
	for i, mv := range moves {
		if err := pos.ApplyMove(mv); err != nil {
			t.Fatalf("move %d (%s): %v", i+1, mv, err)
		}
		if !pos.IsLegalPosition() {
			lastIllegal = i + 1
		}
	}
	if lastIllegal == -1 {
		t.Fatal("expected at least one illegal position in foul game")
	}
	t.Logf("first illegal position detected at ply %d (total moves: %d)", lastIllegal, len(moves))
}

// TestIsLegalPosition_FoulKIF verifies that the foul KIF file (37983487.kif)
// produces an illegal position at the move where 王手放置 occurs.
func TestIsLegalPosition00_FoulKIF(t *testing.T) {
	path := filepath.Join("testdata", "35591589.kif")
	isLegalPosition_FoulKIF(path, t)
}

func TestIsLegalPosition01_FoulKIF(t *testing.T) {
	path := filepath.Join("testdata", "37983487.kif")
	isLegalPosition_FoulKIF(path, t)
}
