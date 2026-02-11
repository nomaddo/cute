package test

import (
	"path/filepath"
	"testing"

	cute "cute/src"
)

func TestKIFToSFENInitial(t *testing.T) {
	path := filepath.Join("testdata", "initial.kif")
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		t.Fatalf("failed to load board: %v", err)
	}
	sfen, err := board.SFENAt(0)
	if err != nil {
		t.Fatalf("failed to build sfen: %v", err)
	}
	want := "lnsgkgsnl/1r5b1/ppppppppp/9/9/9/PPPPPPPPP/1B5R1/LNSGKGSNL b - 1"
	if sfen != want {
		t.Fatalf("unexpected sfen: got %s want %s", sfen, want)
	}
}

func TestKIFToSFENBasicAigakari(t *testing.T) {
	path := filepath.Join("testdata", "basic_aigakari.kif")
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		t.Fatalf("failed to load board: %v", err)
	}

	expectedSFENs := []string {
		"lnsgkgsnl/1r5b1/ppppppppp/9/9/9/PPPPPPPPP/1B5R1/LNSGKGSNL b - 1",
		"lnsgkgsnl/1r5b1/ppppppppp/9/9/7P1/PPPPPPP1P/1B5R1/LNSGKGSNL w - 2",
		"lnsgkgsnl/1r5b1/p1ppppppp/1p7/9/7P1/PPPPPPP1P/1B5R1/LNSGKGSNL b - 3",
		"lnsgkgsnl/1r5b1/p1ppppppp/1p7/7P1/9/PPPPPPP1P/1B5R1/LNSGKGSNL w - 4",
		"lnsgkgsnl/1r5b1/p1ppppppp/9/1p5P1/9/PPPPPPP1P/1B5R1/LNSGKGSNL b - 5",
		"lnsgkgsnl/1r5b1/p1ppppppp/9/1p5P1/9/PPPPPPP1P/1BG4R1/LNS1KGSNL w - 6",
		"lnsgk1snl/1r4gb1/p1ppppppp/9/1p5P1/9/PPPPPPP1P/1BG4R1/LNS1KGSNL b - 7",
		"lnsgk1snl/1r4gb1/p1ppppppp/7P1/1p7/9/PPPPPPP1P/1BG4R1/LNS1KGSNL w - 8",
		"lnsgk1snl/1r4gb1/p1ppppp1p/7p1/1p7/9/PPPPPPP1P/1BG4R1/LNS1KGSNL b p 9",
		"lnsgk1snl/1r4gb1/p1ppppp1p/7R1/1p7/9/PPPPPPP1P/1BG6/LNS1KGSNL w Pp 10",
		"lnsg2snl/1r2k1gb1/p1ppppp1p/7R1/1p7/9/PPPPPPP1P/1BG6/LNS1KGSNL b Pp 11",
		"lnsg2snl/1r2k1g+R1/p1ppppp1p/9/1p7/9/PPPPPPP1P/1BG6/LNS1KGSNL w BPp 12",
		"lnsg3nl/1r2k1gs1/p1ppppp1p/9/1p7/9/PPPPPPP1P/1BG6/LNS1KGSNL b BPrp 13",
	}

	for i, want := range expectedSFENs {
		sfen, err := board.SFENAt(i)
		if err != nil {
			t.Fatalf("failed to build sfen at move %d: %v", i, err)
		}
		if sfen != want {
			t.Fatalf("unexpected sfen at move %d: got %s want %s", i, sfen, want)
		}
	}
}
