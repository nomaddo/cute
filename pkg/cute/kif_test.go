package cute_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cute "cute/pkg/cute"
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
	assertPackRoundTrip(t, want)
}

func TestKIFToSFENBasicAigakari(t *testing.T) {
	path := filepath.Join("testdata", "basic_aigakari.kif")
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		t.Fatalf("failed to load board: %v", err)
	}

	expectedSFENs := []string{
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
		assertPackRoundTrip(t, want)
	}
}

func TestKIFToSFENReal(t *testing.T) {
	path := filepath.Join("testdata", "real.kif")
	board, err := cute.LoadBoardFromKIF(path)
	if err != nil {
		t.Fatalf("failed to load board: %v", err)
	}

	expectedSFENs := []string{
		"lnsgkgsnl/1r5b1/ppppppppp/9/9/9/PPPPPPPPP/1B5R1/LNSGKGSNL b - 1",
		"lnsgkgsnl/1r5b1/ppppppppp/9/9/2P6/PP1PPPPPP/1B5R1/LNSGKGSNL w - 2",
		"lnsgkgsnl/1r5b1/pppppp1pp/6p2/9/2P6/PP1PPPPPP/1B5R1/LNSGKGSNL b - 3",
		"lnsgkgsnl/1r5b1/pppppp1pp/6p2/9/2P6/PP1PPPPPP/1BG4R1/LNS1KGSNL w - 4",
		"lnsgkgsnl/1r5b1/ppppp2pp/5pp2/9/2P6/PP1PPPPPP/1BG4R1/LNS1KGSNL b - 5",
		"lnsgkgsnl/1r5b1/ppppp2pp/5pp2/9/2P6/PP1PPPPPP/1BGS3R1/LN2KGSNL w - 6",
		"lnsgkg1nl/1r3s1b1/ppppp2pp/5pp2/9/2P6/PP1PPPPPP/1BGS3R1/LN2KGSNL b - 7",
		"lnsgkg1nl/1r3s1b1/ppppp2pp/5pp2/9/2PP5/PP2PPPPP/1BGS3R1/LN2KGSNL w - 8",
		"lnsgkg1nl/1r5b1/ppppps1pp/5pp2/9/2PP5/PP2PPPPP/1BGS3R1/LN2KGSNL b - 9",
		"lnsgkg1nl/1r5b1/ppppps1pp/5pp2/9/2PP5/PP1SPPPPP/1BG4R1/LN2KGSNL w - 10",
		"lnsgkg1nl/1r5b1/ppppp2pp/4spp2/9/2PP5/PP1SPPPPP/1BG4R1/LN2KGSNL b - 11",
		"lnsgkg1nl/1r5b1/ppppp2pp/4spp2/9/2PPS4/PP2PPPPP/1BG4R1/LN2KGSNL w - 12",
		"lnsgkg1nl/1r5b1/ppppp2pp/4s1p2/5p3/2PPS4/PP2PPPPP/1BG4R1/LN2KGSNL b - 13",
		"lnsgkg1nl/1r5b1/ppppp2pp/4s1p2/5p3/2PPS4/PP2PPPPP/1BGR5/LN2KGSNL w - 14",
		"lnsgkg1nl/5r1b1/ppppp2pp/4s1p2/5p3/2PPS4/PP2PPPPP/1BGR5/LN2KGSNL b - 15",
		"lnsgkg1nl/5r1b1/ppppp2pp/4s1p2/5p3/2PPS4/PP2PPPPP/1BGR1G3/LN2K1SNL w - 16",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1pp1/5p3/2PPS4/PP2PPPPP/1BGR1G3/LN2K1SNL b - 17",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1pp1/5p3/2PPS4/PPB1PPPPP/2GR1G3/LN2K1SNL w - 18",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/5p1p1/2PPS4/PPB1PPPPP/2GR1G3/LN2K1SNL b - 19",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/5p1p1/2PPS4/PPB1PPPPP/2GR1G1S1/LN2K2NL w - 20",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/5p3/2PPS2p1/PPB1PPPPP/2GR1G1S1/LN2K2NL b - 21",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/5p3/2PPS2P1/PPB1PPP1P/2GR1G1S1/LN2K2NL w P 22",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/9/2PPSp1P1/PPB1PPP1P/2GR1G1S1/LN2K2NL b P 23",
		"lnsgkg1nl/5r1b1/ppppp3p/4s1p2/9/2PPSP1P1/PPB1P1P1P/2GR1G1S1/LN2K2NL w 2P 24",
		"lnsgkg1nl/7b1/ppppp3p/4s1p2/9/2PPSr1P1/PPB1P1P1P/2GR1G1S1/LN2K2NL b 2Pp 25",
		"lnsgkg1nl/7b1/ppppp3p/4s1p2/9/2PPSr1P1/PPB1PPP1P/2GR1G1S1/LN2K2NL w Pp 26",
		"lnsgkg1nl/7b1/ppppp3p/4s1p2/9/2PPS2r1/PPB1PPP1P/2GR1G1S1/LN2K2NL b P2p 27",
		"lnsgkg1nl/7b1/ppppp3p/4s1p2/9/2PPS2r1/PPB1PPPPP/2GR1G1S1/LN2K2NL w 2p 28",
		"lnsgkg1nl/7b1/ppppp2rp/4s1p2/9/2PPS4/PPB1PPPPP/2GR1G1S1/LN2K2NL b 2p 29",
		"lnsgkg1nl/7b1/ppppp2rp/4s1p2/3P5/2P1S4/PPB1PPPPP/2GR1G1S1/LN2K2NL w 2p 30",
		"lns1kg1nl/3g3b1/ppppp2rp/4s1p2/3P5/2P1S4/PPB1PPPPP/2GR1G1S1/LN2K2NL b 2p 31",
		"lns1kg1nl/3g3b1/ppppp2rp/3Ps1p2/9/2P1S4/PPB1PPPPP/2GR1G1S1/LN2K2NL w 2p 32",
		"lns1kg1nl/3g3b1/ppp1p2rp/3ps1p2/9/2P1S4/PPB1PPPPP/2GR1G1S1/LN2K2NL b 3p 33",
		"lns1kg1nl/3g3b1/ppp1p2rp/3Rs1p2/9/2P1S4/PPB1PPPPP/2G2G1S1/LN2K2NL w P3p 34",
		"lns1kg1nl/3g3b1/ppppp2rp/3Rs1p2/9/2P1S4/PPB1PPPPP/2G2G1S1/LN2K2NL b P2p 35",
		"lns1kg1nl/3g3b1/ppppp2rp/4s1p2/9/2P1S4/PPB1PPPPP/2G2G1S1/LN1RK2NL w P2p 36",
		"lnsk1g1nl/3g3b1/ppppp2rp/4s1p2/9/2P1S4/PPB1PPPPP/2G2G1S1/LN1RK2NL b P2p 37",
		"lnsk1g1nl/3g3b1/ppppp2rp/4s1p2/2P6/4S4/PPB1PPPPP/2G2G1S1/LN1RK2NL w P2p 38",
		"lns2g1nl/2kg3b1/ppppp2rp/4s1p2/2P6/4S4/PPB1PPPPP/2G2G1S1/LN1RK2NL b P2p 39",
		"lns2g1nl/2kg3b1/ppppp2rp/4s1p2/2P6/P3S4/1PB1PPPPP/2G2G1S1/LN1RK2NL w P2p 40",
		"ln3g1nl/1skg3b1/ppppp2rp/4s1p2/2P6/P3S4/1PB1PPPPP/2G2G1S1/LN1RK2NL b P2p 41",
		"ln3g1nl/1skg3b1/ppppp2rp/4s1p2/P1P6/4S4/1PB1PPPPP/2G2G1S1/LN1RK2NL w P2p 42",
		"ln3g1nl/1skg3b1/ppppp2r1/4s1p1p/P1P6/4S4/1PB1PPPPP/2G2G1S1/LN1RK2NL b P2p 43",
		"ln3g1nl/1skg3b1/ppppp2r1/4s1p1p/P1P6/1P2S4/2B1PPPPP/2G2G1S1/LN1RK2NL w P2p 44",
		"ln3g1nl/1skg3b1/ppppp2r1/4s1p2/P1P5p/1P2S4/2B1PPPPP/2G2G1S1/LN1RK2NL b P2p 45",
		"ln3g1nl/1skg3b1/ppppp2r1/4s1p2/PPP5p/4S4/2B1PPPPP/2G2G1S1/LN1RK2NL w P2p 46",
		"ln3g1nl/1skg5/ppppp2r1/4s1p2/PPP5p/4S4/2+b1PPPPP/2G2G1S1/LN1RK2NL b Pb2p 47",
		"ln3g1nl/1skg5/ppppp2r1/4s1p2/PPP5p/4S4/2N1PPPPP/2G2G1S1/L2RK2NL w BPb2p 48",
		"ln3g1nl/1skg5/ppppp2r1/4sbp2/PPP5p/4S4/2N1PPPPP/2G2G1S1/L2RK2NL b BP2p 49",
		"ln3g1nl/1skg5/ppppp2r1/4sbp2/PPP5p/4S4/2N1PPPPP/2G2G1S1/L2R1K1NL w BP2p 50",
		"ln3g2l/1skg5/ppppp1nr1/4sbp2/PPP5p/4S4/2N1PPPPP/2G2G1S1/L2R1K1NL b BP2p 51",
		"ln3g2l/1skg5/ppppp1nr1/4sbp2/PPP5p/4S4/2N1PPPPP/2G2GKS1/L2R3NL w BP2p 52",
		"ln3g2l/1skg5/ppppp1nr1/4sbp2/PPP6/4S3p/2N1PPPPP/2G2GKS1/L2R3NL b BP2p 53",
		"ln3g2l/1skg5/ppppp1nr1/4sbp2/PPP6/4S3P/2N1PPPP1/2G2GKS1/L2R3NL w B2P2p 54",
		"ln3g2l/1skg5/ppppp2r1/4sbp2/PPP4n1/4S3P/2N1PPPP1/2G2GKS1/L2R3NL b B2P2p 55",
		"ln3g2l/1skg5/ppppp2r1/1P2sbp2/P1P4n1/4S3P/2N1PPPP1/2G2GKS1/L2R3NL w B2P2p 56",
		"ln3g2l/1skg5/p1ppp2r1/1p2sbp2/P1P4n1/4S3P/2N1PPPP1/2G2GKS1/L2R3NL b B2P3p 57",
		"ln3g2l/1skg5/p1ppp2r1/1pP1sbp2/P6n1/4S3P/2N1PPPP1/2G2GKS1/L2R3NL w B2P3p 58",
		"ln3g2l/1skg5/p2pp2r1/1pp1sbp2/P6n1/4S3P/2N1PPPP1/2G2GKS1/L2R3NL b B2P4p 59",
		"ln3g2l/1skg5/p2pp2r1/1pp1sbp2/P2S3n1/8P/2N1PPPP1/2G2GKS1/L2R3NL w B2P4p 60",
		"ln3g2l/1skg5/p2pp2r1/1pp2bp2/P2s3n1/8P/2N1PPPP1/2G2GKS1/L2R3NL b B2Ps4p 61",
		"ln3g2l/1skg5/p2pp2r1/1pp2bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL w BS2Ps4p 62",
		"ln3g2l/1skg5/p2pp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL b BS2P4p 63",
		"ln3g2l/1skg5/p1Ppp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL w BSP4p 64",
		"l4g2l/1skg5/p1npp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL b BSP5p 65",
		"l4g2l/1skg5/p1+Npp2r1/1pps1bp2/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL w BSNP5p 66",
		"l4g2l/2kg5/p1spp2r1/1pps1bp2/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL b BSNPn5p 67",
		"l4g2l/2kg5/p1spp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL w BSPn5p 68",
		"l4g2l/1skg5/p2pp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL b BSPn5p 69",
		"l4g2l/1skg5/p1Ppp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL w BSn5p 70",
		"l4g2l/2kg5/p1spp2r1/1pps1bp2/P2N3n1/8P/4PPPP1/2G2GKS1/L2R3NL b BSn6p 71",
		"l4g2l/2kg5/p1+Npp2r1/1pps1bp2/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL w B2Sn6p 72",
		"l4g2l/2kg5/p1spp2r1/1pp2bp2/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL b B2S2n6p 73",
		"l4g2l/2kg5/p1spp2r1/1pp2bp2/P4S1n1/8P/4PPPP1/2G2GKS1/L2R3NL w BS2n6p 74",
		"l4g2l/2kg3b1/p1spp2r1/1pp3p2/P4S1n1/8P/4PPPP1/2G2GKS1/L2R3NL b BS2n6p 75",
		"l4g2l/2kg3b1/p1spp2r1/1pp3S2/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL w BSP2n6p 76",
		"l4g2l/2kg3b1/p1spp4/1pp3Sr1/P6n1/8P/4PPPP1/2G2GKS1/L2R3NL b BSP2n6p 77",
		"l4g2l/2kg3b1/p1spp4/1pp3Sr1/P5Sn1/8P/4PPPP1/2G2GKS1/L2R3NL w BP2n6p 78",
		"l4g2l/2kg3b1/p1spp4/1pp3S1r/P5Sn1/8P/4PPPP1/2G2GKS1/L2R3NL b BP2n6p 79",
		"l4g2l/2kg3b1/p1spp2S1/1pp5r/P5Sn1/8P/4PPPP1/2G2GKS1/L2R3NL w BP2n6p 80",
		"l4g2l/2kg3b1/p1spp2S1/1pp5r/P5S2/8P/4PP+nP1/2G2GKS1/L2R3NL b BP2n7p 81",
		"l4g2l/2kg3b1/p1spp2S1/1pp5r/P5S2/8P/4PPSP1/2G2GK2/L2R3NL w BNP2n7p 82",
		"l4g2l/2kg3b1/p1spp2S1/1pp5r/P5S2/6p1P/4PPSP1/2G2GK2/L2R3NL b BNP2n6p 83",
		"l4g2l/2kg3b1/p1spp2S1/1pp5r/P5S2/6S1P/4PP1P1/2G2GK2/L2R3NL w BN2P2n6p 84",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b1S2/6S1P/4PP1P1/2G2GK2/L2R3NL b BN2P2n6p 85",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b4/5SS1P/4PP1P1/2G2GK2/L2R3NL w BN2P2n6p 86",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b4/5SS1P/4PPpP1/2G2GK2/L2R3NL b BN2P2n5p 87",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b4/5SS1P/4PPpP1/2G2G3/L2R1K1NL w BN2P2n5p 88",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b4/1n3SS1P/4PPpP1/2G2G3/L2R1K1NL b BN2Pn5p 89",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P3b4/1n3SS1P/4PPpP1/5G3/L1GR1K1NL w BN2Pn5p 90",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P8/1n3bS1P/4PPpP1/5G3/L1GR1K1NL b BN2Psn5p 91",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P8/1n3PS1P/4P1pP1/5G3/L1GR1K1NL w 2BN2Psn5p 92",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P8/1n3PS1P/4P1pP1/2s2G3/L1GR1K1NL b 2BN2Pn5p 93",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P8/1n3PS1P/4P1pP1/2G2G3/L2R1K1NL w 2BSN2Pn5p 94",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P8/5PS1P/4P1pP1/2+n2G3/L2R1K1NL b 2BSN2Pgn5p 95",
		"l4g2l/2kg5/p1spp2S1/1pp5r/P2R5/5PS1P/4P1pP1/2+n2G3/L4K1NL w 2BSN2Pgn5p 96",
		"l4g2l/2kg5/p1spp2S1/1ppr5/P2R5/5PS1P/4P1pP1/2+n2G3/L4K1NL b 2BSN2Pgn5p 97",
		"l4g2l/2kg5/p1spp2S1/1ppR5/P8/5PS1P/4P1pP1/2+n2G3/L4K1NL w R2BSN2Pgn5p 98",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1pP1/2+n2G3/L4K1NL b R2BSN2Prgn5p 99",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2G3/L4K2L w R2BSN3Prgn5p 100",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2G3/L4K1rL b R2BSN3Pgn5p 101",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2G3/L4KRrL w 2BSN3Pgn5p 102",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2G1+r1/L4KR1L b 2BSN3Pgn5p 103",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n3G+r1/L4KR1L w 2BSN3Pgn5p 104",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2pG+r1/L4KR1L b 2BSN3Pgn4p 105",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n2KG+r1/L5R1L w 2BSN4Pgn4p 106",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n1gKG+r1/L5R1L b 2BSN4Pn4p 107",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n1K1G+r1/L5R1L w 2BGSN4Pn4p 108",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n1K1G2/L5+r1L b 2BGSN4Prn4p 109",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+n1K4/L5G1L w R2BGSN4Prn4p 110",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4P1NP1/2+nrK4/L5G1L b R2BGSN4Pn4p 111",
		"l4g2l/2kg5/p2pp2S1/1pps5/P8/5PS1P/4PKNP1/2+nr5/L5G1L w R2BGSN4Pn4p 112",
		"l4g2l/2kg5/p2pp2S1/1pps5/P3n4/5PS1P/4PKNP1/2+nr5/L5G1L b R2BGSN4P4p 113",
		"l4g2l/2kg5/p2pp2S1/1pps5/P3n4/4KPS1P/4P1NP1/2+nr5/L5G1L w R2BGSN4P4p 114",
		"l4g2l/2kg5/p2pp2S1/1pps5/P2+rn4/4KPS1P/4P1NP1/2+n6/L5G1L b R2BGSN4P4p 115",
		"l4g2l/2kg5/p2pp2S1/1pps5/P2+rnK3/5PS1P/4P1NP1/2+n6/L5G1L w R2BGSN4P4p 116",
		"l4g2l/2kg5/p2pp2S1/1pps5/P2+r1K3/5PS1P/4P+nNP1/2+n6/L5G1L b R2BGSN4P4p 117",
		"l4g2l/2kg5/p2pp2S1/1pps2K2/P2+r5/5PS1P/4P+nNP1/2+n6/L5G1L w R2BGSN4P4p 118",
		"l4g2l/2kg5/p2pp2S1/1pps2K2/P2+r5/5PS1P/4P1+nP1/2+n6/L5G1L b R2BGSN4Pn4p 119",
		"l4g2l/2kg5/p2pp2S1/1pps2K2/P2+r1S3/5P2P/4P1+nP1/2+n6/L5G1L w R2BGSN4Pn4p 120",
		"l4g2l/2kg5/p2pp2S1/1pps2K2/P2+r1S3/5P+n1P/4P2P1/2+n6/L5G1L b R2BGSN4Pn4p 121",
		"l4g2l/2kg5/pN1pp2S1/1pps2K2/P2+r1S3/5P+n1P/4P2P1/2+n6/L5G1L w R2BGS4Pn4p 122",
	}

	for i, want := range expectedSFENs {
		sfen, err := board.SFENAt(i)
		if err != nil {
			t.Fatalf("failed to build sfen at move %d: %v", i, err)
		}
		if sfen != want {
			t.Fatalf("unexpected sfen at move %d: got %s want %s", i, sfen, want)
		}
		assertPackRoundTrip(t, want)
	}
}

func assertPackRoundTrip(t *testing.T, sfen string) {
	t.Helper()

	pos, err := cute.PositionFromSFEN(sfen)
	if err != nil {
		t.Fatalf("failed to parse sfen: %v", err)
	}
	packed, err := cute.PackPosition256(pos)
	if err != nil {
		t.Fatalf("failed to pack sfen: %v", err)
	}
	unpacked, err := cute.UnpackPosition256(packed)
	if err != nil {
		t.Fatalf("failed to unpack sfen: %v", err)
	}
	moveNumber := parseMoveNumber(sfen)
	got := unpacked.ToSFEN(moveNumber)
	if got != sfen {
		t.Fatalf("pack/unpack mismatch: got %s want %s", got, sfen)
	}
}

func parseMoveNumber(sfen string) int {
	fields := strings.Fields(sfen)
	if len(fields) >= 4 {
		if move, err := strconv.Atoi(fields[3]); err == nil {
			return move
		}
	}
	return 1
}

func TestBuildGameRecordEvaluatesTestKIFs(t *testing.T) {
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

	testDir := filepath.Join(repoRoot, "test")
	files, err := cute.CollectKIF(testDir)
	if err != nil {
		t.Fatalf("failed to collect kifs: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no .kif files found in test dir")
	}

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

	cache := make(map[string]cute.Score)
	moveTimeMs := 1
	for _, path := range files {
		record, err := cute.BuildGameRecord(ctx, path, session, moveTimeMs, cache)
		if err != nil {
			t.Fatalf("failed to build game record for %s: %v", path, err)
		}
		if record.MoveCount == 0 {
			t.Fatalf("empty record for %s", path)
		}
		if int32(len(record.MoveEvals)) != record.MoveCount {
			t.Fatalf("move eval count mismatch for %s: got %d want %d", path, len(record.MoveEvals), record.MoveCount)
		}
		for _, eval := range record.MoveEvals {
			if eval.ScoreType != "cp" && eval.ScoreType != "mate" {
				t.Fatalf("unexpected score type for %s: %s", path, eval.ScoreType)
			}
		}
	}

	// Ensure the engine is still responsive after processing all games.
	if _, _, err := session.Evaluate(ctx, "lnsgkgsnl/1r5b1/ppppppppp/9/9/9/PPPPPPPPP/1B5R1/LNSGKGSNL b - 1", moveTimeMs); err != nil {
		t.Fatalf("engine stopped after evaluations: %v", err)
	}
}
