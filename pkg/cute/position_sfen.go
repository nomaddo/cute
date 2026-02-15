package cute

// PositionFromSFEN parses an SFEN string into a Position.
func PositionFromSFEN(sfen string) (Position, error) {
	return parseSFENPosition(sfen)
}
