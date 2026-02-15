#!/usr/bin/env ruby
# frozen_string_literal: true

require 'bioshogi'
require 'json'

# Read KIF file with proper encoding (Shift_JIS or UTF-8)
def read_kif_file(path)
  # Try UTF-8 first
  begin
    content = File.read(path, encoding: 'UTF-8')
    return content if content.valid_encoding?
  rescue Encoding::InvalidByteSequenceError, Encoding::UndefinedConversionError
  end
  
  # Fall back to Shift_JIS
  begin
    return File.read(path, encoding: 'Shift_JIS:UTF-8')
  rescue Encoding::InvalidByteSequenceError, Encoding::UndefinedConversionError
  end
  
  # Last resort: force UTF-8 with replacement
  File.read(path, encoding: 'UTF-8', invalid: :replace, undef: :replace)
end

# Generate SFEN strings for all positions in a KIF file using bioshogi
def generate_sfens(kif_path)
  content = read_kif_file(kif_path)
  info = Bioshogi::Parser.parse(content)
  
  sfens = []
  
  # Create container and start from initial position
  container = Bioshogi::Container::Basic.start
  
  # Initial position (move 0)
  sfens << {
    move: 0,
    sfen: build_full_sfen(container, 1)
  }
  
  # Parse moves from info object
  pi = info.instance_variable_get(:@pi)
  move_infos = pi.instance_variable_get(:@move_infos)
  
  # Execute each move
  move_infos.each_with_index do |move_info, index|
    input_str = move_info[:input]
    container.execute(input_str)
    sfens << {
      move: index + 1,
      sfen: build_full_sfen(container, index + 2)
    }
  end
  
  sfens
rescue => e
  STDERR.puts "Error processing #{kif_path}: #{e.class}: #{e.message}"
  STDERR.puts e.backtrace.first(5).join("\n")
  []
end

# Build full SFEN with turn, hand pieces, and move number
def build_full_sfen(container, move_number)
  board_sfen = container.board.to_sfen
  
  # Determine turn: 'b' for black (sente), 'w' for white (gote)
  turn = container.current_player.location.key == :black ? 'b' : 'w'
  
  # Build hand pieces string in standard SFEN order
  # Order: rook, bishop, gold, silver, knight, lance, pawn
  # First black (uppercase), then white (lowercase)
  piece_order = [:rook, :bishop, :gold, :silver, :knight, :lance, :pawn]
  hand_parts = []
  
  [:black, :white].each do |location_key|
    player = container.player_at(location_key)
    pieces_hash = player.piece_box.to_h
    next if pieces_hash.empty?
    
    piece_order.each do |piece_key|
      count = pieces_hash[piece_key]
      next unless count && count > 0
      
      piece_char = piece_sfen_char(piece_key, location_key)
      if count > 1
        hand_parts << "#{count}#{piece_char}"
      else
        hand_parts << piece_char
      end
    end
  end
  
  hand_str = hand_parts.empty? ? '-' : hand_parts.join
  
  "#{board_sfen} #{turn} #{hand_str} #{move_number}"
end

# Convert piece key to SFEN character
def piece_sfen_char(piece_key, location)
  chars = {
    pawn: 'P', lance: 'L', knight: 'N', silver: 'S',
    gold: 'G', bishop: 'B', rook: 'R', king: 'K'
  }
  char = chars[piece_key] || piece_key.to_s[0].upcase
  location == :white ? char.downcase : char
end

if ARGV.empty?
  STDERR.puts "Usage: #{$0} <kif_file>"
  exit 1
end

kif_path = ARGV[0]
unless File.exist?(kif_path)
  STDERR.puts "File not found: #{kif_path}"
  exit 1
end

sfens = generate_sfens(kif_path)
puts JSON.generate(sfens)
