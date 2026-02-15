#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "pathname"
require "nkf"
require "etc"
require "bioshogi"
require "parquet"

COLUMNS = %w[
  game_id
  game_type
  sente_name
  sente_rating
  gote_name
  gote_rating
  turn_max
  sente_attack_tags
  sente_defense_tags
  sente_technique_tags
  sente_note_tags
  gote_attack_tags
  gote_defense_tags
  gote_technique_tags
  gote_note_tags
].freeze

options = {
  dir: nil,
  glob: "**/*.kif",
  output: "out/kif_tags.parquet",
  limit: 0,
  jobs: Etc.nprocessors,
  dry_run: false,
  skip_existing: true,
  verbose: false,
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby tools/classify_kif_to_db.rb [options] [kif_paths...]"

  opts.on("--dir DIR", "Directory containing KIF files") do |v|
    options[:dir] = v
  end
  opts.on("--glob GLOB", "Glob pattern under --dir (default: **/*.kif)") do |v|
    options[:glob] = v
  end
  opts.on("--output FILE", "Parquet output path (default: out/kif_tags.parquet)") do |v|
    options[:output] = v
  end
  opts.on("--limit N", Integer, "Limit number of files processed (0=disabled)") do |v|
    options[:limit] = v
  end
  opts.on("--jobs N", Integer, "Number of worker processes (default: CPU count)") do |v|
    options[:jobs] = v
  end
  opts.on("--[no-]skip-existing", "Skip if game_id already exists in output") do |v|
    options[:skip_existing] = v
  end
  opts.on("--dry-run", "Parse and print tags without writing Parquet") do
    options[:dry_run] = true
  end
  opts.on("--verbose", "Print per-file tag details") do
    options[:verbose] = true
  end
end

parser.parse!(ARGV)

paths = []
if options[:dir]
  dir = Pathname.new(options[:dir])
  paths.concat(Dir.glob(dir.join(options[:glob]).to_s))
end
paths.concat(ARGV)
paths = paths.map { |p| File.expand_path(p) }.uniq.sort

if paths.empty?
  warn parser.to_s
  exit 1
end

def normalize_tags(infos)
  list = infos
  list = list.normalize if list.respond_to?(:normalize)
  list.flat_map do |info|
    names = []
    names << info.name if info.respond_to?(:name)
    names.concat(info.alias_names) if info.respond_to?(:alias_names)
    names
  end.compact.uniq
end

def player_tags(player)
  bundle = player.tag_bundle
  {
    attack: normalize_tags(bundle.attack_infos),
    defense: normalize_tags(bundle.defense_infos),
    technique: normalize_tags(bundle.technique_infos),
    note: normalize_tags(bundle.note_infos),
    style_key: (bundle.main_style_info&.key rescue nil),
    style_name: (bundle.main_style_info&.name rescue nil),
  }
end

def extract_player_info(header, keys)
  return [nil, nil] unless header

  raw = nil
  keys.each do |key|
    v = header[key]
    if v && !v.to_s.strip.empty?
      raw = v.to_s.strip
      break
    end
  end
  return [nil, nil] unless raw

  if (md = raw.match(/\A(.+?)[(（]\s*(\d+)\s*[)）]\z/))
    name = md[1].strip
    rating = md[2].to_i
    return [name, rating]
  end

  [raw, nil]
end

def load_existing(output)
  store = COLUMNS.to_h { |c| [c, []] }
  existing_paths = Set.new
  return { existing_paths: existing_paths, store: store } unless File.exist?(output)

  begin
    Parquet.each_row(output, result_type: :array, columns: COLUMNS) do |row|
      COLUMNS.each_with_index { |col, idx| store[col] << row[idx] }
      existing_paths << row[0]
    end
  rescue => e
    warn "failed to read existing parquet (#{output}): #{e.class}: #{e.message}"
    existing_paths.clear
    store = COLUMNS.to_h { |c| [c, []] }
  end

  { existing_paths: existing_paths, store: store }
end

def game_id_from_path(path)
  File.basename(path).sub(/\.kif\z/i, "")
end

def build_row(path)
  data = File.binread(path)
  body = NKF.nkf("-w", data)

  parser = Bioshogi::Parser.parse(body, {
    typical_error_case: :embed,
    ki2_function: false,
    validate_feature: false,
    analysis_feature: true,
  })

  header = parser.pi&.header
  players = parser.container.players
  sente = players.find { |p| p.location.key.to_s == "black" }
  gote = players.find { |p| p.location.key.to_s == "white" }

  sente_tags = sente ? player_tags(sente) : {}
  gote_tags = gote ? player_tags(gote) : {}

  game_type = header&.[]("棋戦")
  sente_name, sente_rating = extract_player_info(header, ["先手", "下手", "先手番"])
  gote_name, gote_rating = extract_player_info(header, ["後手", "上手", "後手番"])

  {
    "game_id" => game_id_from_path(path),
    "game_type" => game_type,
    "sente_name" => sente_name,
    "sente_rating" => sente_rating,
    "gote_name" => gote_name,
    "gote_rating" => gote_rating,
    "turn_max" => parser.container.turn_info.turn_offset,
    "sente_attack_tags" => sente_tags[:attack]&.join(", "),
    "sente_defense_tags" => sente_tags[:defense]&.join(", "),
    "sente_technique_tags" => sente_tags[:technique]&.join(", "),
    "sente_note_tags" => sente_tags[:note]&.join(", "),
    "gote_attack_tags" => gote_tags[:attack]&.join(", "),
    "gote_defense_tags" => gote_tags[:defense]&.join(", "),
    "gote_technique_tags" => gote_tags[:technique]&.join(", "),
    "gote_note_tags" => gote_tags[:note]&.join(", "),
  }
end

processed = 0
created = 0
skipped = 0
errors = 0
last_report_at = Time.now

require "set"
existing = options[:skip_existing] && !options[:dry_run] ? load_existing(options[:output]) : nil
existing_paths = existing ? existing[:existing_paths] : Set.new
store = existing ? existing[:store] : COLUMNS.to_h { |c| [c, []] }

paths_to_process = if options[:skip_existing] && !existing_paths.empty?
  paths.reject { |path| existing_paths.include?(game_id_from_path(path)) }
else
  paths
end
skipped = paths.length - paths_to_process.length
total = paths_to_process.length

jobs = options[:jobs].to_i
jobs = 1 if jobs <= 0
jobs = total if jobs > total && total > 0

mutex = Mutex.new

def report_progress(processed, total, created, skipped, errors, last_report_at)
  if Time.now - last_report_at >= 1
    percent = total > 0 ? ((processed.to_f / total) * 100).round(1) : 0
    warn format("progress: %d/%d (%.1f%%) created=%d skipped=%d errors=%d", processed, total, percent, created, skipped, errors)
    return Time.now
  end
  last_report_at
end

if jobs <= 1
  paths_to_process.each do |path|
    break if options[:limit] > 0 && processed >= options[:limit]
    processed += 1
    last_report_at = report_progress(processed, total, created, skipped, errors, last_report_at)

    begin
      row = build_row(path)
      if options[:dry_run]
        puts "#{path}: #{row}" if options[:verbose]
      else
        COLUMNS.each { |col| store[col] << row[col] }
        created += 1
      end
    rescue => e
      errors += 1
      warn "#{path}: #{e.class}: #{e.message}"
    end
  end
else
  slices = Array.new(jobs) { [] }
  paths_to_process.each_with_index do |path, idx|
    slices[idx % jobs] << path
  end

  readers = []
  pids = []

  slices.each do |slice|
    read_io, write_io = IO.pipe
    pid = Process.fork do
      read_io.close
      slice.each do |path|
        begin
          row = build_row(path)
          Marshal.dump({ type: :row, path: path, row: row }, write_io)
        rescue => e
          Marshal.dump({ type: :error, path: path, error: "#{e.class}: #{e.message}" }, write_io)
        end
      end
      write_io.close
      exit! 0
    end

    write_io.close
    pids << pid
    readers << Thread.new do
      begin
        loop do
          msg = Marshal.load(read_io)
          mutex.synchronize do
            processed += 1
            last_report_at = report_progress(processed, total, created, skipped, errors, last_report_at)
            if msg[:type] == :row
              if options[:dry_run]
                puts "#{msg[:path]}: #{msg[:row]}" if options[:verbose]
              else
                COLUMNS.each { |col| store[col] << msg[:row][col] }
                created += 1
              end
            else
              errors += 1
              warn "#{msg[:path]}: #{msg[:error]}"
            end
          end
        end
      rescue EOFError
        # done
      ensure
        read_io.close
      end
    end
  end

  readers.each(&:join)
  pids.each { |pid| Process.wait(pid) }
end

unless options[:dry_run]
  Dir.mkdir(File.dirname(options[:output])) unless Dir.exist?(File.dirname(options[:output]))
  schema = COLUMNS.map do |name|
    if name == "turn_max" || name.end_with?("_rating")
      { name => "int32" }
    else
      { name => "string" }
    end
  end
  rows_enum = Enumerator.new do |yielder|
    row_count = store[COLUMNS[0]].length
    row_count.times do |idx|
      yielder << COLUMNS.map { |col| store[col][idx] }
    end
  end
  Parquet.write_rows(
    rows_enum,
    schema: schema,
    write_to: options[:output]
  )
end

puts "processed=#{processed} created=#{created} skipped=#{skipped} errors=#{errors}"
