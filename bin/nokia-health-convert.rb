#!/usr/bin/env ruby
require 'csv'
require 'date'
require 'fileutils'
require 'json'
require 'optparse'
require 'active_support/inflector'

formats = [
  :fitbit_weight,
  :nokia_weight,
]

options = {
  input_dir: './data',
  output_dir: './output',
  input_format: :fitbit_weight,
  output_format: :nokia_weight,
}

class Entry
  attr_reader :timestamp # DateTime

  def initialize(timestamp: DateTime.new(1970,1,1))
    @timestamp = timestamp
  end
end

class BodyEntry < Entry
  attr_reader :total_weight_kg  # float
  attr_reader :fat_weight_kg    # float
  attr_reader :bone_weight_kg   # float
  attr_reader :muscle_weight_kg # float
  attr_reader :water_weight_kg  # float
  attr_reader :comments         # string

  def initialize(timestamp: DateTime.new(1970,1,1),
                 total_weight_kg:,
                 fat_weight_kg: nil,
                 bone_weight_kg: nil,
                 muscle_weight_kg: nil,
                 water_weight_kg: nil,
                 comments: nil)
    super(timestamp: timestamp)

    @total_weight_kg = total_weight_kg
    @fat_weight_kg = fat_weight_kg
    @bone_weight_kg = bone_weight_kg
    @muscle_weight_kg = muscle_weight_kg
    @water_weight_kg = water_weight_kg
    @comments = comments
  end
end

class FitbitWeight
  def self.load(hash)
    time = DateTime.parse("#{hash['date']}T#{hash['time']}")
    total = hash['weight']
    fat_pct = hash['fat'].to_f
    fat = fat_pct > 0 ? fat_pct / 100 * total : nil

    BodyEntry.new(timestamp: time, total_weight_kg: total, fat_weight_kg: fat)
  end
end

class NokiaWeight
  BATCH_SIZE = 300

  attr_reader :date        # string (yyyy-mm-dd hh:mm:ss)
  attr_reader :weight      # float (server side units :/)
  attr_reader :fat_mass    # float
  attr_reader :bone_mass   # float
  attr_reader :muscle_mass # float
  attr_reader :hydration   # float
  attr_reader :comments    # string

  def initialize(date:,
                 weight:,
                 fat_mass: nil,
                 bone_mass: nil,
                 muscle_mass: nil,
                 hydration: nil,
                 comments: nil)
    @date = date
    @weight = weight
    @fat_mass = fat_mass
    @muscle_mass = muscle_mass
    @hydration = hydration
    @comments = comments
  end

  def self.from_body_entry(obj)
    date = obj.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    weight = obj.total_weight_kg # assume kg since import api is unit agnostic circa 2018
    fat_mass = obj.fat_weight_kg
    muscle_mass = obj.muscle_weight_kg
    hydration = obj.water_weight_kg
    comments = obj.comments

    self.new(
      date: date,
      weight: weight,
      fat_mass: fat_mass,
      muscle_mass: muscle_mass,
      hydration: hydration,
      comments: comments)
  end

  def to_csv
    [
      @date,
      @weight,
      @fat_mass,
      @muscle_mass,
      @hydration,
      @comments,
    ].to_csv
  end

  def self.csv_header
    "Date,Weight,Fat mass,Bone mass,Muscle mass,Hydration,Comments\n"
  end
end

OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename __FILE__} " +
  	'--input-dir INPUT_DIR ' +
  	'--output-dir OUTPUT_DIR '

  opts.on('-d', '--input-dir PATH', 'input directory') do |optarg|
    options[:input_dir] = optarg
  end

  opts.on('-D', '--output-dir PATH', 'output directory') do |optarg|
    options[:output_dir] = optarg
  end

  #opts.on('-f', '--input-format FORMAT', 'input format') do |optarg|
  #  options[:input_format] = optarg
  #end

  #opts.on('-F', '--output-format FORMAT', 'output format') do |optarg|
  #  options[:output_format] = optarg
  #end
end.parse!

# FitbitWeight

def flush(queue, dir, klass, index)
  FileUtils.mkdir_p dir
  filename = "#{dir}/#{klass.name}.#{index}.csv"
  puts "#{filename}"
  File.open(filename, 'w+') do |f|
    queue.each { |line| f.puts(line) }
  end
end

output_class = options[:output_format].to_s.camelize.constantize
queue = [output_class.csv_header]
index = 0
for file in Dir["#{options[:input_dir]}*"] do
  for entry in JSON.load(File.read(file)) do
    body_entry = FitbitWeight.load(entry)
    output_entry = output_class.from_body_entry(body_entry)
    queue << output_entry.to_csv
    if queue.length == output_class::BATCH_SIZE
      flush(queue, options[:output_dir], output_class, index)
      index += 1
      queue = []
    end
  end
end
# flush queue remainder
flush(queue, options[:output_dir], output_class, index)
