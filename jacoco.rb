#!/usr/bin/env ruby

require 'json'

arg = ARGV[0]
arg ||= "false"

$help = arg.include?("help")
$json = arg.include?("json")

if $help
  puts "Scrape jacoco output to generate a global test coverage report, pass \"json\" as an argument to get json output"
  puts "Uses settings.gradle in current directory to find list of components to check"
  puts "shows score of 0% if it does not find a file under build/jacocoHtml/index.html for the given component"
  exit(1)
end

$json_out = {}

def show(component, coverage)
  if $json
    $json_out[component]=coverage
  else
    puts "#{component}: #{coverage}"
  end
end

components_file = "settings.gradle"
components_list = IO.read(components_file)
  .split("\n")
  .select{ |l| not (l.nil? || l.empty?) }
  .map{ |line|  line.split("'")[1]}
  .select{ |l| not l.nil? }

components_list.each do |comp|
  filename = "#{comp}/build/jacocoHtml/index.html"
  #puts "parsing: #{filename}" # DEBUG
  unless File.exist?(filename)
    show(comp, "0%")
    next
  end
  content = IO.read(filename)

  # there is one section in the jacoco output with the total coverage, that is the table footer.  It is the only section
  # bracketed by <tfoot> ... </tfoot>
  footer = content.split("tfoot")[1]
    .gsub(">","").gsub("<","").gsub("&gt;","").gsub("&lt;","")
  #puts "footer: #{footer}" # DEBUG

  # \d+ is the percent value (ie 54%) this regex grabs the part of the jacoco footer that has the test coverage percent
  # ctr2 is the label for the table section that has the coverage percent we want
  regex1 = /ctr2\/span"\/span\d+%span/
  regex2 = /td class="ctr2"\d+%\/td/

  match = regex1.match(footer) 
  match = regex2.match(footer) if match.nil?
  part = match[0] # get the first match on the regex
  #puts "part: #{part}" # DEBUG

  percent = part[/\d+%/] # extract just the percent value

  show(comp, percent)

end

if $json
  puts $json_out.to_json
end
