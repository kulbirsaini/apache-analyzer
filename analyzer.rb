#!/usr/bin/env ruby
#
# (C) Copyright Kulbir Saini <saini@saini.co.in>
#

require 'csv'
require 'active_support/all'
require 'active_record'
require 'mysql2'

HEADERS = [ :domain, :client_ip, :u1, :u2, :time, :request, :status, :size, :referer, :user_agent ]
SKIP_IPS = [ '108.161.130.153', '127.0.0.1', '183.83.35.235' ]
SKIP_METHODS = [ 'OPTIONS', 'PROPFIND' ]

def connect_db
  ActiveRecord::Base.establish_connection(YAML.load_file('database.yml'))
end

def create_table
  table_name = Message.table_name
  query = "CREATE TABLE IF NOT EXISTS #{table_name} (id BIGINT PRIMARY KEY AUTO_INCREMENT, domain VARCHAR(64), client_ip VARCHAR(24), time TIMESTAMP, request VARCHAR(512), method VARCHAR(12), path VARCHAR(255), status INT, size INT, referer VARCHAR(512), user_agent VARCHAR(255));"
  indices = {
    :domain => "CREATE INDEX domain_index ON #{table_name} (domain);",
    :client_ip => "CREATE INDEX client_ip_index ON #{table_name} (client_ip);",
    :method => "CREATE INDEX method_index ON #{table_name} (method);",
    :status => "CREATE INDEX status_index ON #{table_name} (status);",
    :user_agent => "CREATE INDEX user_agent_index ON #{table_name} (user_agent);",
    :time => "CREATE INDEX time_index ON #{table_name} (time);"
  }
  ActiveRecord::Base.connection.execute(query)
  results = ActiveRecord::Base.connection.execute("SHOW INDEX FROM #{table_name};")
  existing_indices = results.map{ |result| Hash[results.fields.zip(result)] }.map{ |result| result['Column_name'].to_sym }
  (indices.keys - existing_indices).each { |index| ActiveRecord::Base.connection.execute(indices[index]) }
  nil
end

class Message < ActiveRecord::Base
  POST_THRESHOLD = 50
  REQUEST_THRESHOLD = 300

  def self.offending_ips(from = 2.days.ago.to_time, to = Time.now)
    where(:method => 'POST').where('time => ? AND time <= ?', [from, to]).select('client_ip, COUNT(*) as access_count').group('client_ip').order('access_count desc').select{ |m| m['access_count'] > POST_THRESHOLD }.map{ |m| m.client_ip }
  end

  def self.blacklist_ips(from = 2.days.ago.to_time, to = Time.now)
    @blacklist_ips = where('client_ip IN (?)', Message.offending_ips(from, to)).select('client_ip, COUNT(*) as access_count').group('client_ip').order('access_count desc').select{ |m| m['access_count'] > REQUEST_THRESHOLD }.map{ |m| m.client_ip }.sort
    File.open('blacklist.txt', 'w').write(@blacklist_ips.join("\n"))
  end
end

def parse(file)
  i = 1
  last_time = Message.order('time').last.try(:time)
  puts last_time
  File.open(file).each_line do |line|
    puts i
    i += 1
    sleep 4 if i % 2000 == 0
    begin
      parsed_line = CSV.parse_line(line.sub(/\[([^\]]+)\]/, '"\1"').gsub('\"', ''), :col_sep => ' ', :headers => HEADERS)
      domain = parsed_line[:domain].split(':').first
      client_ip = parsed_line[:client_ip]
      next if SKIP_IPS.include?(client_ip)
      time = parsed_line[:time].sub(/:/, ' ').to_datetime
      next if last_time and last_time > time
      request = parsed_line[:request]
      if request == '-'
        request, method, path = '', '', ''
      else
        method, path = parsed_line[:request].scan(/[^ ]+/)
      end
      next if SKIP_METHODS.include?(method)
      status = parsed_line[:status].to_i
      size = parsed_line[:size].to_i
      referer = parsed_line[:referer]
      user_agent = parsed_line[:user_agent]
      Message.create({ :domain => domain, :client_ip => client_ip, :time => time, :request => request, :method => method, :path => path, :status => status, :size => size, :referer => referer, :user_agent => user_agent })
    rescue => exception
      puts line
      puts "Error during processing: #{$!}"
      puts "Backtrace:\n\t#{exception.backtrace.join("\n\t")}"
      break
    end
  end
end

if __FILE__ == $0
  connect_db
  create_table
  parse(ARGV[0])
end
