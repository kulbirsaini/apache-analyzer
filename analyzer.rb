#!/usr/bin/env ruby
#
# (C) Copyright Kulbir Saini <saini@saini.co.in>
#

require 'csv'
require 'active_support/all'
require 'active_record'
require 'mysql2'

HEADERS = [ :domain, :unique_id, :client_ip, :u1, :u2, :time, :request, :status, :size, :referer, :user_agent ]
SKIP_IPS = [ '108.161.130.153', '127.0.0.1', '183.83.35.235' ]
SKIP_METHODS = [ 'OPTIONS', 'PROPFIND' ]

def connect_db
  ActiveRecord::Base.establish_connection(YAML.load_file('database.yml'))
end
connect_db

def create_table
  table_name = Message.table_name
  query = "CREATE TABLE IF NOT EXISTS #{table_name} (id BIGINT PRIMARY KEY AUTO_INCREMENT, domain VARCHAR(64), client_ip VARCHAR(24), time TIMESTAMP, request VARCHAR(512), method VARCHAR(12), path VARCHAR(255), status INT, size INT, referer VARCHAR(512), user_agent VARCHAR(255), unique_id VARCHAR(64) BINARY NOT NULL);"
  indices = {
    :domain_index => "CREATE INDEX domain_index ON #{table_name} (domain);",
    :client_ip_index => "CREATE INDEX client_ip_index ON #{table_name} (client_ip);",
    :method_index => "CREATE INDEX method_index ON #{table_name} (method);",
    :status_index => "CREATE INDEX status_index ON #{table_name} (status);",
    :user_agent_index => "CREATE INDEX user_agent_index ON #{table_name} (user_agent);",
    :time_index => "CREATE INDEX time_index ON #{table_name} (time);",
    :unique_id_index => "CREATE UNIQUE INDEX unique_id_index ON #{table_name} (unique_id);"
  }
  ActiveRecord::Base.connection.execute(query)
  results = ActiveRecord::Base.connection.execute("SHOW INDEX FROM #{table_name};")
  existing_indices = results.map{ |result| Hash[results.fields.zip(result)] }.map{ |result| result['Key_name'].to_sym }.uniq
  (indices.keys - existing_indices).each { |index| ActiveRecord::Base.connection.execute(indices[index]) }
  nil
end

class Message < ActiveRecord::Base
  POST_THRESHOLD = 50
  REQUEST_THRESHOLD = 300

  def self.offending_ips(from = 2.days.ago.to_time, to = Time.now)
    where(:method => 'POST').where('time >= ? AND time <= ?', from, to).select('client_ip, COUNT(*) as access_count').group('client_ip').order('access_count desc').select{ |m| m['access_count'] > POST_THRESHOLD }.map{ |m| m.client_ip }
  end

  def self.blacklist_ips(from = 2.days.ago.to_time, to = Time.now)
    @blacklist_ips = where('client_ip IN (?)', Message.offending_ips(from, to)).where('time >= ? AND time <= ?', from, to).select('client_ip, COUNT(*) as access_count').group('client_ip').order('access_count desc').select{ |m| m['access_count'] > REQUEST_THRESHOLD }.map{ |m| m.client_ip }.sort
    File.open('blacklist.txt', 'w').write(@blacklist_ips.join("\n") + "\n")
    puts "Written #{@blacklist_ips.count} IP addresses"
  end
end

def parse(file)
  db_queries = 0
  File.open(file).each_with_index do |line, index|
    print '.' if index % 10 == 0
    puts " #{index + 1}" if (index + 1) % 1200 == 0
    sleep 4 if (db_queries + 1) % 3000 == 0 or (index + 1) % 20000 == 0
    begin
      parsed_line = CSV.parse_line(line.sub(/\[([^\]]+)\]/, '"\1"').gsub('\"', ''), :col_sep => ' ', :headers => HEADERS)
      unique_id = parsed_line[:unique_id]
      client_ip = parsed_line[:client_ip]
      request = parsed_line[:request]
      if request == '-'
        request, method, path = '', '', ''
      else
        method, path = parsed_line[:request].scan(/[^ ]+/)
      end
      next if SKIP_IPS.include?(client_ip) or SKIP_METHODS.include?(method) or Message.where(:unique_id => unique_id).first
      time = parsed_line[:time].sub(/:/, ' ').to_datetime
      domain = parsed_line[:domain].split(':').first
      status = parsed_line[:status].to_i
      size = parsed_line[:size].to_i
      referer = parsed_line[:referer]
      user_agent = parsed_line[:user_agent]
      Message.create({ :domain => domain, :client_ip => client_ip, :time => time, :request => request, :method => method, :path => path, :status => status, :size => size, :referer => referer, :user_agent => user_agent, :unique_id => unique_id })
      db_queries += 1
    rescue SystemExit, Interrupt
      puts
      puts index + 1
      puts line
      raise
    rescue => exception
      puts index + 1
      puts line
      puts "Error during processing: #{$!}"
      puts "Backtrace:\n\t#{exception.backtrace.join("\n\t")}"
      break
    end
  end
  puts
end

if __FILE__ == $0
  create_table
  parse(ARGV[0])
end
