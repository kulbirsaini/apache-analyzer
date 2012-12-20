#!/usr/bin/env ruby

#
# (C) Copyright Kulbir Saini <saini@saini.co.in>
#

require 'csv'
require 'active_support/all'
require 'active_record'
require 'mysql2'

HEADERS = [ :domain, :client_ip, :u1, :u2, :time, :request, :status, :size, :referer, :user_agent ]

results = []

ActiveRecord::Base.establish_connection(
  :adapter  => "mysql2",
  :host     => "localhost",
  :username => "",
  :password => "",
  :database => ""
)

class Message < ActiveRecord::Base
end

def format_time(line)
  line.sub(/\[([^\:]+):([^\]]+)\]/, '"\1 \2"')
end

def parse(file)
  i = 1
  File.open(file).each_line do |line|
    if line =~ /POST/ and line !~ /108\.161\.130\.153/ and line !~ /183\.83\.35\.235/
      parsed_line = CSV.parse_line(format_time(line), :col_sep => ' ', :headers => HEADERS)
      domain = parsed_line[:domain].split(':').first
      client_ip = parsed_line[:client_ip]
      time = parsed_line[:time].to_datetime
      request = parsed_line[:request]
      if request == '-'
        request, method, path = '', '', ''
      else
        method, path = parsed_line[:request].scan(/[^ ]+/)
      end
      status = parsed_line[:status].to_i
      size = parsed_line[:size].to_i
      referer = parsed_line[:referer]
      user_agent = parsed_line[:user_agent]
      Message.create({ :domain => domain, :client_ip => client_ip, :time => time, :request => request, :method => method, :path => path, :status => status, :size => size, :referer => referer, :user_agent => user_agent })
    end
    puts i
    i += 1
    sleep 10 if i % 2000 == 0
  end
end

parse(ARGV[0]) if __FILE__ == $0
