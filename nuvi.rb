require "net/http"
require "uri"
require 'zip'
require 'redis'
require 'digest/sha1'

module Nuvi

  @@redis = Redis.new

  class << self
    def download_url(url)
      uri = URI.parse(url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.read_timeout = 500
      http.get(uri)
    end

    def get_zip_links(page_url)
      puts "Getting zip links from page #{page_url}"
      download_url(page_url).body.scan(/href=".*\.zip"/).map{|link_tag| link_tag[6..-2]}.map(&->(url){url.include?("http") ? url : "#{page_url}/#{url}"})
    end

    def download_zip(zip_url)
      puts "Downloading zip file from #{zip_url}"
      zip = download_url(zip_url).body
      File.open(zip_url.split('/').last, 'w+') do |f|
        f.write(zip)
      end
      zip_url.split('/').last
    end

    def extract_zip(zip_path)
      puts "Extracting zip contents for zip #{zip_path}"
      xml_files = []
      Zip::File.open(zip_path) do |zip_file|
        zip_file.each do |entry|
          xml_files << entry.get_input_stream.read
        end
      end
      xml_files
    end

    def publish_xml_to_redis(xml_document)
      puts "Publishing xml to redis"
      puts xml_document.class
      digest = Digest::SHA1.hexdigest(xml_document)
      if !@@redis.sismember("NEWS_XML_SET", digest)
        puts "Document not already in redis. Adding xml document."
        @@redis.rpush("NEWS_XML", xml_document)
        @@redis.sadd("NEWS_XML_SET", digest)
      end
      nil
    end

    def process_page(page_url)
      links = get_zip_links(page_url)
      num_processes = 8
      links_per_process = links.count / num_processes
      process_links = links.each_slice(links_per_process)
      puts "Downloading #{links.count} links using #{num_processes} different processes"
      child_pids = []
      process_links.each do |segment|
        pid = Process.fork do
          puts "Forking new process to process #{segment.count} links..."
          segment.map{|link| extract_zip(download_zip(link)).map{|xml| publish_xml_to_redis(xml)}; nil}
          # segment.map{|link| extract_zip(download_zip(link))}.flatten.map{|xml| publish_xml_to_redis(xml)}
        end
        child_pids << pid
      end
      child_pids.each{|pid| Process.wait(pid)}
    end
  end
end
