begin
  require "riak"
rescue LoadError
  puts "You need the riak-client gem (>= 0.8) to use the Riak store. \
          If 0.8 is not released yet, gem install riak-client --pre to get it."
  exit
end

module Moneta
  module Adapters
    class Riak
      include Defaults

      def initialize(options = {})
        bucket = options.delete(:bucket) || 'cache'
        @content_type = options.delete(:content_type) || 'application/x-ruby-marshal'
        @cache = ::Riak::Client.new(options)[bucket]
      end

      def key?(key, *)
        !!self[key]
      end

      def [](key)
        begin
          result = @cache.get(key_for(key))
          result && result.data
        rescue ::Riak::FailedRequest => e
          if e.code.to_i == 404
            nil
          else
            raise(e)
          end
        end
      end

      def delete(key, *)
        string_key = key_for(key)
        value = self[key]
        @cache.delete(string_key) if value
        value
      end

      def store(key, value, *)
        key  = key_for(key)
        obj  = @cache.get_or_new(key)
        obj.content_type = @content_type
        obj.data = value
        obj.store
        value
      end

      def clear
        @cache.keys do |keys|
          keys.each { |key| @cache.delete(key) }
        end
      end
    end
  end
end