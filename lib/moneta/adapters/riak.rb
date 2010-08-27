begin
  require "riak"
rescue LoadError
  puts "You need the riak-client gem to use the Redis store"
  exit
end

module Moneta
  module Adapters
    class Riak
      include Defaults

      def initialize(options = {})
        bucket = options.delete(:bucket) || 'cache'
        @cache = ::Riak::Client.new(options)[bucket]
      end

      def key?(key, *)
        !!self[key]
      end

      def [](key)
        begin
          result = @cache.get(key_for(key))
          result && deserialize(result.data)
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
        data = serialize(value)
        obj  = @cache.get_or_new(key)
        obj.data = data
        obj.store
        data
      end

      def clear
        @cache.keys do |keys|
          keys.each { |key| @cache.delete(key) }
        end
      end
    end
  end
end