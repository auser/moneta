begin
  require "memcached"
rescue
  puts "You need the `memcached` gem to use the Memcached moneta store"
  exit
end

module Moneta
  module Adapters
    class Memcached
      include Moneta::Defaults

      def initialize(cache)
        @cache = cache
      end

      def key?(key, *)
        !self[key].nil?
      end

      def [](key)
        deserialize(@cache.get(key_for(key)))
      rescue ::Memcached::NotFound
      end

      def delete(key, *)
        value = self[key]
        @cache.delete(key_for(key)) if value
        value
      end

      def store(key, value, *)
        @cache.set(key_for(key), serialize(value))
      end

      def clear(*)
        @cache.flush
      end

    private
      def key_for(key)
        [super].pack("m").strip
      end
    end
  end
end
