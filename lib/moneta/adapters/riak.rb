begin
  require "riak"
rescue LoadError
  puts "You need the riak-client gem (>= 0.8) to use the Riak store."
  exit
end

module Moneta
  module Adapters
    class Riak
      include Defaults

      class Conflict < StandardError
        attr_reader :robject

        def initialize(robject)
          @robject = robject
          super('Read conflict present')
        end
      end

      def initialize(options = {})
        bucket = options.delete(:bucket) || 'cache'
        @content_type = options.delete(:content_type) || 'application/x-ruby-marshal'
        @cache = ::Riak::Client.new(options)[bucket]
      end

      def key?(key, *)
        @cache.exists?(key_for(key))
      end

      def [](key)
        begin
          robject = @cache.get(key_for(key))
          if robject.conflict?
            raise Conflict.new(robject)
          end
          robject.data
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