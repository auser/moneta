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

      def initialize(options={})
        bucket        = options.delete(:bucket) || 'cache'
        allow_mult    = options.delete(:allow_mult) || false
        @content_type = options.delete(:content_type) || 'application/x-ruby-marshal'
        @client       = ::Riak::Client.new(options)
        @bucket       = @client[bucket]

        if @bucket.allow_mult != allow_mult
          @bucket.allow_mult = allow_mult
        end
      end

      def key?(key, *)
        @bucket.exists?(key_for(key))
      end

      def [](key)
        begin
          robject = @bucket.get(key_for(key))
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
        value = self[key]
        @bucket.delete(key_for(key)) if value
        value
      end

      def store(key, value, *)
        key  = key_for(key)
        obj  = @bucket.get_or_new(key)
        obj.content_type = @content_type
        obj.data = value
        obj.store
        value
      end

      def clear
        @bucket.keys do |keys|
          keys.each { |key| @bucket.delete(key) }
        end
      end
    end
  end
end