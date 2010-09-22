require File.dirname(__FILE__) + '/spec_helper'

begin
  require "moneta/adapters/memcached"

  describe "Moneta::Adapters::Memcached" do
    before(:each) do
      # @native_expires = true
      @cache = Moneta::Builder.build do
        run Moneta::Adapters::Memcached, Memcached.new('localhost:11211', :namespace => 'moneta_spec')
      end
      @cache.clear
    end

    it_should_behave_like "a read/write Moneta cache"
  end
rescue SystemExit
end