require 'spec_helper'

begin
  require "moneta/adapters/riak"

  describe "Moneta::Adapters::Riak" do
    before(:each) do
      @cache = Moneta::Adapters::Riak.new
      @cache.clear
    end

    it_should_behave_like "a read/write Moneta cache"
  end
rescue SystemExit
end
