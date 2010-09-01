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

  describe "reading key with conflicts" do
    before(:each) do
      client = Riak::Client.new
      bucket = client['cache']

      Riak::Client.should_receive(:new).and_return(client)
      client.should_receive(:[]).with('cache').and_return(bucket)

      @robject = Riak::RObject.new(bucket, 'test')
      @robject.load({:headers => {"content-type" => ["multipart/mixed; boundary=foo"]}, :code => 300})

      bucket.should_receive(:get).with('test').and_return(@robject)

      @cache = Moneta::Adapters::Riak.new
      @cache.clear
    end

    it "raises conflict exception" do
      lambda {
        @cache['test']
      }.should raise_error(Moneta::Adapters::Riak::Conflict)
    end

    it "stores robject on the exception" do
      begin
        @cache['test']
      rescue => exception
        exception.robject.should == @robject
      end
    end
  end

rescue SystemExit
end
