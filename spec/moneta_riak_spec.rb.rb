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

  describe "intializing with allow_mult option" do
    it "sets to true if true" do
      Moneta::Adapters::Riak.new(:bucket => 'multifabulous', :allow_mult => true)
      ::Riak::Client.new['multifabulous'].allow_mult.should be_true
    end

    it "sets to false if false" do
      Moneta::Adapters::Riak.new(:bucket => 'multifabulous', :allow_mult => false)
      ::Riak::Client.new['multifabulous'].allow_mult.should be_false
    end
  end

  describe "setting content type" do
    before(:each) do
      @cache = Moneta::Adapters::Riak.new(:content_type => 'application/json')
      @cache.clear
      @raw_cache = ::Riak::Client.new['cache']
    end

    it "serializes on write" do
      @cache['foo'] = {:foo => 'bar'}
      @raw_cache['foo'].data.should == {'foo' => 'bar'}
    end

    it "deserializes on read" do
      obj = @raw_cache.get_or_new('foo')
      obj.content_type = 'application/json'
      obj.data = {'foo' => 'bar'}
      obj.store

      @cache['foo'] = {'foo' => 'bar'}
    end
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
