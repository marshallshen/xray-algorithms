require 'spec_helper'

describe Measure::CrossService do
  describe '#run' do
    it 'runs through all steps for one experiment' do
      accounts = [{login: 'foo', passwd: 'bar'}]
      terms = ['first_term', 'second_term']

      expect(Measure::CrossService).to receive(:load_accounts).and_return(accounts)
      expect(Measure::CrossService).to receive(:load_search_terms).and_return(terms)
      expect(Measure::CrossService).to receive(:load_interests).with(accounts, terms)
      expect(Measure::CrossService).to receive(:search_across_web).with(accounts)
      expect(Measure::CrossService).to receive(:clusterize_snapshots)

      response = Measure::CrossService.run
      expect(response).to eq(:ok)
    end
  end

  describe '#load_accounts' do
    it 'loads testing accounts from config file'
  end

  describe '#load_search_terms' do
    it 'loads all search terms from config file'
  end

  describe '#load_interests' do
    it 'scraps interests based on search terms and account profiles'
  end
end
