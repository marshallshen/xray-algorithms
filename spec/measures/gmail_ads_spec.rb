require 'spec_helper'

describe 'gmail ads' do
  it 'has runnable experiments' do
    expect(GmailAds.run!).to eq(true)
  end
end
