require 'spec_helper'

describe Measure::Youtube do
  it 'transitions to steps via state machine' do
    youtube_measure = Measure::Youtube.new
    expect(youtube_measure.new?).to eq(true)

    youtube_measure.assign_accounts!
    expect(youtube_measure.runnable?).to eq(true)

    youtube_measure.start_measure!
    expect(youtube_measure.running?).to eq(true)

    youtube_measure.start_analysis!
    expect(youtube_measure.measuring?).to eq(true)

    youtube_measure.close_measure!
    expect(youtube_measure.closed?).to eq(true)
  end
end
