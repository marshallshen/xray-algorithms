require_relative '../legacy'

module GmailAds
  # This module is an interfac to run Gmail Ads experiment
  # It provides a receipe of how an experiment is ran
  # All the steps are extracted away somewhere else, including:
  #   - create experiment
  #   - create google accounts
  #   - bind experiment with accounts
  #   - run experiment
  #   - analyze experiment
  class << self
    def run!(options={})
      true
    end
  end
end
