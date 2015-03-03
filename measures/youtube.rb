require_relative '../legacy'

module Measure
  class Youtube
    include Workflow

    workflow do
      state :new do
        event :assign_accounts, transitions_to: :runnable
      end

      state :runnable do
        event :start_measure, transitions_to: :running
      end

      state :running do
        event :start_analysis, transitions_to: :measuring
      end

      state :measuring do
        event :close_measure, transitions_to: :closed
      end

      state :closed
    end


    def assign_accounts
      p "assigning google accounts.."
    end


    def start_measure
      p "start measurement.."
    end


    def start_analysis
      p "start analysis.."
    end

    def close_measure
      p "close measure..."
    end
  end
end

