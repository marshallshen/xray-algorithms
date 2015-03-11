require_relative 'cross_service/interests_scrapper'
module Measure
  module CrossService
    class << self
      def run
        accounts = load_accounts
        terms = load_search_terms
        load_interests(accounts, terms)
        search_across_web(accounts)
        clusterize_snapshots

        :ok
      end

      def load_accounts
        @accounts ||= YAML::load_file('measures/config.yml')['accounts']
      end

      def load_search_terms
        ['music', 'food', 'dogs']
      end

      def saerch_across_web
        p "TODO: search across web"
      end

      def clusterize_snapshots
        p "TODO: clusterize snapshots"
      end

      def load_interests(accounts, terms)
        accounts.each do |account|
          scrapper = ::InterestScraper.new(account)
          scrapper.login!
          sleep(2)
          scrapper.get_youtube_video_ads(terms.sample)
          sleep(2)
          interests = scrapper.get_interests!
          puts interests.inspect
        end
      end
    end
  end
end

