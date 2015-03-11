require 'capybara/poltergeist'
require 'yaml'

class InterestScraper
  include Capybara::DSL

  attr_accessor :google, :account

  PREFERENCES_ENDPOINT = 'https://www.google.com/settings/ads'
  LOGIN_ENDPOINT       = 'https://accounts.google.com/ServiceLogin'

  def initialize(account)
    initialize_capybara
    @account = account
    @google = Capybara::Session.new(:poltergeist)
  end

  def initialize_capybara
    Capybara.configure do |c|
      c.run_server = false
      c.default_driver = :poltergeist
      c.app_host = 'http://www.google.com'
    end
  end

  def login!
    google.visit(LOGIN_ENDPOINT)

    google.within("form#gaia_loginform") do
      google.fill_in 'Email', with: account[:login]
      google.fill_in 'Passwd', with: account[:passwd]
    end

    google.uncheck 'Stay signed in'
    google.click_on 'Sign in'
  ensure
    enable_interest!
  end

  def enable_interest!
    google.visit(PREFERENCES_ENDPOINT)
    sleep(1)
      #google.driver.save_screenshot("#{Time.now.to_s}_before.png", :full => true)
    begin
      return if !google.has_selector?(:xpath, "/html/body/div[6]/div/div[3]/div[7]/div[2]/div[2]/div[2]/form[1]/div[1]")

      google.find(:xpath, "/html/body/div[6]/div/div[3]/div[7]/div[2]/div[2]/div[2]/form[1]/div[1]").click
      #google.driver.save_screenshot("#{Time.now.to_s}_after_success.png", :full => true)
    rescue
      #google.save_screenshot("#{Time.now.to_s}_after_error.png", :full => true)
    end
  end

  def get_interests!
    google.visit(PREFERENCES_ENDPOINT)

    sleep(1)

    Interests.new.tap do |i|
      i.on_google      = scrape_interests '/html/body/div[6]/div/div[3]/div[5]/div[2]/div[1]/div[2]/div[1]/div'
      i.across_the_web = scrape_interests '/html/body/div[6]/div/div[3]/div[5]/div[2]/div[2]/div[2]/div[1]/div'
    end
  end

  def scrape_interests(xpath)
    begin
      google.find(:xpath, xpath).click
      google.all('td.Yt').map{ |e| e.text }
    rescue
      []
    end
  end

  class Interests
    attr_accessor :on_google, :across_the_web
  end

  def get_youtube_video_ads(search)
    query = "https://www.youtube.com/results?search_query=#{search.gsub(/ /, '+')}"
    google.visit(query)
    sleep(2)

    video_ad_list_tmp = []
    video_ad_list = []

    begin
      video_ad_list_tmp = google.all('div.pyv-afc-ads-inner').first.all('div.yt-lockup-content')
      video_ad_list_tmp.each do |ad|
        title = ad.all('h3.yt-lockup-title').first.text
        long_url = ad.all('h3.yt-lockup-title').first.all('a').first['href']
        short_url = long_url.split(/adurl=/).last
        by = ad.all('div.yt-lockup-byline').first.all('a').first.text
        description = ad.all('div.yt-lockup-description').first.text
        video_ad_list.push(
        {title: title,
         long_url: long_url,
         short_url: short_url,
         by: by,
         description: description})
      end
    rescue
      puts 'died here'
    end

    video_ad_list.each do |ad|
      ad.each_pair do |k, v|
        next if k == :long_url
        puts k.to_s + ": " + v
      end
      puts
    end
    puts
  end

  def clean!
    google.driver.clear_cookies
    google.reset!
  end
end
#config = YAML::load_file('measures/config.yml')
#accounts = config['accounts']
#settings = accounts.map do |account|
#  {
#    login: account['login'],
#    passwd: account['passwd']
#  }
#end
#s = InterestScraper.new(settings.last)
#s.login!
#sleep(2)
#s.get_youtube_video_ads('music')
#sleep(2)
#interests = s.get_interests!
#puts interests.inspect
#s.clean!
#
#s = InterestScraper.new(settings.first)
#s.login!
#sleep(2)
#s.get_youtube_video_ads('food')
#sleep(2)
#interests = s.get_interests!
#puts interests.inspect
