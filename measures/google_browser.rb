require_relative '../app.rb'

class GoogleBrowser
  include Capybara::DSL

  Capybara.register_driver :selenium do |app|
    Capybara::Selenium::Driver.new(app, :browser => :chrome)#, :args => ['--incognito'])
    # require 'selenium/webdriver'
    # Selenium::WebDriver::Firefox::Binary.path = "/opt/firefox/firefox"
    # Capybara::Selenium::Driver.new(app, :browser => :firefox)
  end

  attr_accessor :google
  
  def initialize
    # Capybara.default_wait_time = 30
    @google = Capybara::Session.new(:selenium)
    Capybara.run_server = false
    Capybara.default_driver = :selenium
    Capybara.app_host = 'http://www.google.com'
  end

  def login!(account, link = 'https://accounts.google.com/ServiceLogin?hl=en')
    @google.visit(link)
    @google.within("form#gaia_loginform") do
      @google.fill_in 'Email', :with => (account[:login] || account["login"])
      @google.fill_in 'Passwd', :with => (account[:passwd] || account["passwd"])
    end
      @google.uncheck 'Stay signed in'
      @google.click_on 'Sign in'
  end

  def get_interests
    query = "https://www.google.com/settings/ads"
    @google.visit(query)
    sleep(1)

    interest_google_list = []
    interest_web_list = []
    begin
      @google.find(:xpath, '/html/body/div[6]/div/div[3]/div[5]/div[2]/div[1]/div[2]/div[1]/div').click
      interest_google_list = @google.all('td.Yt').map{|e| e.text}
    rescue
    end

    begin
      @google.find(:xpath, '/html/body/div[6]/div/div[3]/div[5]/div[2]/div[2]/div[2]/div[1]/div').click
      interest_web_list = @google.all('td.Yt').map{|e| e.text}
    rescue
    end

    puts "Interests in Google Services".green
    puts interest_google_list

    puts
    puts "Interests across the Web".green
    puts interest_web_list
  end

  def get_youtube_video_ads(search)
    query = "https://www.youtube.com/results?search_query=#{search.gsub(/ /, '+')}"
    @google.visit(query)
    sleep(2)

    video_ad_list_tmp = []
    video_ad_list = []

    begin
      video_ad_list_tmp = @google.all('div.pyv-afc-ads-inner').first.all('div.yt-lockup-content')
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
     end

    video_ad_list.each do |ad|
      ad.each_pair do |k, v|
        next if k == :long_url
        puts k.to_s.green + ": " + v
      end
      puts
    end
    puts
  end

  def clean
    @google.driver.browser.manage.delete_all_cookies
    @google.reset!
  end

  def self.test
    session = self.new
    account = {:login => 'qlanxray111', :passwd => 'qlanmdp111a'}

    session.login!(account)
    sleep(2)
    session.get_youtube_video_ads('finance')
    sleep(1)
    session.get_interests
  end
end
