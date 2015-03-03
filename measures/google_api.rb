class GoogleAPI
  class AuthError < RuntimeError; end

  def self.test
    acc = { login: "qlanxray111", passwd: "qlanmdp111a" }
    api = self.new(acc)
    api.login
    api.get_youtube_video_ads('finance')
    # api.get_interests
  end

  class CookieJar < Faraday::Middleware
    def initialize(app)
      super
      @cookies = {}
    end

    def pprint_meta(env, type)
      return if true

      case type
      when :request; color = :green; header = env[:request_headers]
      when :response; color = :red; header = env[:response_headers]
      end

      puts
      puts "request".send(color)
      puts "url ".send(color) + env[:url].to_s
      puts "verb ".send(color) + env[:method].to_s
      puts env[:body].to_s if type == :request && env[:method] == :post
      puts "headers ".send(color) + header.to_s
    end

    def call(env)
      set_meta(env)
      set_cookies(env)
      pprint_meta(env, :request)

      parse_cookies(env)
    end

    def cookies_for_host(env)
      @cookies ||= {}
    end

    def set_meta(env)
      env[:request_headers]['user-agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.132 Safari/537.36'
    end

    def set_cookies(env)
      env[:request_headers]["cookie"] = cookies_for_host(env).map { |k,v| "#{k}=#{v}"}.join("; ")
    end

    def parse_cookies(env)
      response = @app.call(env)
      response.on_complete do |e|
        pprint_meta(env, :response)

        raw_array = (e[:response_headers]['set-cookie'] || "").split(",")
        array = []
        skip = false

        raw_array.each do |item|
          unless skip
            array << item
          end
          if (item =~ /(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/)
            skip = true
          else
            skip = false
          end
        end

        cookies = array.select { |x| x =~ /=/ }.map { |x| x.split(';').first.strip.split('=', 2) }
        cookies_for_host(e).merge!(Hash[cookies])
      end
      response
    end

    Faraday.register_middleware :request, :cookie_jar => lambda { self }
  end

  attr_accessor :conn

  def initialize(account)
    @conn = Faraday.new do |faraday|
      faraday.request  :url_encoded
      faraday.response :follow_redirects, :limit => 20
      faraday.request  :cookie_jar
      faraday.adapter  :net_http_persistent
    end
    account = account.attributes unless account.class.name == "Hash"
    @acc = Hash[account.map { |k,v| [k.to_s, v] }]
  end

  def login
    response = @conn.get "https://accounts.google.com/ServiceLogin"
    raise "oops" if response.status != 200

    n = Nokogiri::HTML(response.body)
    galx = n.css('form input[name="GALX"]').attr('value').to_s

    response = @conn.post "https://accounts.google.com/ServiceLoginAuth", {
      "GALX"             => galx,
      "Email"            => @acc['login'],
      "Passwd"           => @acc['passwd'],
      "continue"         => "https://www.google.com/settings/ads",
      "followup"         => "https://www.google.com/settings/ads",
      "PersistentCookie" => "yes",
      "signIn" => "Sign in",
    }
    raise AuthError if response.body =~ /incorrect/
    File.open('abcd2.html', 'wb') {|f| f.write(response.body)}
    return self
  end

  def get_interests
    response = @conn.get("https://www.google.com/settings/ads")
    page = Nokogiri::HTML(response.body)
    
    if response.body =~/Please re-enter your password/
      puts "enter password"
      galx = page.css('form input[name="GALX"]').attr('value').to_s
      continue = page.css('form input[name="continue"]').attr('value').to_s
      followup = page.css('form input[name="followup"]').attr('value').to_s
      osid = page.css('form input[name="osid"]').attr('value').to_s
      _utf8 = page.css('form input[name="_utf8"]').attr('value').to_s
      bgresponse = page.css('form input[name="bgresponse"]').attr('value').to_s
      pstMsg = page.css('form input[name="pstMsg"]').attr('value').to_s
      puts galx
      puts continue
      puts followup
      puts osid
      puts _utf8
      puts bgresponse
      puts pstMsg
      response = @conn.post "https://accounts.google.com/ServiceLoginAuth", {
      "GALX"             => galx,
      "continue"         => continue,
      "followup"         => followup,
      "osid"             => osid,
      "_utf8"            => _utf8,
      "pstMsg"           => "1",
      "bgresponse"       => bgresponse,
      "Email"            => @acc['login'],
      "Passwd"           => @acc['passwd'],
      "signIn" => "Sign in",
      }
      raise AuthError if response.body =~ /incorrect/
    end

    File.open('abcd.html', 'wb') {|f| f.write(response.body)}
    page = Nokogiri::HTML(response.body)

    interests_web = page.css('a.gb_Xa gb_Ta')
    #gbq1 > div > a
    # interests_web = page.css('tr.BK_yr_pQ')
    puts
    puts interests_web.inspect
    
  end

  def get_youtube_video_ads(search)
    response = @conn.get("https://www.youtube.com/results?search_query=#{search.sub(/ /, '+')}")
    page = Nokogiri::HTML(response.body)

    # interests_web = page.css('div.pyv-afc-ads-inner')
    # interests_web = page.css('#results > div > div.pyv-afc-ads-inner > div:nth-child(3) > div')
    # interests_web = page.css('div.pyv-afc-ads-inner > div:nth-child(3) > div')
    File.open('abcd3.html', 'wb') {|f| f.write(response.body)}
    # interests_web = page.xpath("//*[@id='results']/div/div[1]/div[3]/div")
    interests_web = page.xpath("//*[@id='results']/div/div[1]/div")

    puts interests_web.inspect
    # binding.pry
    #results > div > div.pyv-afc-ads-inner > div:nth-child(3) > div
    puts
  end
end
