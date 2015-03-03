###################################################################
##### This file includes all codes inherited from XRay code  ######
#####   Please pull out ANYTHING needed to a better location ######
#####   2015/03/03 Marshall Shen                             ######
###################################################################


class Experiment
  class AccountNumberError < RuntimeError; end
  class AccountCombinationError < RuntimeError; end

  include Mongoid::Document
  store_in database: ->{ self.database }

  field :name
  index({ name: 1 }, { :unique => true })
  field :type
  field :account_number  # uncludes +1 for the master
  field :e_perc_a  # puts each email in :e_perc_a * :account_number accounts
  field :emails  # an array of emails groupes in threads
  field :fill_up_emails, :default => []  # an array of emails sent to all accounts
  field :master_account  # string, id of the master account
  field :master_emails
  field :e_a_assignments
  field :measurements, :default => []
  field :has_master, :default => true
  field :analyzed, :default => false

  belongs_to :recurrent_exp
  index({ recurrent_exp: 1 })

  def self.current
    Experiment.where( :name => Mongoid.tenant_name ).first
  end

  class_attribute :exps_accs_emails_map
  def self.curr_accs_emails_map
    self.exps_accs_emails_map ||= {}
    name = self.current.name
    unless self.exps_accs_emails_map[name]
      self.exps_accs_emails_map[name] = {}
      Account.each do |a|
        emails_in = AccountEmail.where(account: a).uniq.map(&:email)
                                .select! {|em| em.cluster_targeting_id != "garbage"}
        self.exps_accs_emails_map[name] ||= []
        self.exps_accs_emails_map[name] [a.id] = emails_in
      end
    end
    return self.exps_accs_emails_map[name]
  end

  def self.duplicate_exp(name, new_name)
    exp = self.where( name: name ).first
    exp_attrs = exp.attributes
    exp_attrs['name'] = new_name
    new_exp = self.create(Hash[exp_attrs])
    accs = Mongoid.with_tenant(name) do
      GoogleAccount.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      accs.each { |a| GoogleAccount.new(a).tap { |na| na.id = a['_id'] }.save }
    end
    new_exp.measurements = []
    new_exp.save
  end

  def self.delete_redis_cache
    ns = "#{Experiment.current.name}:"
    Redis.instance.keys(ns+"*").each do |key|
      Redis.instance.del(key)
    end
  end

  def self.duplicate_snapshots(name, new_name)
    exp_new = self.where(name: new_name).first

    ad_snaps = Mongoid.with_tenant(name) do
      AdSnapshot.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      ad_snaps.each { |a| AdSnapshot.new(a).tap { |na| na.id = a['_id'] }.save }
    end

    email_snaps = Mongoid.with_tenant(name) do
      EmailSnapshot.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      email_snaps.each { |a| EmailSnapshot.new(a).tap { |na| na.id = a['_id'] }.save }
    end

    exp_new.save
  end

  def self.email_body_from_title(exp, title, body = nil)
    exp = Experiment.where(name: exp).first
    exp.emails.each do |e|
      email_text = e.first["text"] || e.first["html"]
      if e.first['subject'] == title && (body == nil || body.include?(email_text))
        return e.map { |x| x["text"] }.compact.join
      end
    end
    return nil
  end

  def email_body_from_title(title, body = nil)
    self.emails.each do |e|
      email_text = e.first["text"] || e.first["html"]
      if e.first['subject'] == title && (body == nil || body.include?(email_text))
        return e.map { |x| x["text"] }.compact.join
      end
    end
    return nil
  end

  def email_index_from_title(title, body = nil)
    self.emails.each_with_index do |e, i|
      email_text = e.first["text"] || e.first["html"]
      if e.first['subject'] == title && (body == nil || body.include?(email_text))
        return i
      end
    end
    return nil
  end

  def email_index_from_sig_id(signature_id)
    subject, body = JSON.parse(signature_id)
    self.emails.each_with_index do |e, i|
      email_text = e.first["text"] || e.first["html"]
      if e.first['subject'] == subject && (body == nil || body.include?(email_text))
        return i
      end
    end
    return nil
  end

  class_attribute :database
  def self.fixed_db(db_name)
    self.database = db_name.to_s
  end

  class_attribute :account_pool
  def self.accounts_from(db_name)
    self.account_pool = db_name.to_s
  end

  fixed_db :experiments
  accounts_from :google_pool

  def build_indexes
    Mongoid.create_db_indexes(self.name)
  end

  def self.build_indexes(exp_name)
    Mongoid.create_db_indexes(exp_name)
  end

  def master_account
    return nil unless has_master
    return super if super
    self.master_account = Mongoid.with_tenant(self.name) do
      GoogleAccount.first.id.to_s rescue nil
    end
  end

  def last_measurement
    self.measurements.map { |m| m["start"].to_i }.max
  end

  def all_reloads
    self.measurements.map { |m| m["reloads"] }.sum
  end

  def get_accounts
    n = Mongoid.with_tenant(self.name) { GoogleAccount.count }
    return if account_number <= n

    av = Mongoid.with_tenant(self.class.account_pool) do
      GoogleAccount.available_accounts.count
    end
    raise AccountNumberError if av < account_number - n

    accounts = Mongoid.with_tenant(self.class.account_pool) do
      GoogleAccount.available_accounts[0..account_number - n - 1]
    end
    accounts.each do |acc|
      acc = acc.attributes
      pool_id = acc.delete("_id")
      Mongoid.with_tenant(self.name) { GoogleAccount.create(acc) }
      Mongoid.with_tenant(self.class.account_pool) do
        acc = GoogleAccount.where( :id => pool_id).first
        acc.used ||= []
        acc.used.push(type)
        acc.save
      end
    end
  end

  def assign_emails
    self._assign_emails(:all)
  end

  def assign_emails_redundant_exp
    self._assign_emails([])
  end

  def _assign_emails(master_es = :all)
    self.emails ||= []
    self.e_a_assignments ||= {}

    if master_es == :all && has_master
      tmp_h = {}
      self.emails.flatten.each do |e|
        key = e['subject'].sub(/RE: /, '')
        e['subject'] = key
        (tmp_h[key] ||= []).push(e)
      end
      self.master_emails = tmp_h.values
    else
      self.master_emails = master_es
    end

    combinations = []
    Mongoid.with_tenant(self.name) do
      nac = GoogleAccount.count
      accs = GoogleAccount.all.map(&:id).select { |i| i.to_s != self.master_account }
      self.emails.count.times do
        combinations.push(accs.sample((self.e_perc_a * nac).floor))
      end
    end
   raise AccountCombinationError if combinations.size < emails.size

    self.emails.each_with_index do |e, i|
      combinations.shift.each { |aid| (self.e_a_assignments[aid] ||= []).push(i) }
    end
  ensure
    self.save
  end

  def send_emails(async = false, check = true)
    to_send = []
    self.e_a_assignments.each do |a_id, threads|
      threads = threads.map { |t_id| self.emails[t_id] }
      threads += self.fill_up_emails
      to_send += self.format_emails(threads, a_id)
    end
    to_send += self.format_emails(self.master_emails + self.fill_up_emails, self.master_account) if has_master

    e_sender = EmailSender.new
    to_send.shuffle.each { |s| e_sender.send_thread(s[0], s[1], nil, async, check) }
  end

  def send_emails_from_acc(async = false)
    to_send = []
    self.e_a_assignments.each do |a_id, threads|
      threads = threads.map { |t_id| self.emails[t_id] }
      threads += self.fill_up_emails
      to_send += self.format_emails(threads, a_id)
    end
    to_send += self.format_emails(self.master_emails + self.fill_up_emails, self.master_account) if has_master

    e_sender = EmailSender.new
    to_send.shuffle.each { |s| e_sender.send_thread(s[0], e_sender.random_recipient, s[1], async, false) }
  end

  def format_emails(threads, a_id, flatten_all = false)
    dest = Mongoid.with_tenant(self.name) do
      g = GoogleAccount.where( id: a_id ).first
      { "email"  => g.gmail, "login"  => g.login, "passwd" => g.passwd }
    end
    if flatten_all
      threads = threads.map { |t| t.map { |e| [e] } }.reduce([]) { |sum, t| sum = sum + t }
    end
    threads.map { |t| [t, dest] }
  end

  def _random_assignment(emails)
    acc_ids = Mongoid.with_tenant(self.name) { Account.all.map(&:_id).map(&:to_s) }
    assignments = {}
    acc_ids.each do |acc_id|
      assignments[acc_id] ||= []
      emails.each do |email|
        # put email in acc with e_perc_a probability
        assignments[acc_id].push email if Random.rand <= self.e_perc_a
        # add emails that go everywhere
      end
      assignments[acc_id] += self.fill_up_emails
    end
    return assignments
  end

  def add_emails(emails)
    self.emails += emails
    self.emails.uniq!
    assignments = self._random_assignment(emails)
    to_send = []
    assignments.each do |a_id, threads|
      to_send += self.format_emails(threads, a_id)
    end

    e_sender = EmailSender.new
    to_send.shuffle.each { |s| e_sender.send_thread(s[0], s[1], nil, true, true) }
  ensure
    self.save
  end

  def start_measurement(reloads, async = false, queue_name = :default, to_reload = :all)
    self.measurements.push :start => Time.now.utc, :reloads => reloads
    if async
      GmailScraper.scrap_accounts_async(self.name, reloads, queue_name)
    else
      GmailScraper.scrap_accounts(self.name, reloads, to_reload)
    end
  ensure
    self.save
  end

  def self.by_name(name)
    Experiment.where(name: name).first
  end

  def self.prepare_data(exp_names, logging = false)
    exp_names.each do |exp_name|
      Mongoid.with_tenant(exp_name) do
        puts "start preparing data"
        Experiment.delete_redis_cache
        puts "maped" if logging
        Email.redo_clustering
        puts "e clusterd" if logging
        Ad.redo_clustering
        puts "a clusterd" if logging
      end
    end
  end

  def self.analyse_exp(name)
    self.build_indexes(name)
    Experiment.prepare_data([name])
    Mongoid.with_tenant(name) do
      Ad.recompute_scores([:bool_behavior, :context, :set_intersection], false, false)
      Ad.compute_ad_data
    end
    # otherwise redis blows up
    Experiment.delete_redis_cache
    Experiment.by_name(name).tap do |e|
      e.analyzed = true
      e.save
    end
  end

  def self.reanalyse_all
    Experiment.each do |exp|
      Mongoid.with_tenant(exp.name) do
        Experiment.analyse_exp(exp.name)
      end
    end
  end

  def self.recompute_data_all
    Experiment.each do |exp|
      Mongoid.with_tenant(exp.name) do
        Ad.compute_ad_data
      end
    end
  end

  def self.recompute_data(name)
    Mongoid.with_tenant(name) do
      Ad.compute_ad_data
    end
  end

  def self.analyse_large_itr(nbs)
    nbs.each do |nb|
      exp_name = "large_itr#{nb}"
      Mongoid.with_tenant(exp_name) do
        Ad.compute_ad_data
      end
      Statistics.analyse_large_itr(nb)
      Statistics.analyse_large_itr_set_intersection(nb)
    end
  end
end

require 'descriptive_statistics'
class Account
  include Mongoid::Document
  include Mongoid::Timestamps

  field :used
  field :is_master
  field :login

  has_many :snapshots
  has_many :account_snapshot_clusters, dependent: :destroy


  @recommendation_snapshots = []
  @product_snapshots = []

  items = %w(ad email product recommendation)
  items.map do |item|
    sn_name = "#{item}_snapshot"
    define_method(sn_name.pluralize) { snapshots.where(_type: sn_name.classify) }
    rel_name = "account_#{item}"
    define_method(rel_name.pluralize) { account_snapshot_clusters.where(_type: rel_name.classify) }
  end

  def self.get_amazon_accounts
    ps_label = "ps"
    rs_label = "rs"
    ret_accounts = Hash.new()

    Mongoid.with_tenant("amazon-1") do
      ProductSnapshot.each do |ps|
        acc = ps.account
        if !ret_accounts.include?(acc)
          ret_accounts[acc] = Hash.new()
          ret_accounts[acc][ps_label] = Set.new()
          ret_accounts[acc][rs_label] = Set.new()
        end
        ret_accounts[acc][ps_label].add(ps)
      end
      RecommendationSnapshot.each do |rs|
        acc = rs.account
        if !ret_accounts.include?(acc)
          ret_accounts[acc] = Hash.new()
          ret_accounts[acc][ps_label] = Set.new()
          ret_accounts[acc][rs_label] = Set.new()
        end
        ret_accounts[acc][rs_label].add(rs)
      end
    end
    return ret_accounts
  end

  def self.write_amazon_acc_data(datapath, amazon_accs)
    acc_data = []
    amazon_accs.keys().each do |acc|
      acc_id = acc.id
      ps_count = amazon_accs[acc]["ps"].length
      rs_count = amazon_accs[acc]["rs"].length
      acc_data.push([acc_id, ps_count, rs_count].join(" "))
    end
    data_file = File.open(datapath, "w")
    data_file.write(acc_data.join("\n"))
    data_file.close()

  end

  def self.write_amazon_statistics(datapath, amazon_accs)
    rec_prod = Hash.new()
    amazon_accs.keys().each do |acc|
      ps_count = amazon_accs[acc]["ps"].length
      rs_count = amazon_accs[acc]["rs"].length
      if !rec_prod.include?(ps_count)
        rec_prod[ps_count] = []
      end
      rec_prod[ps_count].push(rs_count)
    end

    stat_data = ["count min q1 median q3 max"]
    rec_prod.keys().each do |ps_count|
      desc_stats = rec_prod[ps_count].descriptive_statistics
      data_line = "#{ps_count} #{desc_stats[:min]} #{desc_stats[:q1]} #{desc_stats[:median]} #{desc_stats[:q3]} #{desc_stats[:max]}"
      stat_data.push(data_line)
    end
    puts stat_data
    data_file = File.open(datapath, "w")
    data_file.write(stat_data.join("\n"))
    data_file.close()

  end

  def self.write_amazon_account_data(pathname=File.join(Rails.root, "data/amazon"))
    xy_filename = "amazon_acc.dat"
    stats_filename = "amazon_acc_stats.dat"
    amazon_accs = Account.get_amazon_accounts

    write_amazon_acc_data(File.join(pathname, xy_filename), amazon_accs)
    write_amazon_statistics(File.join(pathname, stats_filename), amazon_accs)
  end

  def self.get_wish_list_permuations
    combinations = Hash.new()
    amazon_accs = get_amazon_accounts()

    amazon_accs.keys().each do |acc|
      ps_ids = amazon_accs[acc]["ps"].collect{|x| x._id}.sort()
      if !combinations.include?(ps_ids.length)
        combinations[ps_ids.length] = Set.new()
      end
      combinations[ps_ids.length].add(Set.new(ps_ids))
      puts ps_ids
    end
    return combinations
  end

  def get_product_snapshots()
    p_snaps = []
      snapshots.each do |snap|
        if snap.class == ProductSnapshot
          p_snaps.push(snap)
        end
      end
      return p_snaps
  end

  def get_recommendation_snapshots()
    if @recommendation_snapshots == nil || @recommendation_snapshots.length == 0
      @recommendation_snapshots = []
      snapshots.each do |snap|
        if snap.class == RecommendationSnapshot
          @recommendation_snapshots.push(snap)
        end
      end
    end
    return @recommendation_snapshots
  end

  def reset_snapshot_caches()
    @recommendation_snapshots = []
    @product_snapshots = []
  end

  def has_cluster?(cluster_id)
    AccountSnapshotCluster.where( account: self, snapshot_cluster_id: cluster_id ).count > 0
  end

  def self.master_account
    exp = Experiment.where( :name => Mongoid.tenant_name ).first
    if exp == nil
      Account.where(:is_master => true).first
    else
    Account.where( id: exp.master_account ).first
    end
  end
end

class GoogleAccount < Account
  field :passwd
  field :gmail
  field :voice_number
  field :voice_verifs, default: 0
  field :email_verifs, default: 0
  field :emails_labeled
  field :phone
  field :gender
  field :first_name
  field :last_name
  field :email
  field :bd
  field :bm
  field :by

  field :checked, :default => false
  index({ login: 1 }, { :unique => true })

  def self.label_all_emails(exp_name)
    Mongoid.with_tenant(exp_name) do
      GoogleAccount.each_with_index { |acc, i| acc.label_emails(exp_name, i) }
    end
  end

  def related_email_subjects
    AccountEmail.where( account: self ).all.map { |ae| ae.email.random_snapshot.subject }
  end

  def label_emails(exp_name, index)
    return if self.emails_labeled

    puts "#{exp_name}: new account #{index}"
    imap = Gmail.connect!(self.login, self.passwd)
    emails = imap.inbox.all.map do |e|
      [e.gmsg_id.to_s(16), e.gthread_id.to_s(16), e.subject, e.body]
    end
    imap.logout
    emails.each do |email|
      cands = Experiment.where( name: exp_name ).first.emails.map.with_index do |e, i|
        #
        # TODO support experiment threads with multiple emails
        #
        e = e.first
        e['subject'] == email[2] && email[3].include?(e['text'] || e['html']) ? i : nil
      end.compact
      if cands.count == 1
        EmailSnapshot.where( account: self, e_id: email[0]).each do |e_sn|
          e_sn.exp_e_id = cands.first
          e_sn.save
        end
      end
    end
    self.emails_labeled = true
    self.save
  end

  def self.available_accounts
    GoogleAccount.where( "used" => nil )
  end
end

class GmailAPI
  class AuthError < RuntimeError; end

  def self.test
    acc = { login: "CUCloudAudit1", passwd: "CUCloudAudit" }
    api = self.new(acc)
    api.login
    api.get_ads_for("143e519f55e223f3")
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
      "PersistentCookie" => "yes",
      "signIn" => "Sign in",
    }
    raise AuthError if response.body =~ /incorrect/
    return self
  end

  def hashify(data)
    Hash[data.map { |v| [v[0], v[1..-1]] }]
  end

  def get_ads_for(mid)
    r = @conn.get("https://mail.google.com/mail/?view=ad&th=#{mid}&search=inbox")
    ads = ExecJS.eval "eval(#{r.body[6..-1].force_encoding('UTF-8')})"
    ads = hashify(ads.first)


    items = []
    side_ads = [*ads['ads'][0]] rescue []
    items.concat side_ads.compact.map { |v| { :full_id     => v[7],
                                              :campaign_id => v[7].split('_')[0],
                                              :name        => v[0..2].join(" "),
                                              :click       => v[3],
                                              :url         => v[4]} }

    # if we want the top ads
    # top_ads = ads['fb'][1] rescue []
    # items.concat top_ads.compact.map { |v| { :full_id     => v[6],
                                             # :type        => v[0..1].join(" "),
                                             # :name        => v[2..3].join(" "),
                                             # :click       => v[4],
                                             # :url         => v[4]} }

    return items
  rescue => e
    puts "[GmailAPI] get_ads error"
    raise e
  end
end

class GmailScraper
  def scrap_account(account, iteration, to_reload = :all)
    puts "START it: #{iteration} acc: #{account.id}"
    ip = LocationHelper._get_ext_ip
    imap = Gmail.connect!(account.login, account.passwd)
    emails = imap.inbox.all.map do |e|
      if to_reload.to_s == "all" || to_reload.include?(e.subject)
        e.read!
        # [e.msg_id.to_s(16), e.thread_id.to_s(16), e.subject, e.body.to_s]
        [e.msg_id.to_s(16), e.thread_id.to_s(16),
         e.subject, EmailSnapshot.cached_body(account.login, e.msg_id.to_s(16))]
      else
        nil
      end
    end.compact
    imap.logout
    api = GmailAPI.new(account).login
    emails.each do |e|
      e_id = e[0]
      t_id = e[1]
      puts "NEW EMAIL it: #{iteration} e_id: #{e_id} t_id: #{t_id} acc: #{account.id}"
      exp = Experiment.current
      exp_e_id = exp.email_index_from_title(e[2], e[3])
      # exp_email = exp.emails[exp_e_id].first["text"] rescue nil
      email_snapshot = EmailSnapshot.create!({ :account   => account,
                                               :iteration => iteration,
                                               :e_id      => e_id,
                                               :t_id      => t_id,
                                               :subject   => e[2],
                                               # :body      => e[3],
                                               # :text      => exp_email,
                                               :exp_e_id  => exp_e_id,
                                               :ip        => ip, })
      ads = api.get_ads_for(e_id)
      ads.each do |ad_sn|
        puts "NEW AD it: #{iteration} e_id: #{e_id} t_id: #{t_id} acc: #{account.id}"
        ad_sn.merge!({ :account   => account,
                       :iteration => iteration,
                       :context   => email_snapshot,
                       :ip        => ip, })
        AdSnapshot.create!(ad_sn)
        puts "END NEW AD it: #{iteration} e_id: #{e_id} t_id: #{t_id} acc: #{account.id}"
      end
      # sleep 5 + Random.rand(10)
      puts "END NEW EMAIL count:#{ads.count} it: #{iteration} e_id: #{e_id} t_id: #{t_id} acc: #{account.id}"
    end
    puts "END it: #{iteration} acc: #{account.id}"
  end

  def self.scrap_accounts(exp_name, n, to_reload = :all)
    n.times do |i|
      Mongoid.with_tenant(exp_name) do
        GoogleAccount.each { |acc| self.new.scrap_account(acc, i, to_reload) }
      end
    end
  end

  def self.scrap_accounts_async(exp_name, n, queue_name = :default)
    n.times do |i|
      self.scrap_once_async(exp_name, i, queue_name)
    end
  end

  def self.scrap_many_exps_async(exp_names, n, queue_name = :default)
    n.times do |i|
      exp_names.each do |exp_name|
        self.scrap_once_async(exp_name, i, queue_name)
      end
    end
  end

  def self.scrap_once_async(exp_name, i, queue_name = :default)
    Mongoid.with_tenant(exp_name) do
      GoogleAccount.each do |acc|
        if queue_name == :default
          ScrapingWorker.perform_async(exp_name, acc.id.to_s, i)
        else
          Sidekiq::Client.push({
            'class' => ScrapingWorker,
            'queue' => queue_name,
            'args'  => [exp_name, acc.id.to_s, i]
          })
        end
      end
    end
  end
end

class Snapshot
  include Mongoid::Document
  include Mongoid::Timestamps

  #paginates_per 20

  field :object
  field :iteration
  field :signatures  # for test purposes
  field :ip

  field :ported, :default => false

  belongs_to :account
  belongs_to :snapshot_cluster
  belongs_to :context, class_name: 'Snapshot'
  belongs_to :context_cluster, class_name: 'SnapshotCluster'

  index({ account: 1 })
  index({ snapshot_cluster: 1 })
  index({ context: 1 })
  index({ context_cluster: 1 })
  index({ snapshot_cluster: 1 , context_cluster: 1 })
  index({ account: 1 , snapshot_cluster: 1 })
  index({ account: 1 , context_cluster: 1 })
  index({ account: 1 , snapshot_cluster: 1, context_cluster: 1 })

  class_attribute :context_klass
  def self.has_context(klass)
    self.context_klass = klass.to_s.classify.constantize
  end

  # creates and alias for the cluster name
  # in all snapshot subclass
  def self.inherited(c)
    super
    c.send(:define_method, c.name.tableize.split("_").first) do
      self.snapshot_cluster
    end
  end

  def self.cluster_klass
    self.name.tableize.split("_").first.classify.constantize
  end

  def cluster_klass
    self.class.cluster_klass
  end

  def cache_namespace
    "#{Experiment.current.name}:#{self.class.name}"
  end

  def snapshot_cluster
    ns = self.cache_namespace
    cluster_sig_id = Redis.instance.hget("#{ns}:#{self.id}", "cluster")
    self.cluster_klass.where(sig_id_hash: Digest::MD5.base64digest(cluster_sig_id)).first
  end

  validates :account, :presence => true

  def self.set_context_clusters
    self.no_timeout.each do |sn|
      sn.context_cluster = nil
      sn.context_cluster = sn.context.snapshot_cluster if sn.context
      sn.save!
    end
  end

  # if it's one all signatures a unique and clustering can
  # be optimized
  def self.signatures_count
    return 0
  end
end

class EmailSnapshot < Snapshot
  field :e_id      # the email id in gmail
  field :t_id      # the thread id in gmail
  field :exp_e_id  # the email id in the experiment (the email index in emails)
                   # created from gmail_api.rb
  field :subject
  field :text
  field :body

  index({ e_id: 1 })
  index({ account: 1, e_id: 1 })
  index({ account: 1, subject: 1 })

  field :outsider, :default => false

  def self.signatures_count
    return 1
  end

  def signatures
    return super if super
    s = self.subject || Redis.instance.hget("#{self.account.login}:#{self.e_id}", "subject")
    b = self.body || Redis.instance.hget("#{self.account.login}:#{self.e_id}", "body")
    b = nil if b == " " || b == ""
    t = Experiment.current.email_body_from_title(s,b)
    [s, t].compact.to_json.to_a

    # self.subject.to_a
    # (self.exp_e_id || self.subject).to_s.to_a
  end

  def self.cached_body(acc_login, email_id)
    Redis.instance.hget("#{acc_login}:#{email_id}", "body")
  end

  def self.cached_subject
    Redis.instance.hget("#{acc_login}:#{email_id}", "subject")
  end

  # for the fancy one not with titles, see google_account
  def self.map_emails_to_exp
    emails = Experiment.where( :name => Mongoid.tenant_name ).first.emails
    emails.each_with_index do |email, i|
      email.each do |e|
        s = e['subject']
        EmailSnapshot.where( subject: s ).each do |sn|
          sn.exp_e_id = i
          sn.save
        end
      end
    end
  end
end

class AdSnapshot < Snapshot
  field :url
  field :name
  field :click
  field :account_id
  field :campaign_id
  field :full_id

  index({ campaign_id: 1 })
  index({ url: 1 })

  has_context :email_snapshot

  def redirect_url
    uri = URI.parse(CGI.parse(self["click"])["adurl"].first)
    "http://#{uri.host}#{uri.path}"
  rescue nil
  end

  def self.signatures_count
    return 1
  end

  def signatures
    return super if super
    [
      # self.campaign_id == "0" ? nil : self.campaign_id,
      # self.name,
      self.url,
    ].compact
  end

  def self.clean
    AdSnapshot.each { |ad| ad.destroy if ad.signatures.length == 0 }
  end
end

require 'open-uri'
class SnapshotCluster
  include Mongoid::Document
  include Mongoid::Timestamps

  has_many :snapshots
  has_many :account_snapshot_clusters, dependent: :destroy

  field :distr, default: {}
  field :signature_id # the first element of sorted signatures
  field :sig_id_hash
  index({ sig_id_hash: 1 })
  field :signatures
  field :acc_distr
  field :context_distr
  field :acc_context_distr
  field :in_master
  index({ in_master: 1 })

  # for matching
  field :log_footprint
  field :matched
  field :match_group
  index({ match_group: 1 })

  # keep scores
  has_many :targeting_scores, dependent: :destroy
  # field :scores, :default => {}
  # field :tmp_scores, :default => {}


  class_attribute :context_klass
  def self.has_context(klass)
    self.context_klass = klass.to_s.classify.constantize
  end

  class_attribute :signature_klass
  def self.has_signature(klass)
    self.signature_klass = klass.to_s.classify.constantize
  end

  # creates and alias for the account  # in subclasses
  def self.inherited(c)
    super
    c.send(:define_method, "account_#{c.name.downcase.pluralize}") do
      self.account_snapshot_clusters
    end
  end

  class_attribute :snapshot_klass
  def self.cluster_of(items)
    self.snapshot_klass = items.to_s.classify.constantize
  end

  cluster_of :snapshots

  def self.relation_klass
    "Account#{self.name}".classify.constantize
  end

  def create_relationship(account_ids)
    relation_klass = self.class.relation_klass
    account_ids.each do |id|
      relation_klass.create(account_id: id,
                            snapshot_cluster:self) if relation_klass.where(account_id: id, snapshot_cluster:self).count == 0
    end
  end

  def cache_namespace
    "#{Experiment.current.name}:#{self.class.name}"
  end

  def snapshots
    ns = self.cache_namespace
    item_ids = Redis.instance.smembers("#{ns}:#{self.signature_id}")
    item_ids.map do |id|
      self.snapshot_klass.where(id: id).first
    end
  end

  def snapshots_count
    ns = self.cache_namespace
    Redis.instance.scard("#{ns}:#{self.signature_id}")
  end

  def random_snapshot
    ns = self.cache_namespace
    item_id = Redis.instance.srandmember("#{ns}:#{self.signature_id}")
    self.snapshot_klass.where(id: item_id).first
  end

  # accounts this cluster is in
  def related_accounts
    self.class.related_accounts(self.id.to_s)
  end

  def self.related_accounts(cluster_id)
    AccountSnapshotCluster.where(snapshot_cluster_id: cluster_id)
                          .all.map { |asc| asc.account }.uniq
  end

  def self.in_master
    self.no_timeout.where(in_master: true).all
  end

  def self.compute_cache
    # signature_id => signatures
    signature_map = {}
    # item_id => {cluster => cluster_signature_id,
    #             context => context_cluster_signature_id,
    #             account => account_id}
    item_info_map = {}
    # signature_id => [item_id]
    cluster_map = {}
    # signature_id => account => number of occurences
    cluster_account_distr = {}
    # signature_id => context => number of occurences
    cluster_context_distr = {}
    # signature_id => account => email => number of occurences
    cluster_account_context_distr = {}
    # signature_id => in_master?
    cluster_master_map = {}

    # signature => item_ids
    items_map = {}
    # signature => signatures
    sigs_map = {}
    snapshot_klass.no_timeout.each do |item|
      sigs = item.signatures.uniq
      sigs.each do |sig|
        sigs_map[sig]  ||= Set.new
        items_map[sig] ||= Set.new
        sigs_map[sig].merge(sigs)
        items_map[sig].add(item.id)
        item_info_map[item.id] ||= {}
        item_info_map[item.id]["account"] = item.account.id
        if item.context
          item_info_map[item.id]["context"] = item.context.snapshot_cluster.id
          # item_info_map[item.id]["context"] = item.context.snapshot_cluster.signature_id
        end
      end
    end

    # Array[Array[signatures, item_ids]] each inside array is a cluser
    clusters = sigs_map.keys.map do |sig|
      next unless sigs_map[sig]

      # get all cluster's signatures
      sigs = sig.to_a.to_set
      loop do
        old_sigs, sigs = sigs, sigs.reduce(Set.new) { |set, s| set.merge(sigs_map[s]) }
        break if old_sigs == sigs
      end
      # get all items in cluster and ivalidate related sigs
      sorted_sigs = sigs.map(&:to_s).uniq.sort
      sig_id = sorted_sigs.first
      signature_map[sig_id] = sorted_sigs
      items_in_cluster = sorted_sigs.reduce(Set.new) do |set, s|
        sigs_map[s] = nil
        set.merge(items_map[s])
      end
      # return the cluster id and the items
      [sig_id, items_in_cluster]
    end.compact

    master_acc_id = Account.master_account
    clusters.each do |cluster|
      sig_id = cluster.first
      items_in_cluster = cluster.last
      # update the cluster value of the items
      items_in_cluster.each do |item_id|
        acc = item_info_map[item_id]["account"]
        ctxt = item_info_map[item_id]["context"]
        # remember number of occurences in accounts
        cluster_account_distr[sig_id] ||= Hash.new(0)
        cluster_account_distr[sig_id][acc] += 1
        cluster_master_map[sig_id] = true if acc.to_s == master_acc_id.to_s
        # remember number of occurences in context
        if self.context_klass
          cluster_context_distr[sig_id] ||= Hash.new(0)
          cluster_context_distr[sig_id][ctxt] += 1
          cluster_account_context_distr[sig_id] ||= {}
          cluster_account_context_distr[sig_id][acc] ||= Hash.new(0)
          cluster_account_context_distr[sig_id][acc][ctxt] += 1
        end
        # remember each item's cluster
        item_info_map[item_id]["cluster"] = sig_id
      end
    end

    cluster_map = Hash[clusters]

    return {signature_map: signature_map,
            item_info_map: item_info_map,
            cluster_map: cluster_map,
            cluster_account_distr: cluster_account_distr,
            cluster_context_distr: cluster_context_distr,
            cluster_account_context_distr: cluster_account_context_distr,
            cluster_map: cluster_map,
            cluster_master_map: cluster_master_map}
  end

  # some cluster spcific data to add
  # eg see implementation in Email
  def self.cluster_data(signature_id)
    {}
  end

  def self.write_clusters(signature_map,
                          cluster_account_distr,
                          cluster_context_distr,
                          cluster_account_context_distr,
                          cluster_master_map)
    signature_map.each do |sig_id, sigs|
      cluster = {signature_id: sig_id,
                 sig_id_hash: Digest::MD5.base64digest(sig_id),
                 signatures: sigs,
                 acc_distr: cluster_account_distr[sig_id],
                 context_distr: cluster_context_distr[sig_id],
                 acc_context_distr: cluster_account_context_distr[sig_id],
                 in_master: cluster_master_map[sig_id]}
      self.create!(cluster.merge(self.cluster_data(sig_id)))
    end
  end

  def self.write_relations(cluster_account_distr,
                           cluster_context_distr,
                           cluster_account_context_distr)
    # store AccountSnapshotCluster relation with num
    cluster_account_distr.each do |sig_id, distr|
      begin
      relation_klass = self.relation_klass
      cluster = self.where(sig_id_hash: Digest::MD5.base64digest(sig_id)).first
      distr.each do |acc_id, n_occ|
        relation_klass.create(account_id: acc_id,
                              snapshot_cluster:cluster,
                              n_occurences: n_occ)
      end
      rescue
        binding.pry
      end
    end
    # Do we need?:
    # store SnapshotClusterContext relation with num
    # store AccountSnapshotClusters 3 party relation with Acc Email Ad and number
  end

  def self.cache_in_redis(item_info_map, cluster_map)
    Redis.instance.pipelined do
      ns = "#{Experiment.current.name}:#{self.snapshot_klass.name}"
      item_info_map.each do |item, info|
        Redis.instance.hmset("#{ns}:#{item}", *info.flatten)
      end
      ns = "#{Experiment.current.name}:#{self.name}"
      cluster_map.each do |sig_id, item_set|
        Redis.instance.sadd("#{ns}:#{sig_id}", item_set.to_a)
      end
    end
    return nil
  end

  def self.redo_clustering
    self.delete_all
    self.relation_klass.delete_all
    TargetingScore.delete_all
    self.do_clustering
  end

  def self.do_clustering
    caches = self.compute_cache
    puts "cache"
    self.write_clusters(caches[:signature_map],
                        caches[:cluster_account_distr],
                        caches[:cluster_context_distr],
                        caches[:cluster_account_context_distr],
                        caches[:cluster_master_map])
    puts "clusters"
    self.write_relations(caches[:cluster_account_distr],
                         caches[:cluster_context_distr],
                         caches[:cluster_account_context_distr])
    puts "relations"
    self.cache_in_redis(caches[:item_info_map],
                        caches[:cluster_map])
    puts "redis"
  end


  def self.sort_scores(types, tmp = false)
    threshold = 0.01
    i = 0
    Ad.each do |ad|
      i += 1
      puts i
      types.each do |type|
        old_scores = ad.get_scores(type)
        next if old_scores == nil
        new_scores = []
        nb_scores_to_sort = 0
        old_scores.each_pair do |id, score|
          if score >= threshold
            new_scores.insert(0,{:id => id, :score => score})
            nb_scores_to_sort += 1
          else
            new_scores.push({:id => id, :score => score})
          end
        end
        top_sorted = new_scores[0...nb_scores_to_sort]
        top_sorted.sort!{|x,y| y[:score] <=> x[:score]}
        new_scores = top_sorted + new_scores.drop(nb_scores_to_sort)
        ad.set_scores_sorted(type, new_scores, tmp)
      end
    end
  end

  # analysis to "instanciate" in cluster classes see Ad for an example
  # change with great care and run tests
  #
  # types are "context" or "behavior"

  # the guess for targeting relations
  def targeting_items(types, tmp=false, accounts = :all, input_ids = nil)
    context_weight = (types.include?(:context) ? 1 : 0)
    behavior_weight = (types & [:behavior, :bool_behavior]).count > 0 ? 1 : 0
    behavior_weight += (types & [:bool_behavior_new]).count > 0 ? 1 : 0
    tot_weight = context_weight + behavior_weight
    sc = types.map do |t|
      # don't recompute for context, we always use master only
      if (accounts == :all && t != :bool_behavior_new) || t == :context
        [t, self.get_scores(t, tmp)]
      elsif accounts == :all && t == :bool_behavior_new
        [t, self.get_scores(t, tmp)]
      elsif accounts != :all && t == :bool_behavior_new
        [t, self.compute_scores_bool_behavior_new(tmp, accounts)]
      else
        [t, self.compute_scores(t, tmp, accounts, input_ids)]
      end
    end.reduce(Hash.new(0)) do |glbl, s|
      weight = s.first == :context ? context_weight : behavior_weight
      s = s.last
      s.keys.each do |k|
        score = weight * s[k].to_f / tot_weight
        glbl[k] += score.nan? ? 0 : score
      end
      glbl
    end

    # void_score = sc.delete('')
    max_sc = sc.values.select { |n| !n.to_f.nan? }.max
    cands = sc.select do |id, score|
      !score.to_f.nan? && score >= max_sc * 0.9  && id != "" && id && id != "garbage"
    end

    return [] if cands.count > 2 # || void_score > 2 * max_sc
    cands.keys
  end

  @@score_types = [:context, :behavior, :bool_behavior, :set_intersection]
  class << self
    attr_accessor :parameters  # parameters for targeting models
    attr_accessor :tmp_parameters  # parameters for targeting models
    attr_accessor :data_klass  # the related data class (eg would be email for ad)
  end

  def self.targeted_by(klass)
    self.data_klass = klass
  end

  def self.set_params(type, data, tmp = false)
    if tmp
      (self.tmp_parameters ||= {})[type] = data
    else
      (self.parameters ||= {})[type] = data
    end
  end

  def self.get_params(type, tmp = false)
    tmp ? (self.tmp_parameters ||= {})[type] : (self.parameters ||= {})[type]
  end

  def get_scores(type, tmp=false)
    score_klass = "#{type.to_s.camelize}Score".constantize
    score = score_klass.where(snapshot_cluster: self).first
    return nil if score == nil
    res = tmp ? score.tmp_scores : score.scores
    res || (type == :bool_behavior_new ? self.compute_scores_bool_behavior_new(tmp, :all) : self.compute_scores(type, tmp))
  end

  def get_scores_sorted(type, tmp=false)
    score_klass = "#{type.to_s.camelize}ScoreSorted".constantize
    score = score_klass.where(snapshot_cluster: self).first
    return nil if score == nil
    res = tmp ? score.tmp_scores : score.scores
    res || (type == :bool_behavior_new ? self.compute_scores_bool_behavior_new(tmp, :all) : self.compute_scores(type, tmp))
  end

  def set_scores(type, scores, tmp=false)
    score_klass = "#{type.to_s.camelize}Score".constantize
    score = score_klass.where(snapshot_cluster: self).first
    score ||= score_klass.create(snapshot_cluster: self)
    if tmp
      score.tmp_scores = scores
    else
      score.scores = scores
    end
    score.save
  end

  def set_scores_sorted(type, scores, tmp=false)
    score_klass = "#{type.to_s.camelize}ScoreSorted".constantize
    score = score_klass.where(snapshot_cluster: self).first
    score ||= score_klass.create(snapshot_cluster: self)
    if tmp
      score.tmp_scores = scores
    else
      score.scores = scores
    end
    score.save
  end  

  #
  # compute the scores
  #

  # subclass that if you don't want some clusters to be counted as valid data
  # regarding targeting ; in that case return "garbage" instead
  #
  # eg in Email.rb
  def garbage?
    return self.cluster_targeting_id == "garbage"
  end

  def cluster_targeting_id
    self.id.to_s
  end

  def self.recompute_scores(types = @@score_types, tmp = false, master_only = false)
    # Compute list of emails in each account if type is :bool_behavior_new
    input_ids ||= self.data_klass.all.select{ |x| !x.garbage? }.map { |x| x.id.to_s }
    if types.include?(:set_intersection)
      # compute account => emails mapping
      acc_input_map = {}
      input_ids.each do |input_id|
        self.related_accounts(input_id).each do |acc|
          acc_input_map[acc.id.to_s] ||= []
          acc_input_map[acc.id.to_s].push input_id
        end
      end
    end
    puts "here"
    if !master_only
      self.all.no_timeout.each_with_index do |ad, i|
        print "\r> Item " + "#{i+1}".green
        types.each do |t|
          if t != :set_intersection
            ad.compute_scores(t, tmp, :all, input_ids)
          else
            ad.compute_scores_set_intersection(tmp, :all, input_ids, acc_input_map)
          end
        end
      end
    else
      self.no_timeout.in_master.each_with_index do |ad, i|
        print "\r> Item " + "#{i+1}".green
        types.each do |t|
          if t != :set_intersection
            ad.compute_scores(t, tmp, :all, input_ids)
          else
            ad.compute_scores_set_intersection(tmp, :all, input_ids, acc_input_map)
          end
        end
      end
    end
    puts " Done!"
  end

  def compute_scores(type, tmp = false, accounts = :all, input_ids = nil)
    to_save = (accounts == :all)
    # We want everything for context
    # accounts = [Account.master_account] if type == :context && accounts == :all

    # scores = self.class.data_klass.all.map do |e|
    # FALSE cores = self.context_distr.keys.map do |eid|
    input_ids ||= self.class.data_klass.all.map { |x| x.id.to_s }
    scores = input_ids.map do |eid|
      # tarid = e.cluster_targeting_id
      # tarid != "garbage" ? [tarid, self.send("p_x_e_a_#{type}", e, tmp, accounts)] : nil
      [eid, self.send("p_x_e_a_#{type}", eid, tmp, accounts)]
    end.compact.push([nil, self.send("p_x_void_a_#{type}", tmp, accounts)])  # for void email (ie not targeted)
    tot = scores.reduce(0) { |sum, e| sum + e[1] }
    sc = Hash[scores.map { |s| [s[0], s[1] / tot] }]
    if to_save
      self.set_scores(type, sc, tmp)
    end
    sc
  end

  def compute_scores_set_intersection(tmp = false, accounts = :all, input_ids = nil, acc_input_map = nil)
    to_save = (accounts == :all)

    input_ids ||= self.class.data_klass.all.map { |x| x.id.to_s } unless input_ids != nil
    if acc_input_map == nil
      acc_input_map = {}
      input_ids.each do |input_id|
        self.related_accounts.each do |acc|
          acc_input_map[acc.id.to_s] ||= []
          acc_input_map[acc.id.to_s].push input_id
        end
      end
    end

    # to_save = (accounts == :all)
    params = self.class.get_params(:set_intersection)
    x = params[:account_poportion_threshold]
    max_size = params[:max_combination_size]

    if accounts == :all
      active_accounts = self.acc_distr.select { |k,v| v > 0 }.keys
    else
      active_accounts = (self.acc_distr.select { |k,v| v > 0 }.keys & accounts)
    end

    tot_active_accounts = active_accounts.count
    covered_aaccs = 0
    targeted_inputs = []
    while covered_aaccs.to_f / tot_active_accounts.to_f < x && targeted_inputs.count < max_size
      # input => list of covered active accounts
      covered_active_accounts = {}
      active_accounts.each do |acc_id|
        acc_input_map[acc_id].each do |input_id|
          covered_active_accounts[input_id] ||= []
          covered_active_accounts[input_id].push acc_id
        end
      end
      input_id, accs = covered_active_accounts.sort_by {|k,v| v.count}.last
      targeted_inputs.push input_id
      active_accounts = active_accounts - accs
      covered_aaccs += accs.count
    end

    targeted = covered_aaccs.to_f / tot_active_accounts.to_f >= x && targeted_inputs.count <= max_size
    result = {:targeted              => targeted,
              :proportion_covered    => covered_aaccs.to_f / tot_active_accounts.to_f,
              :combination_size      => targeted_inputs.count,
              :active_accounts_tot_n => tot_active_accounts,
              :targeting_result      => targeted_inputs}
    if to_save
      self.set_scores(:set_intersection, result, tmp)
    end
    result
  end

  def p_x_e_a_context(data_id, tmp = false, accounts = :all)
    params = self.class.get_params(:context, tmp)
    if accounts == :all
      tot = self.acc_distr.values.sum
      data_tot = (self.context_distr[data_id] || 0)
    else
      tot = accounts.map { |acc| self.acc_distr[acc] || 0 }.sum
      data_tot = accounts.map do |acc|
        self.acc_context_distr[acc][data_id] || 0 rescue 0
      end.sum
    end
    tot, data_tot = 100, data_tot * 100.0 / tot if tot > 100
    params[:p] ** data_tot * params[:q] ** (tot - data_tot)
  end

  def p_x_void_a_context(tmp = false, accounts = :all)
    if accounts == :all
      tot = self.acc_distr.values.sum
    else
      tot = accounts.map { |acc| self.acc_distr[acc] || 0 }.sum
    end
    params = self.class.get_params(:context, tmp)
    params[:r] ** [tot, 100].min
  end

  def p_x_e_a_bool_behavior(data_id, tmp = false, accounts = :all)
    params = self.class.get_params(:bool_behavior, tmp)
    if accounts == :all
      n_acc = Account.count
      acc_e = SnapshotCluster.related_accounts(data_id).map { |a| a.id.to_s }
      n_acc_e = acc_e.count
      a_in = self.acc_distr.select { |k,v| v > 0 }.keys
      n_a_in = a_in.count
      a_e_in = (a_in & acc_e).count
    else
      n_acc = accounts.count
      n_acc_e = (SnapshotCluster.related_accounts(data_id).map{ |acc| acc.id.to_s } & accounts).count
      n_a_in = (self.acc_distr.select { |k,v| v > 0 }.keys & accounts).count
      a_e_in = (self.acc_context_distr.select { |k,v| (v[data_id] || 0) > 0 }.keys & accounts).count
    end

    a_ne_in = n_a_in - a_e_in
    a_e_out = n_acc_e - a_e_in
    a_ne_out = n_acc - n_a_in - a_e_out

    a_e_in, a_ne_in, a_e_out, a_ne_out = [a_e_in, a_ne_in, a_e_out, a_ne_out].each { |v| v * 100.0 / n_acc } if n_acc > 100
    p, q = params[:p], params[:q]
    p ** a_e_in * q ** a_ne_in * (1-p) ** a_e_out * (1-q) ** a_ne_out
  end

  def p_x_void_a_bool_behavior(tmp = false, accounts = :all)
    params = self.class.get_params(:bool_behavior, tmp)
    if accounts == :all
      tot = Account.count
      tot_in = self.acc_distr.select { |k,v| v > 0 }.count
    else
      tot = accounts.count
      tot_in = (self.acc_distr.select { |k,v| v > 0 }.keys & accounts).count
    end
    tot_out = tot - tot_in
    tot_in, tot_out = tot_in * 100.0 / tot, tot_out * 100.0 / tot if tot > 100
    r = params[:r]
    r ** tot_in * (1-r) ** tot_out
  end

  # TODO unbreak that
  #
  # try to guess the parameters for the model

  # runs a loop to learn (unsupervise) the parameters of the model
  def self.recompute_all_params
    @@score_types.map { |t| self.compute_parameters(t) if t != :bool_behavior_new}  # map so that it prints
  end

  def self.compute_parameters(type)
    self.set_params(type, { :p => 0.7, :q => 0.01, :r => 0.2 }, true)
    # self.set_params(type, { :p => 0.3, :q => 0.001, :r => 0.002 }, true)
    loop do
      puts "start loop"
      self.recompute_scores([type], true, true)
      puts "compute distr"
      old_distr = self.get_params(type, true)
      new_distr = self.set_params(type, self.distribution(type, true), true)
      puts old_distr
      puts new_distr
      change = new_distr.keys.reduce(0) { |sum, k| sum += (old_distr[k] - new_distr[k]).abs }
      puts change
      break if change < 0.01
    end
    self.get_params(type, true)  # return params at the end
  end

  # call that before you compute parameters
  def self.compute_context_distrs
    self.no_timeout.in_master.each do |item|
      item.distr[:context.to_s] = Hash.new(0)
      item.class.context_klass.no_timeout.each do |ctxt|
        next if ctxt.cluster_targeting_id == 'garbage'
        # item.distr[:context.to_s][ctxt.id.to_s] = Snapshot.where( snapshot_cluster: item, context_cluster: ctxt ).count
        # only master for context?
        master = Account.master_account
        item.distr[:context.to_s][ctxt.id.to_s] = master.snapshots.where( snapshot_cluster: item, context_cluster: ctxt ).count
      end
      item.save
    end
  end
  def self.compute_behavior_distrs
    self.no_timeout.in_master.each do |item|
      item.distr[:behavior.to_s] = Hash.new(0)
      Account.no_timeout.each do |acc|
        item.distr[:behavior.to_s][acc.id.to_s] = Snapshot.where( account: acc, snapshot_cluster: item ).count
      end
      item.save
    end
  end
  def self.compute_bool_behavior_distrs
    self.no_timeout.in_master.each do |item|
      item.distr[:bool_behavior.to_s] = Hash.new(0)
      Account.no_timeout.each do |acc|
        item.distr[:bool_behavior.to_s][acc.id.to_s] = Snapshot.where( account: acc, snapshot_cluster: item ).count > 0 ? 1 : 0
      end
      item.save
    end
  end

  def self.compute_bool_behavior_new_distrs
    self.no_timeout.in_master.each do |item|
      item.distr[:bool_behavior_new.to_s] = Hash.new(0)
      Account.no_timeout.each do |acc|
        item.distr[:bool_behavior_new.to_s][acc.id.to_s] = Snapshot.where(account: acc, snapshot_cluster: item).count > 0 ? 1 : 0
      end
      item.save
    end
  end

  def self.distribution(type, tmp = true)
    targ = untarg = 0

    self.no_timeout.in_master.map do |item|
      puts "new item"
      itm_targ = []
      case type
      when :context
        itm_targ = item.targeting_items([type], tmp)
        tot = tot_targ = tot_wrong_targ = item.snapshots_count
      when :behavior
        itm_targ = item.targeting_items([type], tmp).map { |t| SnapshotCluster.related_accounts(t) }
                       .flatten.map(&:id).map(&:to_s).uniq
        tot = tot_targ = tot_wrong_targ = item.snapshots_count
      when :bool_behavior
        itm_targ = item.targeting_items([type], tmp).map { |t| SnapshotCluster.related_accounts(t) }
                       .flatten.map(&:id).map(&:to_s).uniq
        # tot = GoogleAccount.count
        # tot_targ = itm_targ.count
        # tot_targ = SnapshotCluster.related_accounts(item).count
        # tot_wrong_targ = tot - tot_targ
        tot = tot_targ = tot_wrong_targ = SnapshotCluster.related_accounts(item).count
      when :bool_behavior_new
        itm_targ = item.targeting_items([type], tmp, :all)
                       .map { |t| SnapshotCluster.related_accounts(t) }
                       .flatten.map(&:id).map(&:to_s).uniq
        # tot = GoogleAccount.count
        # tot_targ = itm_targ.count
        # tot_targ = SnapshotCluster.related_accounts(item).count
        # tot_wrong_targ = tot - tot_targ
        tot = tot_targ = tot_wrong_targ = SnapshotCluster.related_accounts(item).count
      end


      if itm_targ.count > 0
        targ += 1
        # the [0] is a hack to avoid dividing by zero
        ps = itm_targ.map { |id| item.distr[type.to_s][id].to_f / tot_targ }
        ps = [0] if ps.count == 0
        qs = (item.distr[type.to_s].keys - itm_targ).map { |id| tot_wrong_targ == 0 ? 0 : item.distr[type.to_s][id].to_f / tot_wrong_targ }
        qs = [0] if qs.count == 0
        { :p => ps.sum.to_f / ps.size, :q => qs.sum.to_f / qs.size, :r => 0 }
      else
        untarg += 1
        rs = item.distr[type.to_s].values.map { |v| v.to_f / tot }
        rs = [0] if rs.count == 0
        { :p => 0, :q => 0, :r => rs.sum.to_f / rs.size }
      end
    end.reduce(Hash.new(0)) do |agg, vals|
      agg[:p] += targ > 0 ?  vals[:p] / targ : 0
      agg[:q] += targ > 0 ? vals[:q] / targ : 0
      agg[:r] += untarg > 0 ? vals[:r] / untarg : 0
      agg
    end
  end

  # matching
  def log_footprint
    return super if super
    log_footprint!
  end

  def log_footprint!
    self.log_footprint = Hash.new(0)
    self.class.signature_klass.each do |ad|
      ne = Snapshot.where( snapshot_cluster: ad, context_cluster: self ).count
      self.log_footprint[ad.id.to_s] = Math.log(ne + 1)
    end
  ensure
    self.save
  end

  def self.recompute_log_footprints
    self.no_timeout.each(&:log_footprint!)
  end
end

class Ad < SnapshotCluster
  cluster_of :ad_snapshots
  has_context :email

  has_many :account_ads
  has_many :gmail_truths

  field :labeled
  index({ labeled: 1 })
  index({ labeled: 1, in_master: 1 })
  index({ labeled: 1, _type: 1 })
  index({ labeled: 1, in_master: 1, _type: 1 })

  field :targeting_email_id
  field :strong_association, :default => false
  field :targeting_data
  index({ targeting_email_id: 1, strong_association: 1 })

  def self.compute_ad_data
    i = 0
    Ad.no_timeout.each { |ad| ad.compute_data; puts i; i += 1 }
  end

  def compute_data
    data = Hash.new
    
    sn = AdSnapshot.where(url: self.signature_id).first
    data["text"] = sn.name
    data["url"] = sn.url

    # data["text"] = self.random_snapshot.name
    # data["url"] = self.random_snapshot.url

    # bbn = self.targeting_items([:bool_behavior_new])
    bb  = self.targeting_items([:bool_behavior])
    mix = self.targeting_items([:context, :bool_behavior])
    # bbn = [] if bbn.count > 1
    bb  = [] if bb.count > 1
    mix = [] if mix.count > 1

    # targ = bbn | bb | mix
    targ = bb | mix
    self.strong_association = false
    if targ.count == 1
      eid = targ.first
      self.targeting_email_id = eid
      email = Email.where(id: eid).first
      
      # accounts_with_email = SnapshotCluster.related_accounts(email.id.to_s).map { |a| a.id.to_s }
      accounts_with_ad = self.acc_distr.select { |k,v| v > 0 }.keys
      # number_accounts_with_email_and_ad = (accounts_with_ad & accounts_with_email).count
      if accounts_with_ad.count >= 3 #and accounts_with_ad.count <= number_accounts_with_email_and_ad + 1
      # if true
        accounts_with_email = SnapshotCluster.related_accounts(email.id.to_s).map { |a| a.id.to_s }
        # accounts_with_ad = self.acc_distr.select { |k,v| v > 0 }.keys
        number_accounts_with_email_and_ad = (accounts_with_ad & accounts_with_email).count
        
        self.strong_association = true
        context = self.get_scores(:context)[email.id.to_s] || 0
        behavior = self.get_scores(:bool_behavior)[email.id.to_s] || 0
        mix = (context + behavior).to_f / 2

        data["targeted_subject"] = email.subject
        data["targeted_body"] = email.body
        data["targeted_index"] = email.exp_email_index
        data["mix_score"] = mix
        data["active_accounts"] = accounts_with_ad.count
        data["aa_with_email"] = number_accounts_with_email_and_ad
        data["behavior_score"] = behavior
        data["context_tot"] = self.context_distr.values.sum
        data["context_email"] = self.context_distr[email.id.to_s]
        data["context_score"] = context
      end
    end
    self.targeting_data = data
    self.save
  end

  def has_truth?
    emails_n = Experiment.where( :name => Mongoid.tenant_name ).first.emails.count
    GmailTruth.where( :ad => self ).count >= emails_n
  end

  def to_label?
    self.in_master && !self.has_truth?
  end

  def targeting_truth
    GmailTruth.where( :ad => self, answer: "Yes" ).all.map { |t| t.get_email.id.to_s rescue nil }.compact.uniq
  end

  def self.pre_label
    tot_labeled = 0
    exp = Experiment.where( :name => Mongoid.tenant_name ).first
    emails = exp.emails
    self.in_master.each do |ad|
      next unless ad.to_label?
      targ = []
      ad.snapshots.each do |sn|
        targ = targ + emails.select { |e| e.first['keywords'].select { |kw| sn.name.downcase.include?(kw) }.count > 0 }
      end
      next unless targ.count > 0
      targ = targ.uniq.map { |e| e.first['subject'] }
      tot_labeled += 1
      ad.labeled = true
      ad.save
      emails.each_with_index do |e, i|
        GmailTruth.create({ :exp_email_id => i,
                            :exp_email    => e,
                            :answer       => targ.include?(e.first['subject']) ? 'Yes' : 'No',
                            :ad           => ad,
        })
      end
    end
    tot_labeled
  end

  # analysis "instanciation"
  targeted_by Email

  # parameters for fexp11
  set_params :context, { :p => 0.5886093628762348,
                         :q => 0.046656567492838724,
                         :r => 0.10576599465435517, }
  # parameters for fexp21
  # set_params :context, { :p => 0.544963902667799,
                         # :q => 0.005844784831041431,
                         # :r => 0.009974560263823037, }
  # parameters for sexp2
  # set_params :context, { :p => 0.8946268656716416, :q => 0.005, :r => 0.02497810024847583, }
  # set_params :context, { :p => 0.37102599807619185, :q => 0.0006, :r => 0.004392822426621953, }
  # parameters for sexp4
  # set_params :context, { :p => 0.8921041734388563, :q => 0.0, :r => 0.026028083136413582, }
  # set_params :context, { :p => 0.3634138507017995, :q => 0.001, :r => 0.00633318678447126, }
  # parameters for sexp8
  # set_params :context, { :p => 0.7140415484182788, :q => 0.0035608648786089876, :r => 0.00938982276988097, }
  # set_params :context, { :p => 0.44680726505660795, :q => 0.00022786378708431214, :r => 0.0029328149802187843, }
  # parameters for sexp16
  # set_params :context, { :p => 0.763461208938781, :q => 0.0033736576180375013, :r => 0.010692008133903454, }
  # parameters for sexp32
  # set_params :context, { :p => 0.19986495390845796, :q => 1.0255085392803115e-05, :r => 0.000805940924089732, }
  # parameters for sexp64
  # set_params :context, { :p => 0.2145633265807903, :q => 1.1882765658226003e-05, :r => 0.000510775409361812, }
  # for cexp2t1
  # set_params :context, { :p => 0.1271913284886456, :q => 0.00047228414145287394, :r => 0.001711873502596595 }
  # for cexp2t2
  # set_params :context, { :p => 0.2148695490035347, :q => 0.0008578988832679802, :r => 0.002325205556331765 }
  # for cexp2t3
  # set_params :context, { :p => 0.23050116308897695, :q => 0.00020000471446269643, :r => 0.0049340326798989614 }
  # set_params :context, { :p => 0.44680726505660795, :q => 0.00000786378708431214, :r => 0.0029328149802187843, }

  # parameters for fishy1
  # set_params :context, {:p=>0.3112593113052315, :q=>6.125294184396454e-05, :r=>0.0050397208311149825 }
  # parameters for fishy1r1
  # set_params :context, {:p=>0.2632031514031126, :q=>0.001092997310635848, :r=>0.004206559188794906}
  # parameters for fishy1r2
  # set_params :context, {:p=>0.33383243487215797, :q=>0.0005136990320305005, :r=>0.0037719894922552044}
  # parameters for fishy2
  # set_params :context, {:p=>0.11840764794850106, :q=>0.0023481038679715864, :r=>0.004509292324872822}
  # parameters for fishy2r1
  # set_params :context, {:p=>0.1680684829924382, :q=>0.005717746631519987, :r=>0.003184215872156013}
  # parameters for fishy2r3
  # set_params :context, {:p => 0.13280670939822337, :q => 0.003536307216783638, :r => 0.005310332681592469}

  
  # set_params :behavior, { :p => 0.9, :q => 0.001, :r => 0.4 }
  # for fexp11
  # set_params :behavior, { :p => 0.018940951903738394, :q => 0.0006802290581868325, :r => 0.009900990099009915 }
  # for sexp2
  # set_params :behavior, { :p => 0.17127804416861414, :q => 0.0012557056587976052, :r => 0.1111111111111111 }
  # set_params :behavior, { :p => 0.17876178141201077, :q => 0.006716042465755651, :r => 0.11111111111111108 }
  # set_params :behavior, { :p => 0.17876178141201077, :q => 0.00, :r => 0.11111111111111108 }
  # for sexp4
  # set_params :behavior, { :p => 0.1418518121957588, :q => 0.00, :r => 0.09090909090909069 }
  # set_params :behavior, { :p => 0.1418518121957588, :q => 0.0011258284596369875, :r => 0.09090909090909069 }
  # set_params :behavior, { :p => 0.1418518121957588, :q => 0.00, :r => 0.09090909090909069 }
  # for sexp8
  # set_params :behavior, {:p=>0.08632113345350882, :q=>0.0012258568018596488, :r=>0.047619047619047686}
  # set_params :behavior, {:p => 0.0840145338091019, :q => 0.0013421915270289753, :r => 0.04761904761904779 }
  # set_params :behavior, {:p => 0.0840145338091019, :q => 0.00, :r => 0.04761904761904779 }
  # for sexp16
  # set_params :behavior, {:p => 0.0675715503570463, :q => 0.00024611465324382575, :r => 0.03846153846153852 }
  # set_params :behavior, {:p => 0.0675715503570463, :q => 0.0, :r => 0.03846153846153852 }
  # parameters for sexp32
  # set_params :behavior, { :p => 0.036826042346191176, :q => 0.0008692261730829297, :r => 0.019607843137254843, }
  # for cexp2t1
  # set_params :behavior, { :p => 0.04852973343866087, :q => 0.0040596268087352665, :r => 0.02777777777777774 }
  # for cexp2t2
  # set_params :behavior, { :p => 0.07788658944219427, :q => 0.0016898309410692063, :r => 0.04000000000000001 }
  # for cexp2t3
  # set_params :behavior, { :p => 0.07348260112438175, :q => 0.0007129344488628888, :r => 0.03846153846153848 }
  # set_params :behavior, {:p => 0.0675715503570463, :q => 0.0, :r => 0.03846153846153852 }
  # for fishy1
  # set_params :behavior, {:p => 0.0675715503570463, :q => 0.00024611465324382575, :r => 0.03846153846153852 }

  # for fexp11
  set_params :bool_behavior, { :p => 0.019287906244881078, :q => 0.0002624623547997546, :r => 0.009900990099009885 }
  # for sexp2
  # set_params :bool_behavior, { :p => 0.17708333333333334, :q => 0.0, :r => 0.11111111111111105 }
  # for sexp4
  # set_params :bool_behavior, { :p => 0.1411877394636014, :q => 0.0, :r => 0.09090909090909084 }
  # for sexp8
  # set_params :bool_behavior, { :p => 0.08443163393199649, :q => 0.0006134284735979652, :r => 0.04761904761904735 }
  # set_params :bool_behavior, { :p => 0.08443163393199649, :q => 0.00, :r => 0.04761904761904735 }
  # for sexp16
  # set_params :bool_behavior, { :p => 0.06583133940320847, :q => 0.0016538763609076112, :r => 0.03846153846153854 }
  # for sexp32
  # set_params :bool_behavior, { :p => 0.03688893862891717, :q => 0.000640650826712676, :r => 0.019607843137255 }
  # for sexp64
  # set_params :bool_behavior, { :p => 0.023286106529775485, :q => 0.0004538778518685613, :r => 0.012345679012345737 }
  # for cexp2t1
  # set_params :bool_behavior, { :p => 0.049883872545832236, :q => 0.002610562571075652, :r => 0.02777777777777784 }
  # for cexp2t2
  # set_params :bool_behavior, { :p => 0.07767728250275427, :q => 0.0017189861910221825, :r => 0.039999999999999925 }
  # for cexp2t3
  # set_params :bool_behavior, { :p => 0.07340726303883754, :q => 0.0005148916391519942, :r => 0.038461538461538505 }
  # set_params :bool_behavior, { :p => 0.0916666666666667, :q => 0.0, :r => 0.06666666666666682 }
  # set_params :bool_behavior, { :p => 0.0715823970037454, :q => 0.0, :r => 0.06666666666666682 }
  #
  # ytgm-2
  # set_params :bool_behavior, { :p => 0.07715608841382837, :q => 0.008146743675851088, :r => 0.04000000000000001 }
  # ytgm-2r1
  # set_params :bool_behavior, { :p => 0.0748468073667682, :q => 0.000, :r => 0.03999999999999998}
  # for fishy1
  # set_params :bool_behavior, { :p => 0.3, :q => 0.001, :r => 0.04999999999999987 }
  # for fishy1r1
  # set_params :bool_behavior, {:p=>0.08687386555033626, :q=>0.0008297980520202742, :r=>0.050000000000000024}
  # for fishy1r2
  # set_params :bool_behavior, {:p=>0.08715147570519474, :q=>0.0011151923273135394, :r=>0.05000000000000007}
  # for fishy2
  # set_params :bool_behavior, {:p=>0.08345411928726831, :q=>0.002758052538754293, :r=>0.04999999999999995}
  # for fishy2r1
  # set_params :bool_behavior, {:p=>0.08127629945811765, :q=>0.0021173271173271172, :r=>0.05000000000000008}
  # for fishy2r3
  # set_params :bool_behavior, {:p => 0.15, :q => 0.001, :r => 0.05}

  set_params :set_intersection, {:account_poportion_threshold => 0.92,
                                 :max_combination_size        => 2,}
end

class Email < SnapshotCluster
  cluster_of :email_snapshots
  has_signature :ad

  field :exp_email_index
  index({ exp_email_index: 1 })

  has_many :account_emails

  def cluster_targeting_id(recompute=false)
    # if it's not a cluster of emaisl we sent, it's garbage
    # eg "Welcome to Gmail" email
    unless self.exp_email_index and !recompute
      # add support for outsiders
      self.exp_email_index = Experiment.current.email_index_from_sig_id(self.signature_id)
      self.save!
    end
    (self.exp_email_index && self.subject != "Disclaimer") ? self.id.to_s : "garbage"
  end

  def subject
    JSON.parse(self.signature_id).first
  end

  def body
    JSON.parse(self.signature_id)[1]
  end

  def self.recompute_clusters_targeting_id
    self.each { |e| e.cluster_targeting_id(true) }
  end

  def subjects
    snapshots.map { |email| email.subject }.uniq
  end

  field :footprint # Hash[Ad => occurence]
  field :normalized_footprint
  field :log_footprint

  field :matched
  field :match_group
  index({ match_group: 1 })

  def self.cluster_data(sig_id)
    { exp_email_index: Experiment.current.email_index_from_sig_id(sig_id) }
  end

  def footprint
    return super if super
    footprint!
  end

  def footprint!
    self.footprint = Hash.new(0)
    Ad.each do |ad|
      self.footprint[ad.id.to_s] = Snapshot.where( snapshot_cluster: ad, context_cluster: self ).count
    end
  ensure
    self.save
  end

  def self.recompute_footprints
    self.no_timeout.each(&:footprint!)
  end

  def normalized_footprint
    return super if super
    normalized_footprint!
  end

  def normalized_footprint!
    self.normalized_footprint = Hash.new(0)
    Ad.each do |ad|
      ne = Snapshot.where( snapshot_cluster: ad, context_cluster: self ).count
      ntot = ad.snapshots_count
      self.normalized_footprint[ad.id.to_s] = ne.to_f / ntot
    end
  ensure
    self.save
  end

  def self.recompute_normalized_footprints
    self.no_timeout.each(&:normalized_footprint!)
  end

  def log_footprint
    return super if super
    log_footprint!
  end

  def log_footprint!
    self.log_footprint = Hash.new(0)
    Ad.each do |ad|
      ne = Snapshot.where( snapshot_cluster: ad, context_cluster: self ).count
      self.log_footprint[ad.id.to_s] = Math.log(ne + 1)
    end
  ensure
    self.save
  end

  def self.recompute_log_footprints
    self.no_timeout.each(&:log_footprint!)
  end
end
