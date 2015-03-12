class Experiment
  class AccountNumberError < RuntimeError; end
  class AccountCombinationError < RuntimeError; end

  include Mongoid::Document
  store_in database: ->{ self.database }

  field :name
  index({ name: 1 }, { :unique => true })
  field :type
  field :account_number  # includes +1 for the master
  field :i_perc_a  # puts each input in :i_perc_a * :account_number accounts
  field :inputs  # an array of inputs
  field :fill_up_inputs, :default => []  # an array of emails sent to all accounts
  field :master_account  # string, id of the master account
  field :master_inputs
  field :i_a_assignments
  field :measurements, :default => []
  field :has_master, :default => true
  field :analyzed, :default => false

  def self.current
    Experiment.where( :name => Mongoid.tenant_name ).first
  end

#  class_attribute :exps_accs_emails_map
#  def self.curr_accs_emails_map
#    self.exps_accs_emails_map ||= {}
#    name = self.current.name
#    unless self.exps_accs_emails_map[name]
#      self.exps_accs_emails_map[name] = {}
#      Account.each do |a|
#        emails_in = AccountEmail.where(account: a).uniq.map(&:email)
#                                .select! {|em| em.cluster_targeting_id != "garbage"}
#        self.exps_accs_emails_map[name] ||= []
#        self.exps_accs_emails_map[name] [a.id] = emails_in
#      end
#    end
#    return self.exps_accs_emails_map[name]
#  end

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

    yt_video_ad_snaps = Mongoid.with_tenant(name) do
      YtVideoAdSnapshot.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      yt_video_ad_snaps.each { |a| YtVideoAdSnapshot.new(a).tap { |na| na.id = a['_id'] }.save }
    end

    yt_side_ad_snaps = Mongoid.with_tenant(name) do
      YtSideAdSnapshot.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      yt_side_ad_snaps.each { |a| YtSideAdSnapshot.new(a).tap { |na| na.id = a['_id'] }.save }
    end

    context_site_ad_snaps = Mongoid.with_tenant(name) do
      ContextSiteAdSnapshot.all.map { |a| Hash[a.attributes] }
    end
    Mongoid.with_tenant(new_name) do
      context_site_ad_snaps.each { |a| ContextSiteAdSnapshot.new(a).tap { |na| na.id = a['_id'] }.save }
    end

    exp_new.save
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

end
