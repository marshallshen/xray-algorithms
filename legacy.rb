###################################################################
##### This file includes all codes inherited from XRay code  ######
#####   Please pull out ANYTHING needed to a better location ######
#####   2015/03/03 Marshall Shen                             ######
###################################################################

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

  set_params :bool_behavior, { :p => 0.019287906244881078, :q => 0.0002624623547997546, :r => 0.009900990099009885 }

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
