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
