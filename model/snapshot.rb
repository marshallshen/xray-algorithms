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
