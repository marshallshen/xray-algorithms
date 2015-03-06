class Account
  include Mongoid::Document
  include Mongoid::Timestamps

  field :used
  field :is_master
  field :login

  has_many :snapshots
  has_many :account_snapshot_clusters, dependent: :destroy

  def self.master_account
    exp = Experiment.where(:name => Mongoid.tenant_name).first
    if exp == nil
      Account.where(:is_master => true).first
    else
      Account.where( id: exp.master_account ).first
    end
  end
end
