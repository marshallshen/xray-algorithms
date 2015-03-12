class ContextSiteAdSnapshot < Snapshot
  field :url
  field :name
  field :click
  field :account_id
  field :campaign_id
  field :full_id

  index({ campaign_id: 1 })
  index({ url: 1 })
end
