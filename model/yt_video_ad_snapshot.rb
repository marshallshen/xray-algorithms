class YtVideoAdSnapshot < Snapshot
  field :title
  field :long_url
  field :short_url
  field :by
  field :description

  index({ short_url: 1 })
  has_context :yt_search
end
