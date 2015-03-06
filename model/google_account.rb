class GoogleAccount < Account
  field :passwd
  field :gmail
  field :phone
  field :gender
  field :first_name
  field :last_name
  field :email
  field :bd
  field :bm
  field :by
  index({ login: 1 }, { :unique => true })

end
