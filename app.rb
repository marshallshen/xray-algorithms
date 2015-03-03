require 'mongoid'
require 'faraday'
require 'kaminari'
require 'workflow'

class Snapshot; end
class EmailSnapshot < Snapshot; end
class AdSnapshot < Snapshot; end
class SnapshotCluster; end
class Experiment; end
class Email < SnapshotCluster; end
class Ad < SnapshotCluster; end
class Account; end
class GoogleAccount < Account; end
class GmailAPI; end
class GmailScraper; end

require_relative 'measures/gmail_ads'
require_relative 'measures/youtube'
