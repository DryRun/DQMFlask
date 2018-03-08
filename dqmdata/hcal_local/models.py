from dqmdata import db
from dqmdata.hcal_local.dqmio import load_dqm_object
from sqlalchemy.ext.declarative import declared_attr

# Utility models
class Channel(db.Model):
	__tablename__ = "channel"
	id            = db.Column(db.Integer, primary_key=True)
	subdet        = db.Column(db.String(8), nullable=False)
	ieta          = db.Column(db.SmallInteger, nullable=False)
	iphi          = db.Column(db.SmallInteger, nullable=False)
	depth         = db.Column(db.SmallInteger, nullable=False)
	crate         = db.Column(db.SmallInteger, nullable=False)
	slot          = db.Column(db.SmallInteger, nullable=False)
	dcc           = db.Column(db.SmallInteger, nullable=False)
	spigot        = db.Column(db.SmallInteger, nullable=False)
	fiber         = db.Column(db.SmallInteger, nullable=False)
	fiber_channel = db.Column(db.SmallInteger, nullable=False)
	emap_version  = db.Column(db.String(8), nullable=False)

	def __init__(self, subdet, ieta, iphi, depth, crate, slot, dcc, spigot, fiber, fiber_channel, emap_version):
		self.subdet = subdet
		self.ieta = ieta
		self.iphi = iphi
		self.depth = depth
		self.crate = crate
		self.slot = slot
		self.dcc = dcc
		self.spigot = spigot
		self.fiber = fiber
		self.fiber_channel = fiber_channel
		self.emap_version = emap_version

	def __repr__(self):
		return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

# Mixins
class RunQuantity(object):
	run = db.Column(db.Integer, nullable=False)
	__abstract__ = True

class ChannelQuantity(object):
	@declared_attr
	def channel_id(cls):
		return db.Column(db.Integer, db.ForeignKey("channel.id"), nullable=False)


	#@declared_attr
	#def channel(cls):
	#	return db.relationship("channel")

	__abstract__ = True

class SubdetQuantity(object):
	#SUBDET_CHOICES = (
	#	('HB', 'HB'),
	#	('HE', 'HE'),
	#	('HEP17', 'HEP17'),
	#	('HF', 'HF'),
	#	('HO', 'HO'),
	#	('NA', 'Unknown'),
	#)
	subdet = db.Column(db.Enum('HB', 'HE', 'HEP17', 'HF', 'HO', 'NA'), name='subdet')
	__abstract__ = True

class SubdetPMQuantity(object):
	#SUBDETPM_CHOICES = (
	#	('HBP', 'HBP'),
	#	('HBM', 'HBM'),
	#	('HEP', 'HEP'),
	#	('HEM', 'HEM'),
	#	('HEP17', 'HEP17'),
	#	('HFP', 'HFP'),
	#	('HFM', 'HFM'),
	#	('HOP', 'HOP'),
	#	('HOM', 'HOM'),
	#	('NA', 'Unknown'),
	#)
	subdetpm = db.Column(db.Enum('HBP', 'HBM', 'HEP', 'HEM', 'HEP17', 'HFP', 'HFM', 'HOP', 'HOM', 'NA'), name='subdetpm')
	__abstract__ = True

class SubdetIEtaQuantity(object):
	#SUBDET_CHOICES = (
	#	('HB', 'HB'),
	#	('HE', 'HE'),
	#	('HEP17', 'HEP17'),
	#	('HF', 'HF'),
	#	('HO', 'HO'),
	#	('NA', 'Unknown'),
	#)
	subdet = db.Column(db.Enum('HB', 'HE', 'HEP17', 'HF', 'HO', 'NA'), name='subdet')
	ieta = db.Column(db.SmallInteger, nullable=False)
	depth = db.Column(db.SmallInteger, nullable=False)
	__abstract__ = True


# Data models
class PedestalMean_Run_Channel(db.Model, RunQuantity, ChannelQuantity):
	__tablename__ = 'pedestal_mean_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	pedestal_mean = db.Column(db.Float)

	def __init__(self, pedestal_mean, run, channel_id):
		self.pedestal_mean = pedestal_mean
		self.run = run
		self.channel = channel_id
	
	def __repr__(self):
		return "id {}, run {}, channel {} => {}".format(self.id, self.run, self.channel, self.pedestal_mean)
		return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J"):
		# Get data
		dqm_data = load_dqm_object(run, "PEDESTAL/Commissioning2018/DQMIO", "Hcal/PedestalTask/Mean/depth")

		# Get histograms
		hist_pedestal_mean = {}
		for depth in range(1, 8):
			hist_pedestal_mean[depth] = dqm_data["depth{}".format(depth)]
		
		# Extract all pedestals from the DQM histograms here
		channels = Channel.query.filter(Channel.emap_version==emap_version)
		for channel in channels:
			xbin, ybin = hcaldqmfunctions.detid_to_histbins(channel.subdet, channel.ieta, channel.iphi, channel.depth)
			this_reading = PedestalMean_Run_Channel(hist_pedestal_mean[channel.depth].GetBinContent(xbin, ybin), run, channel.id)
			this_reading.save()


