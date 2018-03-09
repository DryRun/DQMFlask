import sys
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
	#__abstract__ = True

class ChannelQuantity(object):
	@declared_attr
	def channel_id(cls):
		return db.Column(db.Integer, db.ForeignKey("channel.id"))

	@declared_attr
	def channel(cls):
		return db.relationship(Channel, backref=cls.__name__)

	#@declared_attr
	#def channel(cls):
	#	return db.relationship("channel")

	#__abstract__ = True

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
	#__abstract__ = True

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
	#__abstract__ = True

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
	#__abstract__ = True

def detid_to_histbins(subdet, ieta, iphi):
	if ieta < 0:
		if subdet == "HF":
			xbin = ieta + 42
		else:
			xbin = ieta + 43
	else:
		if subdet == "HF":
			xbin = ieta + 43
		else:
			xbin = ieta + 42
	ybin = iphi
	return xbin, ybin


# Data models
class PedestalMean_Run_Channel(RunQuantity, ChannelQuantity, db.Model):
	__tablename__ = 'pedestal_mean_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	pedestal_mean = db.Column(db.Float)

	def __repr__(self):
		return "id {}, channel {}, run {} => {}".format(self.id, self.channel, self.run, self.pedestal_mean)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print "[PedestalMean_Run_Channel::extract] Extracting for run {}".format(run)

		# Check that this run is not already in DB
		if PedestalMean_Run_Channel.query.filter_by(run=run).count() > 0:
			if not overwrite:
				print "[PedestalMean_Run_Channel::extract] ERROR : Run {} already exists in DB! Specify overwrite to overwrite.".format(run)
				sys.exit(1)
			else:
				for reading in PedestalMean_Run_Channel.query.filter_by(run=run):
					db.session.delete(reading)
				db.session.commit()

		# Get data
		if emap_version == "2017J":
			dataset = "PEDESTAL/Commissioning2016/DQMIO"
		else:
			dataset = "PEDESTAL/Commissioning2018/DQMIO"
		dqm_data = load_dqm_object(run, dataset, "Hcal/PedestalTask/Mean/depth")
		print dqm_data

		# Get histograms
		hist_pedestal_mean = {}
		for depth in range(1, 8):
			hist_pedestal_mean[depth] = dqm_data["depth{}".format(depth)]
		
		# Extract all pedestals from the DQM histograms here
		channels = Channel.query.filter(Channel.emap_version==emap_version)
		for channel in channels:
			if not channel.subdet in ["HB", "HE", "HF", "HO", "HEP17"]:
				continue
			xbin, ybin = detid_to_histbins(channel.subdet, channel.ieta, channel.iphi)
			this_pedestal_mean = hist_pedestal_mean[channel.depth].GetBinContent(xbin, ybin)
			if this_pedestal_mean == 0: # Zero suppress. This plot monitors drifts, not errors.
				continue
			this_reading = PedestalMean_Run_Channel(run=run, pedestal_mean=this_pedestal_mean, channel_id=channel.id)
			print this_reading
			db.session.add(this_reading)
		db.session.commit()



