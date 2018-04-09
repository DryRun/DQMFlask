import sys
from dqmdata import db
from dqmdata.hcal_local.dqmio import load_dqm_object
from sqlalchemy.ext.declarative import declared_attr

class Serializable(object):
	@property
	def as_dict(self):
		blacklist = ['_sa_instance_state']
		blacklist.extend(getattr(self, '_serialize_blacklist', []))
		result = {}
		for k,v in self.__dict__.iteritems():
			if k in blacklist:
				continue
			elif isinstance(v, list): #One to Many/Many to Many relationship, add a list of serialized child objects
				result[k] = [i.as_dict for i in v]
			elif isinstance(v, db.Model): #One to One relationship, serialize the child and include it
				result[k] = v.as_dict
			else:
				result[k] = v
		return result

# Utility models
class Channel(Serializable, db.Model):
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

	# Backrefs
	pedestal_mean_run_channel = db.relationship('PedestalMean_Run_Channel', backref='channel', lazy='dynamic')
	pedestal_rms_run_channel = db.relationship('PedestalRMS_Run_Channel', backref='channel', lazy='dynamic')

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

	# Short string for plot legends
	def get_label(self):
		return "{} | ieta={} | iphi={} | depth={}".format(self.subdet, self.ieta, self.iphi, self.depth)

	@property
	def as_dict(self):
		return {"subdet":self.subdet, "ieta":self.ieta, "iphi":self.iphi, "depth":self.depth}

	# Backref
	#backref_dict = {
	#		"PedestalMean_Run_Channel":pedestal_mean_run_channel, 
	#		"PedestalRMS_Run_Channel":pedestal_rms_run_channel
	#}
	#def get_backref(self, cls):
	#	return self.backref_dict[cls.__name__]



# Mixins: currently disfavored because David doesn't understand how to make mixin foreignkeys work
#class RunQuantity(object):
#	run = db.Column(db.Integer, nullable=False)
#	#__abstract__ = True

#class ChannelQuantity(object):
#	@declared_attr
#	def channel_id(cls):
#		return db.Column(db.Integer, db.ForeignKey("channel.id"))
#
#	@declared_attr
#	def channel(cls):
#		return db.relationship(Channel, backref=cls.__name__, lazy="joined")
#
#	#@declared_attr
#	#def channel(cls):
#	#	return db.relationship("channel")
#
#	#__abstract__ = True

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

def check_overwrite(quantity, run, emap_version, overwrite):
	q = quantity.query.filter(quantity.run==run)
	if q.count() > 0:
		if not overwrite:
			print "[check_overwrite] ERROR : Run {} already exists in DB for quantity {}! Specify overwrite to overwrite.".format(run, quantity)
			return False
		else:
			print "[check_overwrite] INFO : Deleting existing entries for run {}, quantity {}".format(run, quantity)
			for reading in q.all():
				print "[check_overwrite] INFO : Deleting ",
				print reading
				db.session.delete(reading)
			db.session.commit()
	return True


# Data models
class PedestalMean_Run_Channel(Serializable, db.Model):
	__tablename__ = 'pedestal_mean_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer)
	value         = db.Column(db.Float)
	channel_id    = db.Column(db.Integer, db.ForeignKey('channel.id'))

	def __repr__(self):
		return "id {}, channel {}, run {} => {}".format(self.id, self.channel, self.run, self.value)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print "[PedestalMean_Run_Channel::extract] Extracting for run {}".format(run)

		# Check that this run is not already in DB
		if not check_overwrite(PedestalMean_Run_Channel, run, emap_version, overwrite=overwrite):
			return

		# Get data
		if emap_version == "2017J":
			dataset = "PEDESTAL/Commissioning2016/DQMIO"
		else:
			dataset = "PEDESTAL/Commissioning2018/DQMIO"
		dqm_data = load_dqm_object(run, dataset, "Hcal/PedestalTask/Mean/depth")
		#print dqm_data

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
			this_reading = PedestalMean_Run_Channel(run=run, value=this_pedestal_mean, channel_id=channel.id)
			#print this_reading
			db.session.add(this_reading)
		db.session.commit()

class PedestalRMS_Run_Channel(Serializable, db.Model):
	__tablename__ = 'pedestal_rms_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer)
	value         = db.Column(db.Float)
	channel_id    = db.Column(db.Integer, db.ForeignKey('channel.id'))

	def __repr__(self):
		return "id {}, channel {}, run {} => {}".format(self.id, self.channel, self.run, self.value)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print "[PedestalRMS_Run_Channel::extract] Extracting for run {}".format(run)

		# Check that this run is not already in DB
		if not check_overwrite(PedestalRMS_Run_Channel, run, emap_version, overwrite=overwrite):
			print "[PedestalRMS_Run_Channel::extract] INFO : Failed overwrite check for run {}".format(run)
			return

		# Get data
		if emap_version == "2017J":
			dataset = "PEDESTAL/Commissioning2016/DQMIO"
		else:
			dataset = "PEDESTAL/Commissioning2018/DQMIO"
		dqm_data = load_dqm_object(run, dataset, "Hcal/PedestalTask/RMS/depth")
		#print dqm_data

		# Get histograms
		hist_pedestal_rms = {}
		for depth in range(1, 8):
			hist_pedestal_rms[depth] = dqm_data["depth{}".format(depth)]
		
		# Extract all pedestals from the DQM histograms here
		channels = Channel.query.filter(Channel.emap_version==emap_version)
		for channel in channels:
			if not channel.subdet in ["HB", "HE", "HF", "HO", "HEP17"]:
				continue
			xbin, ybin = detid_to_histbins(channel.subdet, channel.ieta, channel.iphi)
			this_pedestal_rms = hist_pedestal_rms[channel.depth].GetBinContent(xbin, ybin)
			if this_pedestal_rms == 0: # Zero suppress. This plot monitors drifts, not errors.
				continue
			this_reading = PedestalRMS_Run_Channel(run=run, value=this_pedestal_rms, channel_id=channel.id)
			#print this_reading
			db.session.add(this_reading)
		db.session.commit()

