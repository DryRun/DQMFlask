import sys
from dqmdata import db

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
	sipmgaina_run_channel = db.relationship('SiPMGainA_Run_Channel', backref='channel', lazy='dynamic') 

	def __init__(self, subdet, ieta, iphi, depth, crate, slot, dcc, spigot, fiber, fiber_channel, emap_version):
		self.subdet        = subdet
		self.ieta          = ieta
		self.iphi          = iphi
		self.depth         = depth
		self.crate         = crate
		self.slot          = slot
		self.dcc           = dcc
		self.spigot        = spigot
		self.fiber         = fiber
		self.fiber_channel = fiber_channel
		self.emap_version  = emap_version
		self.id            = hash((subdet, str(ieta), iphi, depth, emap_version)) # NOTE: python has the unfortunate feature hash(-1) == hash(-2). So here, we convert ieta to a string first.

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

# Try to implement a fast channel lookup... not sure if this works
def get_channel(subdet, ieta, iphi, depth, emap):
	q = Channel.query.filter(id=hash(subdet, ieta, iphi, depth, emap))
	if q.count() != 1:
		print "[get_channel] ERROR : get_channel({}, {}, {}, {}, {}) returned {} entries".format(subdet, ieta, iphi, depth, emap)
		sys.exit(1) # Should prob raise an exception
	return q[0]
