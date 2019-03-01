import sys
from dqmdata import db
from dqmdata.hcal_local.dqmio import load_dqm_object
from dqmdata.common.models.serializable import Serializable
from dqmdata.common.models.channel import Channel
from dqmdata.common.models.local_run import LocalRun, add_run
from dqmdata.common.utilities import *
from sqlalchemy.dialects import postgresql

class PedestalMean_Run(Serializable, db.Model):
	__tablename__ = 'pedestal_mean_run'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer, db.ForeignKey('local_run.run'))
	emap          = db.Column(db.Integer, db.ForeignKey('emap.version'))
	#channel_id    = db.Column(db.Integer, db.ForeignKey('channel.id'))
	values         = db.Column(postgresql.ARRAY(db.Float))

	def __repr__(self):
		return "id {}, run {}".format(self.id, self.channel_id, self.run)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print("[PedestalMean_Run::extract] Extracting for run {}".format(run))

		# Check that this run is not already in DB
		if not check_overwrite(PedestalMean_Run, run, emap_version, overwrite=overwrite):
			return

		# Make sure run is in the run database
		add_run(run, overwrite=False)

		# Get data
		if emap_version == "2017J":
			dataset = "PEDESTAL/Commissioning2016/DQMIO"
		else:
			dataset = "PEDESTAL/Commissioning2018/DQMIO"
		dqm_data = load_dqm_object(run, dataset, "Hcal/PedestalTask/Mean/depth")
		#print(dqm_data)

		# Get histograms
		hist_pedestal_mean = {}
		for depth in range(1, 8):
			hist_pedestal_mean[depth] = dqm_data["depth{}".format(depth)]
		
		# Extract all pedestals from the DQM histograms here
		channels = Channel.query.filter(Channel.emap_version==emap_version)
		array_size = db.func.max(channels.index) - db.func.min(channels.index) # Just to be safe
		values_array = array.array('f', [0.] * array_size)
		for channel in channels:
			if not channel.subdet in ["HB", "HE", "HF", "HO", "HEP17"]:
				continue
			xbin, ybin = detid_to_histbins(channel.subdet, channel.ieta, channel.iphi)
			values_array[channel.index] = hist_pedestal_mean[channel.depth].GetBinContent(xbin, ybin)
			#if this_pedestal_mean == 0: # Zero suppress. This plot monitors drifts, not errors.
			#	continue

		this_reading = PedestalMean_Run(run=run, values=values_array, emap=emap_version)
		db.session.add(this_reading)
		db.session.commit()
