import sys
from dqmdata import db
from dqmdata.hcal_local.dqmio import load_dqm_object
from dqmdata.common.models.serializable import Serializable
from dqmdata.common.models.local_run import LocalRun, add_run
from dqmdata.hcal_local.models.channel import Channel
from dqmdata.common.utilities import *
from sqlalchemy.dialects import postgresql

class PedestalRMS_Run_Channel(Serializable, db.Model):
	__tablename__ = 'pedestal_rms_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer, db.ForeignKey('local_run.run'), index=True)
	channel_id    = db.Column(db.BigInteger, db.ForeignKey('channel.id'), index=True)
	value         = db.Column(db.Float, nullable=False)

	def __repr__(self):
		return "id {}, run {}".format(self.id, self.channel_id, self.run)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print("[PedestalRMS_Run_Channel::extract] Extracting for run {}".format(run))

		# Check that this run is not already in DB
		if not check_overwrite(PedestalRMS_Run_Channel, run, emap_version, overwrite=overwrite):
			return

		# Make sure run is in the run database
		add_run(run, overwrite=False)

		# Get data
		if emap_version == "2017J":
			dataset = "PEDESTAL/Commissioning2016/DQMIO"
		else:
			dataset = "PEDESTAL/Commissioning2018/DQMIO"
		dqm_data = load_dqm_object(run, dataset, "Hcal/PedestalTask/RMS/depth")
		#print(dqm_data)

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
			this_value = hist_pedestal_rms[channel.depth].GetBinContent(xbin, ybin)
			if this_value == 0: # Zero suppress
				continue

			this_reading = PedestalRMS_Run_Channel(run=run, value=this_value, channel_id=channel.id)
			db.session.add(this_reading)
		db.session.commit()
