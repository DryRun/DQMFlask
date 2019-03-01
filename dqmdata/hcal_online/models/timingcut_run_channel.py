import sys
from dqmdata import db
from dqmdata.hcal_online.dqmio import load_dqm_object
from dqmdata.common.models.serializable import Serializable
from dqmdata.common.models.channel import Channel
from dqmdata.common.models.online_run import OnlineRun, add_run
from dqmdata.common.utilities import *

class TimingCut_Run_Channel(Serializable, db.Model):
	__tablename__ = 'timingcut_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer, db.ForeignKey('online_run.run'))
	channel_id    = db.Column(db.Integer, db.ForeignKey('channel.id'))
	value         = db.Column(db.Float)

	def __repr__(self):
		return "id {}, channel {}, run {} => {}".format(self.id, self.channel_id, self.run, self.value)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2018", overwrite=False):
		print("[TimingCut_Run_Channel::extract] Extracting for run {}".format(run))

		# Check that this run is not already in DB
		if not check_overwrite(TimingCut_Run_Channel, run, emap_version, overwrite=overwrite):
			return

		# Make sure run is in the run database
		add_run(run, overwrite=False)

		# Get data
		dqm_data = load_dqm_object(run, "Hcal/DigiTask/TimingCut/depth")
		if len(dqm_data) == 0:
			print("[TimingCut_Run_Channel::extract] ERROR : DQM data is empty! Skipping.")
			return 
		#print(dqm_data)

		# Get histograms
		hist_pedestal_mean = {}
		for depth in range(1, 8):
			hist_pedestal_mean[depth] = dqm_data["depth{}".format(depth)]

		# Extract all pedestals from the DQM histograms here
		channels = Channel.query.filter(Channel.emap_version==emap_version)
		for channel in channels:
			xbin, ybin = detid_to_histbins(channel.subdet, channel.ieta, channel.iphi)
			this_pedestal_mean = hist_pedestal_mean[channel.depth].GetBinContent(xbin, ybin)
			if this_pedestal_mean == 0: # Zero suppress. This plot monitors drifts, not errors.
				continue
			this_reading = TimingCut_Run_Channel(run=run, value=this_pedestal_mean, channel_id=channel.id)
			#print(this_reading)
			db.session.add(this_reading)
		db.session.commit()
