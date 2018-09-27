import sys
from dqmdata import db
from dqmdata.hcal_local.dqmio import load_dqm_object
from dqmdata.hcal_local.models.common import *

# SiPM gain
# Method A: 3 gaussian fit, no kernel smoothing
class SiPMGainA_Run_Channel(Serializable, db.Model):
	__tablename__ = 'sipmgaina_run_channel'
	id            = db.Column(db.Integer, primary_key=True)
	run           = db.Column(db.Integer, db.ForeignKey('local_run.run'))
	value         = db.Column(db.Float)
	value_error   = db.Column(db.Float)
	channel_id    = db.Column(db.Integer, db.ForeignKey('channel.id'))

	def __repr__(self):
		return "id {}, channel {}, run {} => {}".format(self.id, self.channel, self.run, self.value)
		#return "Detector: ({}, {}, {}, {}) | Electronics: ({}, {}, {}, {}) | emap {}".format(self.subdet, self.ieta, self.iphi, self.depth, self.crate, self.slot, self.fiber, self.fiber_channel, self.emap_version)

	# Extract data from DQM histogram
	def extract(self, run, emap_version="2017J", overwrite=False):
		print "[SiPMGainA_Run_Channel::extract] Extracting for run {}".format(run)

		# Check that this run is not already in DB
		if not check_overwrite(SiPMGainA_Run_Channel, run, emap_version, overwrite=overwrite):
			print "[SiPMGainA_Run_Channel::extract] INFO : Failed overwrite check for run {}".format(run)
			return

		# Make sure run is in the run database
		add_run(run, overwrite=False)

		# Load data
		gain_tree = load_sipm_gains(run)
		for entry in xrange(gain_tree.GetEntries()):
			gain_tree.GetEntry(entry)
			channel = get_channel(gain_tree.subdet, gain_tree.ieta, gain_tree.iphi, gain_tree.depth, emap_version)
			this_reading = SiPMGainA_Run_Channel(run=run, value=gain.gain, value_error=gain.dgain, channel_id=channel.id)
			db.session.add(this_reading)
		db.session.commit()

