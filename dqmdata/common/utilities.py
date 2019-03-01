import sys
from dqmdata import db

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
			print("[check_overwrite] ERROR : Run {} already exists in DB for quantity {}! Specify overwrite to overwrite.".format(run, quantity))
			return False
		else:
			print("[check_overwrite] INFO : Deleting existing entries for run {}, quantity {}".format(run, quantity))
			for reading in q.all():
				print("[check_overwrite] INFO : Deleting ",)
				print(reading)
				db.session.delete(reading)
			db.session.commit()
	return True

# Function parse_integer_range
# - Turn a string like 300101,300200:300210 into a list of integers
def parse_integer_range(intstr):
	intlist = []
	for this_str in intstr.split(","):
		if ":" in this_str:
			intmin = int(this_str.split(":")[0])
			intmax = int(this_str.split(":")[1])
			intlist.extend(range(intmin, intmax+1))
		else:
			intlist.append(int(this_str))
	return intlist


#class IEtaAverage():
#	def __init__(self, q_channels):
#		# Build list of output labels (subdet : ieta : depth)
#		self._output_channels = []
#		for channel in q_channels.all():
#			self._output_channels.append(hash_channel(channel))
#
#		# Averaging container: pandas dataframe, columns = channel hashes, rows = runs
#		avg_sum = pandas.Dataframe(columns=self._output_channels)
#		avg_n = pandas.Dataframe(columns=self._output_channels)
#
#
#	# Add a single channel data
#	def add_channel_data(self, channel, data):
#		#Pseudocode
#		#if not run in avg_sum.rows:
#		#	avg_sum.rows.append(run)
#
#
#	# Get average results as json
#	def get_avg_json(self):
#
#	# Utility function: "hash" for channels
#	def hash_channel(self, channel):
#		return Channel.query(subdet=channel.subdet, ieta=channel.ieta, iphi=1, depth=channel.depth, emap_version=channel.emap_version)[0].id
#
