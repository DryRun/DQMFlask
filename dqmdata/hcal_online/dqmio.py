import os
import pickle
from dqmdata.common.localization import *
from dqmdata.common.dqmio_common import download_dqm_json_object, convert_json_to_root, get_dqm_json_cachepath

# Example URL: 
# https://cmsweb.cern.ch/dqm/online/data/json/archive/318733//Global/Online/ALL/Hcal/DigiTask/LETDCTime/depth/depth1?rootcontent=1

# @param run      : run number
# @param dataset  : e.g. /Global/Online/ALL (see the DQM GUI and click on Run)
# @param rootpath : path to histogram inside the ROOT file, e.g. Hcal/DigiTask/LETDCTime/depth/
# @return         : Dict of {name:TObject} of objects inside the specified folder
def load_dqm_object(run, rootpath, dataset="Global/Online/ALL", force_download=False):
	# Check if object exists in cache. If not, download.
	object_name = dataset.replace("/", "_") + "/" + rootpath.replace("/", "_")
	cachepath = get_dqm_json_cachepath("hcal_online", run, object_name)
	if not os.path.isfile(cachepath) or force_download:
		url = "https://cmsweb.cern.ch/dqm/online/data/json/archive/{}/{}/{}".format(run, dataset, rootpath)
		download_dqm_json_object(url, "hcal_online", run, object_name)
	else:
		print "[load_dqm_object] INFO : Loading cached DQM objects from {}".format(cachepath)
	with open(cachepath, 'r') as f:
		json_data = pickle.load(f)
	root_data = convert_json_to_root(json_data)

	return root_data

if __name__ == "__main__":
	# Test loading DQM object
	print load_dqm_object(318733, "Hcal/DigiTask/LETDCTime/depth")
	print "Done."  
