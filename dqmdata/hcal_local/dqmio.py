import os
import pickle
from dqmdata.common.dqmio_common import download_dqm_json_object, convert_json_to_root, get_dqm_json_cachepath

# https://cmsweb.cern.ch/dqm/hcal-online/data/json/archive/311043/PEDESTAL/Commissioning2018/DQMIO/Hcal/PedestalTask/Mean/depth

# @param run      : run number
# @param dataset  : e.g. /PEDESTAL/Commissioning2018/DQMIO (see the DQM GUI and click on Run)
# @param rootpath : path to histogram inside the ROOT file, e.g. Hcal/PedestalTask/Mean/depth
# @return         : Dict of {name:TObject} of objects inside the specified folder
def load_dqm_object(run, dataset, rootpath, force_download=False):
	# Check if object exists in cache. If not, download.
	object_name = dataset.replace("/", "_") + "/" + rootpath.replace("/", "_")
	cachepath = get_dqm_json_cachepath("hcal_local", run, object_name)
	print "[debug] cachepath={}".format(cachepath)
	if not os.path.isfile(cachepath) or force_download:
		url = "https://cmsweb.cern.ch/dqm/hcal-online/data/json/archive/{}/{}/{}".format(run, dataset, rootpath)
		download_dqm_json_object(url, "hcal_local", run, object_name)

	with open(cachepath, 'r') as f:
		json_data = pickle.load(f)
	root_data = convert_json_to_root(json_data)

	return root_data

if __name__ == "__main__":
	# Test loading DQM object
	print load_dqm_object(311043, "PEDESTAL/Commissioning2018/DQMIO", "Hcal/PedestalTask/Mean/depth")
	print "Done."  