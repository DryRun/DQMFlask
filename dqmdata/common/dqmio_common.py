# Functions for processing DQM files
import pickle
import re
import urllib2
from x509auth import *
from ROOT import TBufferFile, TH1F, TProfile, TProfile2D, TH1F, TH2F

cache_directory = "/afs/cern.ch/user/c/cmshcaldqm/DQMFlaskDeploy/cache"

def convert_json_to_root(json_data):
	return_dict = {}
	for idx, item in enumerate(json_data['contents']):
		if "obj" in item.keys() and "rootobj" in item.keys():
			bit_array = array('B')
			bit_array.fromstring(item['rootobj'].decode('hex'))
			tbuffer = TBufferFile(TBufferFile.kRead, len(bit_array), bit_array, False)
			rootType = item['properties']['type']
			if rootType == 'TPROF': 
				rootType = 'TProfile'
			elif rootType == 'TPROF2D': 
				rootType = 'TProfile2D'
			return_dict[item["obj"]] = tbuffer.ReadObject(eval(rootType+'.Class()'))
	return return_dict

# Get the cache location of a DQM object downloaded from the JSON API
def get_dqm_json_cachepath(project, run, object_name):
	return "{}/{}/json_objects/{}/{}.dat".format(cache_directory, project, run, object_name)

# Download a DQM object as JSON
# - URL must be fully provided. The URL structure between subdetectors and DQM flavors is not intuitively compatible, so each subdetector and DQM flavor should implement the URL building.
def download_dqm_json_object(url, project, run, object_name):
	cached_path = get_dqm_json_cachepath(project, run, object_name)
	X509CertAuth.ssl_key_file, X509CertAuth.ssl_cert_file = x509_params()
	if not "?rootcontent=1" in url:
		url += "?rootcontent=1"
	print "[download_dqm_json_object] INFO : Downloading ROOT JSON from {} to {}".format(url, cached_path)
	datareq = urllib2.Request(url)
	datareq.add_header('User-agent', ident)
	# Get data
	data = eval(re.sub(r"\bnan\b", "0", urllib2.build_opener(X509CertOpen()).open(datareq).read()),
			   { "__builtins__": None }, {})
	# Save data to a pickle
	os.system("mkdir -pv {}".format(os.path.dirname(cached_path)))
	with open(cached_path, 'w') as f:
		pickle.dump(data, f)
	return cached_path	


# Get the cache location of a DQM ROOT file
def get_dqm_root_cachepath(run, server, dataset, folder):
	return "{}/{}/{}/{}/{}.dat".format(cache_directory, server, dataset, folder, run)

# Download a DQM file as ROOT
# Example URLs
# https://cmsweb.cern.ch/dqm/hcal-online/data/browse/ROOT/OfflineData/Commissioning2018/PEDESTAL/0003110xx/DQM_V0001_R000311043__PEDESTAL__Commissioning2018__DQMIO.root
# - server = https://cmsweb.cern.ch/dqm/hcal-online
# - dataset = OfflineData/Comissioning2018
# - task = PEDESTAL
# 
# https://cmsweb.cern.ch/dqm/online/data/browse/Original/00031xxxx/0003110xx/DQM_V0001_Hcal2_R000311087.root
# - server = 
def download_dqm_root(run, server, dataset, task):
	X509CertAuth.ssl_key_file, X509CertAuth.ssl_cert_file = x509_params()
	datareq = urllib2.Request('{}/data/browse/ROOT/{}/{}/000{}xx/DQM_V0001_R000{}__{}__{}__DQMIO.root'.format(server, dataset, task, int(math.floor(run/100)), run, task, dataset))
	datareq.add_header('User-agent', ident)
	data = urllib2.build_opener(X509CertOpen()).open(datareq)
	cached_path = get_dqm_root_cachepath(run, server, dataset, rootpath)
	with open(cached_path, 'w') as f:
		f.write(data)

def get_root_objects(run, server, dataset, folder, force_download=False):
	cached_path = get_dqm_file_cachepath(run, server, dataset, folder)
	if not os.path.file_exists(cached_path) or force_download:
		download_dqm_file(run, server, dataset, folder)
	dqm_data = pickle.load(open(cached_path, 'r'))
	for idx,item in enumerate(data['contents']):
		if 'obj' in item.keys() and 'rootobj' in item.keys():
			a = array('B')
			a.fromstring(item['rootobj'].decode('hex'))
			t = TBufferFile(TBufferFile.kRead, len(a), a, False)
			rootType = item['properties']['type']
			if rootType == 'TPROF': rootType = 'TProfile'
			if rootType == 'TPROF2D': rootType = 'TProfile'
			data['contents'][idx]['rootobj'] = t.ReadObject(eval(rootType+'.Class()'))
	return dict( [ (x['obj'], x) for x in data['contents'][1:] if 'obj' in x] )
