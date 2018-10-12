import os
import sys
from datetime import datetime
from dqmdata import db
from dqmdata.common.localization import cache_directory
import requests
from bs4 import BeautifulSoup
import subprocess

def add_run(run, overwrite=False, test=False ):
	# Check if run exists
	q = LocalRun.query.filter(LocalRun.run==run)
	if q.count() > 0:
		if not overwrite:
			print "[LocalRun::add_run] WARNING : Run {} already exists in db. Use overwrite=True to overwrite."
			return
		else:
			print "[LocalRun::add_run] INFO : Deleting run {} from db.".format(run)
			for entry in q.all():
				db.session.delete(entry)
			db.session.commit()

	# Create new run object
	this_run = LocalRun()
	this_run.run = run

	# Run info DB parameters
	runinfo = this_run.query_runinfo(run, keys=["DQM_TASK", "LOCAL_RUNKEY_SELECTED", "HCAL_TIME_OF_FM_START", "MASTERSNIPPET_SELECTED", "EventsRequested"])
	# Time example: 2018-06-21 15:40:22 CEST
	start_time = datetime.strptime(runinfo["HCAL_TIME_OF_FM_START"], '%Y-%m-%d %H:%M:%S %Z')
	this_run.dqm_task = runinfo["DQM_TASK"]
	this_run.local_runkey_selected = runinfo["LOCAL_RUNKEY_SELECTED"]
	this_run.mastersnippet_selected = runinfo["MASTERSNIPPET_SELECTED"]
	this_run.events = int(runinfo["EventsRequested"])

	# Luminosity
	this_run.ytd_lumi = this_run.get_ytd_lumi(run, overwrite_lumi_cache=False)

	if not test:
		db.session.add(this_run)
		db.session.commit()
	else:
		print this_run


class LocalRun(db.Model):
	__tablename__ = "local_run"
	run                    = db.Column(db.Integer, primary_key=True)
	start_time             = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
	ytd_lumi               = db.Column(db.Float)
	dqm_task               = db.Column(db.String, nullable=True)
	local_runkey_selected  = db.Column(db.String, nullable=True)
	mastersnippet_selected = db.Column(db.String, nullable=True)
	events                 = db.Column(db.Integer, nullable=False)

	_cache = cache_directory + "/runs/"

	def __repr__(self):
		return str(self.run)

	def query_runinfo(self, run, keys=["DQM_TASK", "LOCAL_RUNKEY_SELECTED", "HCAL_TIME_OF_FM_START", "MASTERSNIPPET_SELECTED", "EventsRequested"], overwrite_runinfo_cache=True):
		# Alternative idea: sql query. This doesn't work on the VM, because the DB can't be found... might work with a tunnel, but that's annoying.
		# SQL query for runinfo DB.
		# sqlplus -S cms_hcl_runinfo/run2009info@cms_rcms @/WBM/sql_templates/query.sql STRING_VALUE CMS.HCAL%:LOCAL_RUN_KEY runnumber
		
		runinfo_url = "https://cmswbm.cern.ch/cmsdb/servlet/RunParameters?RUN={}&FORMAT=XML".format(run)
		# requests doesn't seem to pick up the SSL cookie...
		# html = requests.get(runinfo_url).text
		runinfo_filename = "{}/runinfo/{}.dat".format(cache_directory, run)
		if not os.path.exists(runinfo_filename) or overwrite_runinfo_cache:
			runinfo_command = "wget '{}' -O {} --no-check-certificate".format(runinfo_url, runinfo_filename)
			print runinfo_command
			os.system(runinfo_command)
		html = ""
		print "[debug] Runinfo saved to {}".format(runinfo_filename)
		with open(runinfo_filename) as runinfo_file:
			for line in runinfo_file:
				html += line

		# Parsing example from https://stackoverflow.com/questions/23377533/python-beautifulsoup-parsing-table
		table_data = {}
		#print html
		soup = BeautifulSoup(html)
		#print soup
		table = soup.find('table', attrs={'class':'example'})
		print table
		table_body = table.find('tbody')

		rows = table_body.find_all('tr')
		for row in rows:
			cols = row.find_all('td')
			for key in keys:
				if key in col[0]:
					table_data[key] = col[1]
		return table_data

	def get_ytd_lumi(self, run, overwrite_lumi_cache=False):
		brilcalc_filename = "{}/brilcalc/{}.dat".format(cache_directory, run)
		if not os.path.exists(brilcalc_file) or overwrite_lumi_cache:
			brilcalc_command = r"brilcalc lumi -u /fb --begin 01/01/{}\ 00:00:00 --end {} -o {}".format(
				start_time.strftime(r"%y"), 
				start_time.strftime(r"%m/%d/%y\ %h:%m:%s"),
				brilcalc_filename
			)
			print brilcalc_command
			os.system(brilcalc_command)
		summary_line = subprocess.check_output(['tail', '-1', brilcalc_filename])
		# Summary line format:
		# #nfill,nrun,nls,ncms,totdelivered(/fb),totrecorded(/fb)

		return float(summary_line.split(",")[4])
