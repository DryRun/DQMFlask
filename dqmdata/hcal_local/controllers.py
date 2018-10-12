# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db, app

# Import models
#from dqmdata.hcal_local.models.common import Channel # Moved to dqmdata.common
from dqmdata.hcal_local.models.pedestalmean_run_channel import PedestalMean_Run_Channel
from dqmdata.hcal_local.models.pedestalrms_run_channel import PedestalRMS_Run_Channel
from dqmdata.hcal_local.models.sipmgaina_run_channel import SiPMGainA_Run_Channel

# Support jsonp
import json
from functools import wraps
from flask import redirect, request, current_app

def jsonpify(f):
    """Wraps JSONified output for JSONP"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            content = str(callback) + '(' + str(f().data) + ')'
            return current_app.response_class(content, mimetype='application/json')
        else:
            return f(*args, **kwargs)
    return decorated_function

hcal_local = Blueprint('hcal_local', __name__, url_prefix='/hcal_local')

import click
import math
from dqmdata import app

@hcal_local.route('/emap/', methods=['GET'])
def emap():
	pass

# Get individual channel data
def parse_integer_range(intstr):
	intlist = []
	for this_str in intstr.split(","):
		if "-" in this_str:
			intmin = int(this_str.split("-")[0])
			intmax = int(this_str.split("-")[1])
			intlist.extend(range(intmin, intmax+1))
		else:
			intlist.append(int(this_str))
	return intlist

@hcal_local.route('/get_channels/<quantity_name>', methods=['GET'])
@jsonpify
def get_channels(quantity_name, max_entries=100, max_channels=100):
	valid_quantities = ["PedestalMean_Run_Channel", "PedestalRMS_Run_Channel"]
	backrefs = {
		"PedestalMean_Run_Channel":"pedestal_mean_run_channel",
		"PedestalRMS_Run_Channel":"pedestal_rms_run_channel"
	}
	if not quantity_name in valid_quantities:
		return render_template("400.html")
	quantity = eval(quantity_name)

	# Get channels
	year2emap = {"2017":"2017J", "2018":"2018"}
	year = request.args.get("year", default="2018", type=str)
	emap_version = year2emap[year]
	q_channels = Channel.query.filter(Channel.emap_version==emap_version)

	if "ieta" in request.args:
		ieta_list = parse_integer_range(request.args.get("ieta"))
		q_channels = q_channels.filter(Channel.ieta.in_(ieta_list))
	if "iphi" in request.args:
		iphi_list = parse_integer_range(request.args.get("iphi"))
		q_channels = q_channels.filter(Channel.iphi.in_(iphi_list))
	if "depth" in request.args:
		depth_list = parse_integer_range(request.args.get("depth"))
		q_channels = q_channels.filter(Channel.depth.in_(depth_list))
	if "subdet" in request.args:
		q_channels = q_channels.filter(Channel.subdet.in_(request.args.get("subdet").split(",")))
	q_channels = q_channels.limit(max_channels)
	print "[get_channels] Query returned {} channels".format(q_channels.count())

	return_data = []
	for channel in q_channels.all():
		# Return data key = legend entry for channel
		q_data = getattr(channel, backrefs[quantity_name])
		if "min_run" in request.args:
			q_data = q_data.filter(quantity.run >= int(request.args.get("min_run")))
		if "max_run" in request.args:
			q_data = q_data.filter(quantity.run <= int(request.args.get("max_run")))
		q_data = q_data.order_by(quantity.run)
		q_data = q_data.limit(max_entries)
		return_data.append({"name":channel.get_label(), "data":[[reading.run, reading.value] for reading in q_data.all()]})

	return json.dumps(return_data)


@app.cli.command(with_appcontext=True)
@click.option('--quantity')
@click.option('--run')
@click.option('--emap')
@click.option('--overwrite', is_flag=True)
def extract(quantity, run, emap, overwrite):
	quantity_object = eval(quantity)()
	quantity_object.extract(run, emap, overwrite=overwrite)

@app.cli.command(with_appcontext=True)
@click.option('--quantity')
@click.option('--run')
def delete(quantity, run):
	counter = 0
	for reading in eval(quantity).query.filter_by(run=run):
		db.session.delete(reading)
		if counter % 200 == 0:
			db.session.flush()
		counter += 1
	db.session.commit()
