# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db, app

# Import models
#from dqmdata.hcal_local.models.common import Channel # Moved to dqmdata.common
from dqmdata.hcal_local.models.pedestalmean_run_channel import PedestalMean_Run_Channel
from dqmdata.hcal_local.models.pedestalrms_run_channel import PedestalRMS_Run_Channel
from dqmdata.hcal_local.models.sipmgaina_run_channel import SiPMGainA_Run_Channel
from dqmdata.hcal_local.models.channel import Channel

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

# Get individual channel data
from dqmdata.common.utilities import parse_integer_range
from dqmdata.common.view_args import ViewArgs
from sqlalchemy import desc


# Master function for getting data as JSON from the database
def get_data(quantity_name, view_args):
	quantity = eval(quantity_name)
	# Backref dict: map quantity name (i.e. class name) to table name.
	backrefs = {
		"PedestalMean_Run_Channel":"pedestal_mean_run_channel",
		"PedestalRMS_Run_Channel":"pedestal_rms_run_channel",
	}

	# Get channels
	year2emap = {"2017":"2017J", "2018":"2018"}
	emap_version = year2emap[view_args.year]
	q_channels = Channel.query.filter(Channel.emap_version==emap_version)
	if view_args.filter_ieta:
		q_channels = q_channels.filter(Channel.ieta.in_(view_args.filter_ieta))
	if view_args.filter_iphi:
		q_channels = q_channels.filter(Channel.iphi.in_(view_args.filter_iphi))
	if view_args.filter_depth:
		q_channels = q_channels.filter(Channel.depth.in_(view_args.filter_depth))
	if view_args.filter_subdet:
		q_channels = q_channels.filter(Channel.subdet.in_(view_args.filter_subdet))
	q_channels.order_by(Channel.subdet.desc(), Channel.depth.desc(), Channel.ieta.desc(), Channel.iphi.desc())
	q_channels = q_channels.limit(view_args.max_channels)

	print("[get_data] Channel query returned {} channels".format(q_channels.count()))

	return_data = []
	if view_args.averaging_method:
		print("ERROR : Averaging not yet implemented! Returning nothing.")
		return return_data
	else:
		for channel in q_channels.all():
			# Return data key = legend entry for channel
			q_data = getattr(channel, backrefs[quantity_name])
			if view_args.min_run:
				q_data = q_data.filter(quantity.run >= view_args.min_run)
			if view_args.max_run:
				q_data = q_data.filter(quantity.run <= view_args.max_run)
			if view_args.exclude_runs:
				q_data = q_data.filter(~quantity.run.in_(view_args.exclude_runs))
			q_data = q_data.order_by(quantity.run)
			q_data = q_data.limit(view_args.max_entries)
			return_data.append({"name":channel.get_label(), "data":[[reading.run, reading.value] for reading in q_data.all()]})
	return return_data

# Routes
@hcal_local.route('/get/<quantity_name>', methods=['GET'])
@jsonpify
def get(quantity_name, max_entries=100, max_channels=100):
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
	print("[get] Query returned {} channels".format(q_channels.count()))

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

@hcal_local.route('/plot/<quantity_name>', methods=['GET'])
def plot(quantity_name):
	valid_quantities = ["PedestalMean_Run_Channel", "PedestalRMS_Run_Channel"]
	if not quantity_name in valid_quantities:
		return render_template("400.html")
	view_args = ViewArgs(request.args)
	json_data = get_data(quantity_name, view_args)

	x_titles = {
		"PedestalMean_Run_Channel":"Run",
		"PedestalRMS_Run_Channel":"Run",
	}

	y_titles = {
		"PedestalMean_Run_Channel":"Pedestal mean [ADC]",
		"PedestalRMS_Run_Channel":"Pedestal RMS [ADC]",
	}

	titles = {
		"PedestalMean_Run_Channel":"Pedestal Mean vs Run",
		"PedestalRMS_Run_Channel":"Pedestal RMS vs Run",
	}

	return render_template("scatterplot.html", json_data=json_data, title=titles[quantity_name], x_title=x_titles[quantity_name], y_title=y_titles[quantity_name])


# Custom commands
from flask.cli import AppGroup
hcal_local_cli = AppGroup('hcal_local')

@hcal_local_cli.command(with_appcontext=True)
@click.option('--quantity')
@click.option('--run')
@click.option('--emap')
@click.option('--overwrite', is_flag=True)
def extract(quantity, run, emap, overwrite):
	quantity_object = eval(quantity)()
	quantity_object.extract(run, emap, overwrite=overwrite)

@hcal_local_cli.command(with_appcontext=True)
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

app.cli.add_command(hcal_local_cli)
