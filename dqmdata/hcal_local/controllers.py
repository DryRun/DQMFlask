# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db, app

# Import models
#from dqmdata.hcal_local.models.common import Channel # Moved to dqmdata.common
from dqmdata.hcal_local.models.pedestalmean_run_channel import PedestalMean_Run_Channel
from dqmdata.hcal_local.models.pedestalrms_run_channel import PedestalRMS_Run_Channel
#from dqmdata.hcal_local.models.sipmgaina_run_channel import SiPMGainA_Run_Channel
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
	q_channels = q_channels.order_by(Channel.subdet, Channel.ieta, Channel.iphi, Channel.depth)
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


# mapcount: count lines in a file
import mmap
def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines

@hcal_local_cli.command(with_appcontext=True)
@click.option('--version')
@click.option('--path')
def process_emap(version, path):
	print("process_emap({}, {})".format(version, path))
	#from models.channel import Channel
	nlines = mapcount(path)
	print_every = int(math.floor(nlines / 20))
	emap = open(path, 'r')
	counter = 0
	for line in emap:
		if counter % print_every == 0:
			print("On line {}/{}".format(counter, nlines))
		counter += 1
		if line[0] == "#":
			continue
		if len(line.split()) < 10:
			continue
		contents = line.split()
		crate         = int(contents[1])
		slot          = int(contents[2])
		dcc           = int(contents[4])
		spigot        = int(contents[5])
		fiber         = int(contents[6])
		fiber_channel = int(contents[7])
		subdet        = str(contents[8])
		ieta          = int(contents[9])
		iphi          = int(contents[10])
		depth         = int(contents[11])

		if not subdet in ["HB", "HE", "HF", "HO", "HEP17"]:
			continue

		channel = Channel(
			subdet        = subdet,
			ieta          = ieta,
			iphi          = iphi,
			depth         = depth,
			crate         = crate,
			slot          = slot,
			dcc           = dcc,
			spigot        = spigot,
			fiber         = fiber,
			fiber_channel = fiber_channel,
			emap_version  = version,
			)
		db.session.add(channel)
		if counter % 200:
			db.session.flush()
	db.session.commit()

@hcal_local_cli.command(with_appcontext=True)
@click.option('--version')
def delete_emap(version):
	counter = 0
	for channel in Channel.query.filter_by(emap_version=version):
		db.session.delete(channel)
		if counter % 200 == 0:
			db.session.flush()
		counter += 1
	db.session.commit()


app.cli.add_command(hcal_local_cli)
