# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app

from dqmdata import db, app

# Import models
from dqmdata.hcal_online.models.tdctime_run_channel import TDCTime_Run_Channel
from dqmdata.hcal_online.models.timingcut_run_channel import TimingCut_Run_Channel
from dqmdata.common.models.channel import Channel
from dqmdata.common.models.online_run import OnlineRun

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

hcal_online = Blueprint('hcal_online', __name__, url_prefix='/hcal_online')

import click
import math
from dqmdata import app

# Get individual channel data
from dqmdata.common.utilities import parse_integer_range
from dqmdata.common.view_args import ViewArgs


# Master function for getting data as JSON from the database
def get_data(quantity_name, view_args):
	quantity = eval(quantity_name)
	# Backref dict: map quantity name (i.e. class name) to table name.
	backrefs = {
		"TDCTime_Run_Channel":"tdctime_run_channel",
		"TimingCut_Run_Channel":"timingcut_run_channel",
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
			if view_args.max_run in request.args:
				q_data = q_data.filter(quantity.run <= view_arrgs.max_run)
			if view_args.exclude_runs:
				q_data = q_data.filter(~quantity.run.in_(view_args.exclude_runs))
			q_data = q_data.order_by(quantity.run)
			q_data = q_data.limit(view_args.max_entries)
			return_data.append({"name":channel.get_label(), "data":[[reading.run, reading.value] for reading in q_data.all()]})
	return return_data


@hcal_online.route('/get/<quantity_name>', methods=['GET'])
@jsonpify
def get(quantity_name):
	valid_quantities = ["TDCTime_Run_Channel", "TimingCut_Run_Channel"]
	if not quantity_name in valid_quantities:
		return render_template("400.html")
	view_args = ViewArgs(request.args)
	return json.dumps(get_data(quantity_name, view_args))

@hcal_online.route('/plot/<quantity_name>', methods=['GET'])
def plot(quantity_name):
	valid_quantities = ["TDCTime_Run_Channel", "TimingCut_Run_Channel"]
	if not quantity_name in valid_quantities:
		return render_template("400.html")
	view_args = ViewArgs(request.args)
	json_data = get_data(quantity_name, view_args)

	x_titles = {
		"TDCTime_Run_Channel":"Run",
		"TimingCut_Run_Channel":"Run",
	}

	y_titles = {
		"TDCTime_Run_Channel":"TDC Time [ns]",
		"TimingCut_Run_Channel":"<TS>_Q",
	}

	titles = {
		"TDCTime_Run_Channel":"TDC Time vs Run",
		"TimingCut_Run_Channel":"<TS>_Q vs Run",
	}

	return render_template("scatterplot.html", json_data=json_data, title=titles[quantity_name], x_title=x_titles[quantity_name], y_title=y_titles[quantity_name])


# Custom commands
from flask.cli import AppGroup
hcal_online_cli = AppGroup('hcal_online')

@hcal_online_cli.command(with_appcontext=True)
@click.option('--quantity')
@click.option('--run')
@click.option('--emap')
@click.option('--overwrite', is_flag=True)
def extract(quantity, run, emap, overwrite):
	quantity_object = eval(quantity)()
	quantity_object.extract(run, emap, overwrite=overwrite)

@hcal_online_cli.command(with_appcontext=True)
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

app.cli.add_command(hcal_online_cli)
