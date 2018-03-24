# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db
from dqmdata.hcal_local.models import *
import json

hcal_local = Blueprint('hcal_local', __name__, url_prefix='/hcal_local')

@hcal_local.route('/emap/', methods=['GET'])
def emap():
	pass

# Top level "get" route
@hcal_local.route('/get/<quantity_name>', methods=['GET'])
def get(quantity_name, max_entries=100):
	valid_quantities = ["PedestalMean_Run_Channel", "PedestalRMS_Run_Channel"]
	if not quantity_name in valid_quantities:
		return render_template("400.html")

	channel_filter_keys = ["ieta", "iphi", "subdet", "depth"]
	channel_filters = {}
	for channel_filter_key in channel_filter_keys:
		if channel_filter_key in request.args:
			if channel_filter_key in ["ieta", "iphi", "depth"]:
				value = int(request.args(channel_filter_key))
			else:
				value = request.args(channel_filter_key)
			channels[channel_filter_key] = value

	# Get data
	quantity = eval(quantity_name)
	data = quantity.query.filter_by(**channel_filters)

	if "min_run" in request.args:
		data = data.filter(quantity.run >= int(request.args.get("min_run")))
	if "max_run" in request.args:
		data = data.filter(quantity.run <= int(request.args.get("max_run")))
	
	# debug
	reading0 = data[0]
	print reading0
	print reading0.as_dict()

	return json.dumps([reading.as_dict() for reading in data.limit(max_entries)])


# Custom commands
import mmap
def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines

import click
import math
from dqmdata import app
@app.cli.command(with_appcontext=True)
@click.option('--version')
@click.option('--path')
def process_emap(version, path):
	from models import Channel
	nlines = mapcount(path)
	print_every = int(math.floor(nlines / 20))
	emap = open(path, 'r')
	counter = 0
	for line in emap:
		if counter % print_every == 0:
			print "On line {}/{}".format(counter, nlines)
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
			emap_version  = version
			)
		db.session.add(channel)
		if counter % 200:
			db.session.flush()
	db.session.commit()

@app.cli.command(with_appcontext=True)
@click.option('--version')
def delete_emap(version):
	counter = 0
	for channel in Channel.query.filter_by(emap_version=version):
		db.session.delete(channel)
		if counter % 200 == 0:
			db.session.flush()
		counter += 1
	db.session.commit()

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
@click.option('--emap')
@click.option('--overwrite', is_flag=True)
def delete(quantity, run):
	counter = 0
	for reading in eval(quantity).query.filter_by(run=run):
		db.session.delete(reading)
		if counter % 200 == 0:
			db.session.flush()
		counter += 1
	db.session.commit()
