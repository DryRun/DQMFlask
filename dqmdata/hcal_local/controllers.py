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

	return_data = {}
	for channel in q_channels.all():
		# Return data key = legend entry for channel
		channel_label = channel.get_label()
		q_data = getattr(channel, backrefs[quantity_name])
		if "min_run" in request.args:
			q_data = q_data.filter(quantity.run >= int(request.args.get("min_run")))
		if "max_run" in request.args:
			q_data = q_data.filter(quantity.run <= int(request.args.get("max_run")))
		q_data = q_data.limit(max_entries)
		return_data[channel_label] = [[reading.run, reading.value] for reading in q_data.all()]

	return json.dumps(return_data)


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
