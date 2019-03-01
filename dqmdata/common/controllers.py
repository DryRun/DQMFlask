# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db
from dqmdata.common.models.local_run import LocalRun

common = Blueprint('common', __name__, url_prefix='/common')

import click
import math
from dqmdata import app

# Routes
@common.route('/emap/', methods=['GET'])
def emap():
	pass


# Command line utilities

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

@app.cli.command(with_appcontext=True)
@click.option('--version')
@click.option('--path')
def process_emap(version, path):
	from models.channel import Channel
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
