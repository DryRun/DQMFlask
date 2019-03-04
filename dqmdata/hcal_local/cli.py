import click
from flask import current_app
from flask_cli import with_appcontext

@app.cli.command(with_appcontext=False)
@click.option('--version')
@click.option('--path')
def process_emap(version, path):
	print "Bad!"
	from models import Channel
	emap = open(path, 'r')
	for line in emap:
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
		db.session.commit()

#@app.cli.command(with_appcontext=False)
#@click.option('--version')
#@click.option('--path')
#def process_emap(version, path):

