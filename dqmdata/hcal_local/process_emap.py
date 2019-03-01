# Save an emap into DB
import os
import sys
import re
from models import Channel

def emap_delete(emap_version):
	for channel in Channel.query.filter_by(emap_version=emap_version):
		db.session.delete(channel)

def emap_process(emap_version, emap_path):
	emap = open(emap_path, 'r')
	for index, line in enumerate(emap):
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
			emap_version  = emap_version,
			index         = index
			)
		channel.save()


if __name__ == "__main__":
	from argparse import ArgumentParser
	parser = ArgumentParser()
	parser.add_argument('emap_version', type=str, help="Version string")
	parser.add_argument('--add', type=str, help="Path to emap")
	parser.add_argument('--delete', action='store_true', help="Delete all rows with specified version")
	args = parser.parse_args()
