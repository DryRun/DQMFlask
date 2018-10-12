import sys
from dqmdata import db

def detid_to_histbins(subdet, ieta, iphi):
	if ieta < 0:
		if subdet == "HF":
			xbin = ieta + 42
		else:
			xbin = ieta + 43
	else:
		if subdet == "HF":
			xbin = ieta + 43
		else:
			xbin = ieta + 42
	ybin = iphi
	return xbin, ybin

def check_overwrite(quantity, run, emap_version, overwrite):
	q = quantity.query.filter(quantity.run==run)
	if q.count() > 0:
		if not overwrite:
			print "[check_overwrite] ERROR : Run {} already exists in DB for quantity {}! Specify overwrite to overwrite.".format(run, quantity)
			return False
		else:
			print "[check_overwrite] INFO : Deleting existing entries for run {}, quantity {}".format(run, quantity)
			for reading in q.all():
				print "[check_overwrite] INFO : Deleting ",
				print reading
				db.session.delete(reading)
			db.session.commit()
	return True
