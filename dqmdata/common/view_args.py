from dqmdata.common.utilities import parse_integer_range

class ViewArgs:
	def __init__(self, args):
		self.averaging_method = args.get('avg', default=None, type=str)
		self.year             = args.get("year", default="2018", type=str)
		self.filter_subdet    = args.get("subdet", default=None, type=str)
		if self.filter_subdet:
			self.filter_subdet = self.filter_subdet.split(",")
		self.filter_ieta      = args.get("ieta", default=None, type=str)
		if self.filter_ieta:
			self.filter_ieta = parse_integer_range(self.filter_ieta)
		self.filter_iphi      = args.get("iphi", default=None, type=str)
		if self.filter_iphi:
			self.filter_iphi = parse_integer_range(self.filter_iphi)
		self.filter_depth     = args.get("depth", default=None, type=str)
		if self.filter_depth:
			self.filter_depth = parse_integer_range(self.filter_depth)
		self.max_channels     = args.get("max_channels", default=100, type=int)
		self.max_entries      = args.get("max_entries", default=100, type=int)
		self.min_run          = args.get("min_run", default=None, type=int)	
		self.max_run          = args.get("max_run", default=None, type=int)
		self.exclude_runs     = args.get("exclude_runs", default=None, type=str)
		if self.exclude_runs:
			self.exclude_runs = parse_integer_range(self.exclude_runs)
