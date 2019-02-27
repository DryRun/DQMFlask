import sys
from dqmdata import db

class Serializable(object):
	@property
	def as_dict(self):
		blacklist = ['_sa_instance_state']
		blacklist.extend(getattr(self, '_serialize_blacklist', []))
		result = {}
		for k,v in self.__dict__.iteritems():
			if k in blacklist:
				continue
			elif isinstance(v, list): #One to Many/Many to Many relationship, add a list of serialized child objects
				result[k] = [i.as_dict for i in v]
			elif isinstance(v, db.Model): #One to One relationship, serialize the child and include it
				result[k] = v.as_dict
			else:
				result[k] = v
		return result
