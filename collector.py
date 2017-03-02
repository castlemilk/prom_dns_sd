import dns.query
import dns.zone
import os
import json




class Collector:
	def __init__(self, nameserver,domain, sd_file):
		if nameserver:
			self.nameserver = nameserver 
		else:
			raise Exception("Must specify a nameserver")
		if domain:
			self.domain = domain
		else:
			raise Exception("Must specify a domain")
		if os.path.exists(sd_file):
			#the file is there
			self.sd_file = os.path.join(os.getcwd(),sd_file)
		elif os.access(os.path.dirname(os.path.join(os.getcwd(),sd_file)), os.W_OK):
			# file does not exist but can create
			self.sd_file = sd_file
		else:
			print('dirname: {}'.format(os.path.dirname(os.path.join(os.getcwd(),sd_file))))
			raise Exception("Cannot write to that location")
			
		
	def get_domain_services(self, domain = None):
		"""
		Takes nameserver and domain and carries out a zone transfer to retrieve
		all services available
		"""
		items = []
		domain = domain if domain else self.domain
		
		z = dns.zone.from_xfr(dns.query.xfr(self.nameserver, domain))
		names = z.nodes.keys()
		for n in names:
			row = z[n].to_text(n).split(' ')
			name = row[0]
			type = row[-2]
			address=row[-1]
			if type != 'A':
				continue
			
				
			item = {
				'name': '.'.join([name,domain]),
				'address': address,
			}
			items.append(item)
		return items

	def update_file(self, targets = None, port = None, labels = {}):
		"""
		Write out services into the format suitable for Prometheus
		file based service discovery, in the following form:
		[
			{
				"targets": ["targetx:portx", ...],
				 "labels": {
					"<labelname>": "<labelvalue>",
					...,
				}
			},
			...
		]
		sample input:
		targets = ['1.1.1.1','2.2.2.2']
		ports = ['9100', '7070']
		labels = {'service': 'kafka'}
		produces:
		[
			{
				"targets": ["1.1.1.1:9100", "2.2.2.2:9100", "1.1.1.1:7070", "2.2.2.2:7070"]
				"labels": {
					"service": "kafka"
				}
			}
		]
		
		
		"""
		if not isinstance(targets, list):
			targets = [targets]
		if not isinstance(ports, list):
			ports = [ports]
		
		item = {}
		prom_targets = reduce(list.__add__, [ map(lambda x: x+":%s" % port, targets) for port in ports ], [])
		item['targets'] = prom_targets
		if labels:
			item['labels'] = labels
		try:
			f_read = open(self.sd_file,'r')
			sd_list_dict = json.load(f)
		except IOError:
			sd_list_dict = []
		sd_list_dict.append(item)
		print(sd_list_dict)
		#with open(self.sd_file, 'w') as f_write:
		#	json.dump(sd_list_dict, f_write)	
		
if __name__=='__main__':
	nameserver='10.0.0.10'
	domain='mgmt.pants.net'
	collector = Collector(nameserver,domain, 'test_sd_file.json')
	print(collector.get_domain_services())
	collector.update()
