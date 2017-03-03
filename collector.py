import dns.query
import dns.zone
import os
import json
from functools import reduce
import re




class Collector:
	def __init__(self, nameserver,domain, sd_file_dir):
		if nameserver:
			self.nameserver = nameserver 
		else:
			raise Exception("Must specify a nameserver")
		if domain:
			self.domain = domain
		else:
			raise Exception("Must specify a domain")
		if os.path.exists(sd_file_dir):
			#the file is there
			self.sd_file_dir = os.path.join(os.getcwd(),sd_file_dir)
		elif os.access(os.path.dirname(os.path.join(os.getcwd(),sd_file_dir)), os.W_OK):
			# file does not exist but can create
			self.sd_file_dir = sd_file_dir
		else:
			print('dirname: {}'.format(os.path.dirname(os.path.join(os.getcwd(),sd_file_dir))))
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

	def update_domain_file(self, ports):
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
		domain_file_path = os.path.join(os.getcwd(),self.domain.replace('.','_')+'.json',)
		try:
			f_read = open(domain_file_path,'r')
			sd_list_dict = json.load(f_read)
			if not sd_list_dict:
				sd_list_dict = []
		except IOError:
			sd_list_dict = []
		print("loading sd_dict")
		print(sd_list_dict)
		target_list = list(map(lambda x: x['targets'], sd_list_dict))
		print(target_list)
		all_targets_active = reduce(list.__add__, list(map(lambda x: x['targets'], sd_list_dict)), []) 
		services = list(map(lambda x: x['name'], self.get_domain_services()))	
		items = []
		for port in ports:
			item = {}
			prom_targets=[]
			for service in services:
				if re.search(port['re'], service):
					prom_targets.append('{}:{}'.format(service, port['port']))
			if prom_targets:
				new_items = list(filter(lambda x: not set(all_targets_active).__contains__(x), prom_targets))
				removed_items = list(filter(lambda x: not set(prom_targets).__contains__(x), all_targets_active))
				print("OUTSIDE:")
				print(sd_list_dict)
				indexes_changed = self.get_indexes_changed(sd_list_dict, prom_targets)
				if indexes_changed:
					for index in indexes_changed:
						if new_items:
							sd_list_dict[index]['targets'] += new_items
						if removed_items:
							sd_list_dict[index]['targets'] = list(filter(lambda x: x not in removed_items,
							sd_list_dict[index]['targets']))
				elif new_items:
					item['targets'] = prom_targets
					item['labels'] = port['labels']
					sd_list_dict.append(item)
		
		with open(domain_file_path, 'w') as f_write:
			json.dump(sd_list_dict, f_write, sort_keys=True, indent=4, separators=(',', ': '))	
	def get_indexes_changed(self, sd_list_dict = [], targets = []):
		if sd_list_dict:
			print("____TARGETS: ")
			print(targets)
			print("____SD_LIST_DICT: ")
			print(sd_list_dict)
			return [sd_list_dict.index(item) for item in filter(lambda x: bool(set(targets) & set(x.get('targets'))), sd_list_dict)]
		else:
			return []
	
		
if __name__=='__main__':
	nameserver='10.0.0.10'
	domain='mgmt.pants.net'
	collector = Collector(nameserver,domain, os.getcwd())
	print(collector.get_domain_services())
	collector.update_domain_file([
		{ 
			're' : '.*',
			'port' : '9100', 
			'labels': { 'service' : 'node' },
		},
		{ 
			're' : 'kafka.*',
			'port' : '7070',
			'labels': { 'service': 'JMX' },
		}
		])
