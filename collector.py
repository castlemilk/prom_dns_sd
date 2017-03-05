import dns.query
import dns.zone
import os
import json
from functools import reduce
import re
import logging
import argparse
import time

def setup_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


class Collector:
	def __init__(self, nameserver,domain, sd_file_dir, loglevel = logging.ERROR):
		self.log = setup_logger('collector')
		if nameserver:
			self.nameserver = nameserver 
			self.log.info('__init__:nameserver:%s', nameserver)
		else:
			raise Exception("Must specify a nameserver")
			self.log.error('NO nameserver given')
		if domain:
			self.domain = domain
			self.log.info('__init__:domain:%s', domain)
		else:
			raise Exception("Must specify a domain")
			self.log.error('NO domain given')
		if os.path.exists(sd_file_dir):
			#the file is there
			self.sd_file_dir = sd_file_dir
			self.log.info('__init__:sd_file_dir:%s', sd_file_dir)
		elif os.access(sd_file_dir, os.W_OK):
			# file does not exist but can create
			self.sd_file_dir = sd_file_dir
			self.log.warn('__init__:dir doesnt exist, but can create:%s', sd_file_dir)
		else:
			print('dirname: {}'.format(sd_file_dir))
			raise Exception("Cannot write to that location")
			self.log.error('__init__:cannot write to given location:%s', sd_file_dir)
			
		
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
		self.log.info('get_domain_services:records:%d', len(items))
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
		domain_file_path = os.path.join(self.sd_file_dir,self.domain.replace('.','_')+'.json',)
		self.log.info('update_domain_file:domain_file_path:%s', domain_file_path)
		try:
			f_read = open(domain_file_path,'r')
			sd_list_dict = json.load(f_read)
			if not isinstance(sd_list_dict, list):
				sd_list_dict = []
			self.log.info('update_domain_file:load success')
		except IOError:
			sd_list_dict = []
			self.log.warn('update_domain_file:file doesnt exist, initialising list')
		except json.decoder.JSONDecodeError:
			self.log.warn('update_domain_file:json_load_fail:initialising')
			sd_list_dict = []
		target_list = list(map(lambda x: x['targets'], sd_list_dict))
		all_targets_active = reduce(list.__add__, list(map(lambda x: x['targets'], sd_list_dict)), []) 
		all_prom_targets = []
		services = list(map(lambda x: x['name'], self.get_domain_services()))	
		sd_list_dict_original = sd_list_dict
		items = []
		
		for port in ports:
			item = {}
			prom_targets=[]
			for service in services:
				if re.search(port['re'], service):
					target = '{}:{}'.format(service, port['port'])
					prom_targets.append(target)
					all_prom_targets.append(target)
			if prom_targets:
				new_items = list(filter(lambda x: not set(all_targets_active).__contains__(x), prom_targets))
				removed_items = list(filter(lambda x: not set(prom_targets).__contains__(x), all_targets_active))
				target_indexes = self.get_target_index(sd_list_dict, prom_targets)
				if target_indexes:
					self.log.info('update_domain_file:target_indexes:%s' % target_indexes)
					for index in target_indexes:
						self.log.info('update_domain_file:index:%s' % index)
						if new_items:
							self.log.info('update_domain_file:%s:new_items:%s' % (index, new_items))
							sd_list_dict[index]['targets'] += new_items
							self.log.info('update_domain_file:added:sd_list_dict[%s][targets]:%s' % (index, sd_list_dict[index]['targets']))
						if removed_items:
							self.log.info('update_domain_file:%s:removed_items:%s' % (index, list(set(removed_items) & set(sd_list_dict[index]['targets']))))
							sd_list_dict[index]['targets'] = list(filter(lambda x: x not in removed_items,
							sd_list_dict[index]['targets']))
				elif new_items:
					self.log.info('update_domain_file:new_items:%s' % (new_items))
					item['targets'] = prom_targets
					item['labels'] = port['labels']
					sd_list_dict.append(item)
		sd_list_dict = list(filter(lambda x: set(x['targets']) & set(all_prom_targets), sd_list_dict))
				
		with open(domain_file_path, 'w') as f_write:
			json.dump(sd_list_dict, f_write, sort_keys=True, indent=4, separators=(',', ': '))	
			self.log.info('update_domain_file:update complete')


	def get_target_index(self, sd_list_dict = [], targets = []):
		return [sd_list_dict.index(item) for item in filter(lambda x: bool(set(targets) & set(x.get('targets'))), sd_list_dict)]
	def get_removable_index(self, sd_list_dict= [], targets = []):
		return [sd_list_dict.index(item) for item in filter(lambda x: bool(len(set(targets) & set(x.get('targets')))==len(x.get('targets'))), sd_list_dict)]
	
	

def main():
	""" DNS service discovery via DNS zone transfers for prometheus file_sd """	
	parser = argparse.ArgumentParser(description='''DNS service discovery via zone transfers for prometheus file_sd''')
	
	# arguments
	parser.add_argument('-i', '--interval',type=int, dest='interval', help='time interval in seconds to poll the DNS server')
	parser.add_argument('-n', '--nameserver', dest='nameserver', help='nameserver to poll')
	parser.add_argument('-z', '--zone', dest='zone', help='domain zone to discover services from')
	parser.add_argument('-f', '--file', dest='file_dest', help='file destination for service discovery')


	args = vars(parser.parse_args())
	
	collector = Collector(args['nameserver'], args['zone'], args['file_dest'])
	while True:
		collector.update_domain_file([
		{
                        'name': 'node-exporter',
                        're' : '.*',
                        'port' : '9100',
                        'labels': { 'service' : 'node' },
                },
                {
                        'name': 'JMX',
                        're' : 'kafka.*',
                        'port' : '7071',
                        'labels': { 'service': 'JMX' },
                },
                ])
		time.sleep(args['interval'])
if __name__=='__main__':
	main()
