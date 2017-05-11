Prometheus Automatic DNS discovery
==================================
## Summary
This service was primarily built to integrate with openstacks dns allocation tool designate, however there was a gap in achieving DNS service
discovery via the built-int tooling in prometheus and DNS services such as Power DNS and Bind.
## How it works
Carries out a basic DNS zone trasfer of requested domains and translates the DNS entries into prometheus targets for file_sd discovery. 
This enables fully automated discovery of all services within a DNS zone. 

Syntax allows for the mapping of targets ports and labels via regex in the following form:

```json
	{
		"re" : ".*",
		"port": "9100",
		"labels": { "service" : "node" },
	}
```

The command line enables the specification of multiple parameters:
```python
python collector.py -i <interval> -n <dns nameserver> -z <dns zone> -f <prometheus sd file>
```
`-i` `--interval`  time interval in seconds to poll the DNS server
`-n` `--nameserver` nameserver', help='nameserver to poll
`-z` `--zone` domain zone to discover services from
`-f` `--file` file destination for service discovery
