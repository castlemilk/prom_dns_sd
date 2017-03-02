Prometheus Automatic DNS discovery
==================================
Summary
##
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
