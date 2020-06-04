#!/usr/bin/python3

# Author: Igor Lima
# Objective: Given a url, parse and decode
# Use in terminal: python urldecode.py "url"

import sys
import urllib.parse
from pprint import pprint

decoded = urllib.parse.unquote(sys.argv[1])
url = urllib.parse.urlparse(decoded)

output = {}
for key in url._fields:
    if key == 'query':
        output['query'] = {}
        query = getattr(url, key)
        query_list = query.split('&')
        for q in query_list:
            query_key = q.split('=')[0]
            query_value = q.split('=')[1]
            output['query'][query_key] = query_value
    else:
        output[key] = getattr(url, key)

pprint(output)
