#!/usr/bin/env python

import sys

curr_key = None
medallion = []

for line in sys.stdin:
	key, value = line.strip().split('\t', 1)
		
	if curr_key == None:
		curr_key = key 
		medallion.append(value)	

	if curr_key == key:
		if value not in medallion:
			medallion.append(value)
						
	else:
		print '%s\t%d' % (curr_key, len(medallion))
		curr_key = key
		medallion = []
		medallion.append(value)

if curr_key != None:
	print '%s\t%d' % (curr_key, len(medallion))
