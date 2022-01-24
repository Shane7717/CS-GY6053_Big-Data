#!/usr/bin/env python

import sys

for line in sys.stdin:
	key, info = line.strip().split('\t', 1)
	info = info.strip().split(',')
	try:
		if float(info[-1]) <= 15:    	
			print '%s\t%d' % ('<=15', 1)
	except ValueError:
		continue
