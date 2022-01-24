#!/usr/bin/env python

import sys

for line in sys.stdin:
	key, info = line.strip().split('\t', 1)
	info = info.strip().split(',')
	try:
		if float(info[11]) >= 0 and float(info[11]) <= 20:
			print '%s\t%d' % ('0,20', 1)
		elif float(info[11]) >= 20.01 and float(info[11]) <= 40:
			print '%s\t%d' % ('20.01,40', 1)
		elif float(info[11]) >= 40.01 and float(info[11]) <= 60:
			print '%s\t%d' % ('40.01,60', 1)
		elif float(info[11]) >= 60.01 and float(info[11]) <= 80:
			print '%s\t%d' % ('60.01,80', 1)
		elif float(info[11]) >= 80.01:   	
			print '%s\t%d' % ('80.01,infinite', 1) 
	
	except ValueError:
		continue
