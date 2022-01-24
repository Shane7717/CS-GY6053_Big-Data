#!/usr/bin/env python

import sys

for line in sys.stdin:
	key, info = line.strip().split('\t', 1)
	info = info.strip().split(',')
	key = key.strip().split(',')
	date, time = key[-1].strip().split(' ', 1)
	
	try:
		tip = float(info[-3])
		revenue = float(info[-6]) + tip + float(info[-5]) 
			
		print '%s\t%.2f,%.2f' % (date, revenue, tip)
	
	except ValueError:
		continue
