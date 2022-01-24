#!/usr/bin/env python

import sys

for line in sys.stdin:
	key, info = line.strip().split('\t', 1)
	info = info.strip().split(',')
	key = key.strip().split(',')
	medallion = key[0]
	driver = key[1]	
	
	print '%s\t%s' % (driver, medallion)
