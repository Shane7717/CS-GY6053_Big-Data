#!/usr/bin/env python

import sys

for line in sys.stdin:
	key, info = line.strip().split('\t', 1)
	info = info.strip().split(',')
	
	print '%s\t%d' % (info[3], 1)
