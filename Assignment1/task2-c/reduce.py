#!/usr/bin/env python

import sys

curr_key = None
count = 0

for line in sys.stdin:
	key, value = line.strip().split('\t', 1)
	if curr_key == None:
		curr_key = key;
	if curr_key == key:
		count = count + int(value)
	else:
		print '%s\t%d' % (curr_key, count)
		curr_key = key
		count = 0
		count = count + int(value)

if curr_key != None:
	print '%s\t%d' % (curr_key, count)
