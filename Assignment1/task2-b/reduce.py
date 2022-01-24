#!/usr/bin/env python

import sys

count = 0

for line in sys.stdin:
	key, value = line.strip().split('\t', 1)
	count = count + int(value)
	
print '%d' % (count)
