#!/usr/bin/env python

import sys

curr_key = None
total_trip = 0
total_day = []

for line in sys.stdin:
	key, value = line.strip().split('\t', 1)
		
	if curr_key == None:
		curr_key = key 
		total_day.append(value)	

	if curr_key == key:
		if value not in total_day:
			total_day.append(value)

		total_trip = total_trip + 1	
						
	else:
		average = float(total_trip) / len(total_day)
		print '%s\t%d,%.2f' % (curr_key, total_trip, average)
		curr_key = key
		total_day = []
		total_day.append(value)
		total_trip = 1

if curr_key != None:
	average	= float(total_trip) / len(total_day)
        print '%s\t%d,%.2f' % (curr_key, total_trip, average)
