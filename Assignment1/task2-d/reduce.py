#!/usr/bin/env python

import sys

curr_key = None
total_revenue = 0
total_tip = 0

for line in sys.stdin:
	key, value = line.strip().split('\t', 1)
	curr_revenue, curr_tip = value.strip().split(',', 1)
	curr_revenue = float(curr_revenue)
	curr_tip = float(curr_tip)
	
	if curr_key == None:
		curr_key = key;
	if curr_key == key:
		total_revenue = total_revenue + curr_revenue
		total_tip = total_tip + curr_tip		
	else:
		print '%s\t%.2f,%.2f' % (curr_key, total_revenue, total_tip)
		curr_key = key
		total_revenue = 0
		total_tip = 0
		total_revenue = total_revenue + curr_revenue
		total_tip = total_tip + curr_tip

if curr_key != None:
	print '%s\t%.2f,%.2f' % (curr_key, total_revenue, total_tip)
