#!/usr/bin/env python
import sys
import os
import string
from datetime import datetime as dt
from csv import reader

# get the indexes for the attributes, matches them to keys and values
filename = os.environ.get("mapreduce_map_input_file")

t = "medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude"
f = "medallion,hack_license,vendor_id,pickup_datetime,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount"
t = t.split(',')
f = f.split(',')

# indexes of "keys"
trip_index = [t.index('medallion'),t.index('hack_license'),t.index('vendor_id'),t.index('pickup_datetime')]

# indexes of "values"
trip_content = []

for i in range(len(t)):
    if i not in trip_index:
        trip_content.append(i)

# indexes of "keys"
fare_index = [f.index('medallion'),f.index('hack_license'),f.index('vendor_id'),f.index('pickup_datetime')]

# indexes of "values"
fare_content = []

for i in range(len(f)):
    if i not in fare_index:
        fare_content.append(i)

# split the lines into a pair
for line in sys.stdin:
    if 'trip' in filename:
        info = line.strip().split(',')

	# if this is the first row of attributes
        if info[0] == 'medallion':
            continue
        keys = list(info[i] for i in trip_index)
	# TODO
        values = list(info[i] for i in trip_content) 
        print '%s\t%s' % (','.join(keys), ','.join(values))

    elif 'fare' in filename:
        info = line.strip().split(',')
        
	# if this is the first row of attributes
	if info[0] == 'medallion':
            continue
        keys = list(info[i] for i in fare_index)
	# TODO
	values = list(info[i] for i in fare_content)
        print '%s\t%s' % (','.join(keys), ','.join(values))
