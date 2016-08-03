import datetime
import operator
import os
import sys
import time

import pyspark

### 0          1                2                 3                4              5                 6                7          8                   9                  10                11
### vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount
### CMT,2014-06-30 00:10:16,2014-06-30 00:25:10,1,4.4000000000000004,-73.994076000000007,40.720134000000002,1,N,-73.957111999999995,40.680574999999997,CRD,15.5,0.5,0.5,2.5,0,19

###app,request_datetime,req_pickup_lat,request_pickup_long,request_outcome,wav_request,wday,hour
###EH0008,01/03/2016 23:59:32,40.63671,-73.92336,2
###
def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

## creating a 200 feet box around the point , assigning the nearest polygon 
def findZone(p, index, zones):
    match = index.intersection((p.x-200, p.y-200, p.x+200, p.y+200))
    nearest = (1e6, None)
    for idx in match:
        nearest = min(nearest, (p.distance(zones.geometry[idx]), idx))
    return nearest[1]

# ZONES='NYBlocks'

def mapToZone(partIndex, parts):
    # global ZONES
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('mergedPLUTO.geojson')
    # index, zones = indexZones('taxi_zones.geojson')

    if partIndex==0:
        parts.next()
        ### (u'V912836', 1, 2015, 8, 15, 22, 16, u'-73.930557', u'40.820133') 
    ##### TPEP & LPEP
    # for line in parts:
    #     fields = line.strip().translate(None,"()").split(',')
    #     shl_number = fields[0].translate(None,"u''")
    #     year = int(fields[2])
    #     month = int(fields[3])
    #     day = int(fields[4])
    #     hour = int(fields[5])
    #     minutes = int(fields[6])
    #     longitude = float(fields[7].translate(None,"u''"))
    #     latitude = float(fields[8].translate(None,"u''"))

    ##### EHail
    for line in parts:
        fields = line.strip().split(',')
        shl_number = fields[1]
        time_str = fields[3]
        time_element = datetime.datetime.strptime(time_str,'%m/%d/%Y %H:%M')
        year = time_element.year
        month = time_element.month
        day = time_element.day
        hour = time_element.hour
        minutes = time_element.minute
        longitude = float(fields[5])
        latitude = float(fields[2])
        outcome = float(fields[4])
	if longitude > 0:
            pickup_location  = geom.Point(proj(latitude,longitude))
        else:
            pickup_location  = geom.Point(proj(longitude,latitude))
        pickup_zone = findZone(pickup_location, index, zones)
        
        if (year == 2015) & (pickup_zone>=0):
            yield ((shl_number,year,month,day,hour,minutes,zones.BBL[pickup_zone],outcome),1)







if __name__=='__main__':
    # global ZONES
    if len(sys.argv)<3:
        print "Usage: <input files> <output path> <ZONES.geojson>"
        sys.exit(-1)
    # ZONES = sys.argv[-1]

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1]),use_unicode=False)

    output = trips \
        .mapPartitionsWithIndex(mapToZone) \
        .reduceByKey(lambda a, b: a+b, 128);
        # .map(lambda (k,v): '%s,%02d-%02d,%s' % (k[0], k[1][0], k[1][1], v))
    output.saveAsTextFile(sys.argv[-1])
