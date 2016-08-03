# Map BBL to location and extract date and hr along with BBL counts

# Loading required libraries
import datetime
import dateutil
import operator
import os
import sys
import time
import pyspark

# Actual Trip Record Fields
# 0	tpep_pickup_datetime
# 30	pickup_longitude
# 31	pickup_latitude

def indexZones(shapeFilename):
	import rtree
	import fiona.crs
	import geopandas as gpd
	index = rtree.Rtree()
	zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
	for idx,geometry in enumerate(zones.geometry):
		index.insert(idx, geometry.bounds)
	return (index, zones)

def findZone(p, index, zones):
	match = index.intersection((p.x-200, p.y-200, p.x+200, p.y+200))
	nearest = (1e6, -1)
	for idx in match:
		nearest = min(nearest, (p.distance(zones.geometry[idx]), idx))
	return nearest[1]

# Declaring as global variable
global geojson_file
geojson_file = sys.argv[2]

def mapToZone(parts):
	import dateutil
	import fiona.crs
	import geopandas as gpd
	import pyproj
	import rtree
	import shapely.geometry as geom
	## setting the projection to epsg:2263
	proj = pyproj.Proj(init="epsg:2263", preserve_units=True) 

	# Using global variable
	global geojson_file
	index, zones = indexZones(geojson_file)

	for line in parts:
		fields = line.strip().split(',')
		if len(fields) == 38:
			if all((fields[0], fields[30], fields[31])):
				# Extracting date and hour
				pickup_datetime = dateutil.parser.parse(fields[0])
				pickup_datehr = str(pickup_datetime.year) + "-" + str(pickup_datetime.month) \
				+ "-" + str(pickup_datetime.day) + "-" + str(pickup_datetime.hour)

				# Getting BBL for lon and lat attributes
				try:
					pickup_location  = geom.Point(proj(float(fields[30]), float(fields[31])))
					pickup_zone = findZone(pickup_location, index, zones)

					if pickup_zone!=-1:
						output = ((str(zones.BBL[pickup_zone]), str(pickup_datehr)), 1)
						yield output
				except ValueError:
					pass

if __name__=='__main__':

	sc = pyspark.SparkContext()

	data = sys.argv[1]

	trips = sc.textFile(data, use_unicode=False).cache()

	output = trips \
		.mapPartitions(mapToZone) \
		.reduceByKey(lambda a, b: (a+b))

	output.saveAsTextFile(sys.argv[-1])
