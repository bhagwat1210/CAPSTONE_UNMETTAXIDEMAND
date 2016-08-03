## Author Taxi Capstone Team
## Segregrate data in supply i.e breadcrumb into occupied(1) and free minute(0) by comparing it
## with the triprecord(observed demand) data. 

import numpy as np 
import pandas as pd
import datetime
import pyspark
import sys




def oper(value):
    for i in value[1][1]:
        if i[0]<=value[1][0][0]<=i[1]:
            return 1
        #else:
    return 0       
        


if __name__=='__main__':

    
  sc = pyspark.SparkContext()

  #hardcoding the input files,these files should be available on cluster
  dat = sc.textFile('TripRecord_CMT.csv')
  dat_b = sc.textFile('BreadCrumb_CMT.csv')

  ## mapping from the pickup data creating taxi shl number and the list of pickup timestamp, drop of time stamd

  dat3 = dat.map(lambda x: x.split(',')).filter(lambda x : len(x[2])!=0 and len(x[13])!=0).map(lambda x: (x[10],\
  (datetime.datetime.strptime(x[2], "%m-%d-%Y %H:%M:%S"),\
  datetime.datetime.strptime(x[13], "%m-%d-%Y %H:%M:%S"))))\
  .map(lambda (x,y): (x, [y])).reduceByKey(lambda p,q: p+q)

     ## mapping the bread crum data and creating shl number as taxi using timestamp
  dat_b1 = dat_b.map(lambda x: x.strip().split(','))\
  .filter(lambda x : len(x)==6 and len(x[2])==19)\
  .map(lambda x : (x[0],(datetime.datetime.strptime(x[2], "%m-%d-%Y %H:%M:%S"),x[3],x[4])))   

    ## joining the dataset data_b1 and dat3(pickup), this will join on index which is taxi number
  dat_final = dat_b1.join(dat3)

	## marking 1 to all the timestamp of breadcrumb data which falls under the pickup and dropoff of the lpep_pickup data
	##  using UDF oper funcion
  dat_final_1 = dat_final.map(lambda x: (x,1) if oper(x) else (x,0))
 	
 	## getting free minute for CMT data

  dat = dat_final_1.map(lambda x : (x[0][0],\
    x[1],x[0][1][0][0].year,x[0][1][0][0].month,x[0][1][0][0].day,x[0][1][0][0].hour,x[0][1][0][0].minute,x[0][1][0][1],x[0][1][0][2])).filter(lambda x: x[1]==0) 

    ## saving to output*/

  dat.saveAsTextFile(sys.argv[-1]);






      








