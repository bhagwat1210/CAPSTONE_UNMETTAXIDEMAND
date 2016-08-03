# CAPSTONE_UNMETTAXIDEMAND
please refer PPT(Final presentation-detecting unmet taxi demand.pptx) for greater clarification .

This contains the code files used for processing the 300 GB files of breadcrumb(GPS) data of yellow and green cab data provided by our sponsor NYC taxi and limosine commision .
Extensively used the Big Data Paradigm and Apache spark on hadoop cluster(provide by NYU CUSP)
.py files basically contains the Big Data processing that was done on the data for :
1. To extract the free minutes of the taxi using the breadcrumb and triprecord data (provided by TLC)
2. For spatial mapping of the free minute units to the respective Building and Block Level(close to 800k) using R-tree
3. For further aggregration on hourly level we used Hive SQL .

Temporal Analysis-final.ipynb contains the temporal analysis of the free minutes , demand and the unmet ratio

Bucket_analysis.ipynb contains the logic of dividing the unmet ratio i.e demand/free min

tpep2015_triprecord_bbl_mapping.py exclusively contains the mapping of yellow taxi triprecord spatial mapping logic

submit_bbl-1.sh is the file used to execute the program.

Contact bsb342@nyu.edu for further clarification , twitter handle @BhagwatBond 
