import sys

from pyspark import SparkContext

# This scrip expects the path to an unzip collection of 
# emails from one of the zip email in the v2 data from
# the source
# usage : spark-submit --master yarn-client TechnicalTestQ2.py /data/text_000


if __name__ == "__main__":
    """
        Usage: TechnicalTestQ2 [txt email folder path on hdfs]
    """
    sc = SparkContext(appName="Sainsbury's Technical Assessment Q1")

    top_100 = sc.wholeTextFiles(sys.argv[1] + "/*.txt",20)\
	.flatMap(lambda (x,z): ( (x, y.replace("\t","")) for y in z.split("\n") ) )\
	.filter(lambda (x,z): len(z) >= 1 and z != "" and z.find("To:",0,4) >=0 )\
    .map( lambda (x,z): (x,z.split(":")[1]))\
    .filter(lambda (x,z): len(z) >= 1 and z != "")\
	.flatMap(lambda (x,z): ( (x, y.replace("\t","").replace("\r","")) for y in z.split(",") ) )\
	.flatMap(lambda (x,z): ( (x, y.replace("\t","").replace("\r","")) for y in z.split(";") ) )\
	.map( lambda (x,z): (z,1))\
    .reduceByKey(lambda x,y: x+y)\
	.sortBy(lambda x: x[1],ascending=False)\
	.take(100)
	
	print(top_100)
    sc.stop()