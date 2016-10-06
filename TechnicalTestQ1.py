import sys

from pyspark import SparkContext

# This scrip expects the path to an unzip collection of 
# emails from one of the zip email in the v2 data from
# the source
# usage : spark-submit --master yarn-client TechnicalTestQ1.py /data/text_000

if __name__ == "__main__":
    """
        Usage: testTast1 [txt email folder path on hdfs]
    """
    sc = SparkContext(appName="Sainsbury's Technical Assessment Q1")
	word_count = sc.wholeTextFiles(sys.argv[1] + "/*.txt",20)\
        .flatMap(lambda (x,z): ( (x, y.replace("\t","")) for y in z.split("\n") ) )\
        .flatMap(lambda (x,z): ( (x, y) for y in z.split(" ") ) )\
        .filter(lambda (x,z): len(z) >= 1 and z != "")\
        .map(lambda (x,z): (x,1) )\
        .reduceByKey(lambda x,y: x+y)\
        .map(lambda (x,y): y)\
		.cache()
    total_word_count = word_count.sum()
    no_of_files = word_count.count()
    Avg_word_per_email_file = total_word_count / no_of_files
	print("Average words per email is: " + str(Avg_word_per_email_file))
	
    sc.stop()