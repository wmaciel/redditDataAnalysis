'''
Created on Dec 7, 2015

@author: loongchan
'''
from pyspark import SparkConf, SparkContext
import sys, json, datetime

def year_from_utc(utc):
    d = datetime.datetime
    d = d.utcfromtimestamp(int(utc))
    return d.year

def main(argv):
    # setup inputs and outputs
    input_directory = argv[0]
    output_directory = argv[1]

    # spark specific setup
    conf = SparkConf().setAppName('just uncompressing')
    sc = SparkContext(conf=conf)

    # read input
    text = sc.textFile(input_directory)
    text = text.repartition(200)

    # convert to magic json formatting
    loadedJson = text.map(lambda line: json.loads(line))

    # make the json skinnier by removing unwanted stuff
    fullRedditJson = loadedJson.map(lambda jObj: (jObj['body'], jObj['name'], jObj['parent_id'])).cache()
    byYear = loadedJson.map(lambda jObj: (year_from_utc(jObj['created_utc']), jObj))
    year2006 = byYear.filter(lambda (year, obj): int(year) == 2006)
#     year2007 = byYear.filter(lambda (year, obj): int(year) == 2007)
#     year2008 = byYear.filter(lambda (year, obj): int(year) == 2008)
#     year2009 = byYear.filter(lambda (year, obj): int(year) == 2009)
#     year2010 = byYear.filter(lambda (year, obj): int(year) == 2010)
#     year2011 = byYear.filter(lambda (year, obj): int(year) == 2011)
#     year2012 = byYear.filter(lambda (year, obj): int(year) == 2012)
#     year2013 = byYear.filter(lambda (year, obj): int(year) == 2013)
#     year2014 = byYear.filter(lambda (year, obj): int(year) == 2014)
#     year2015 = byYear.filter(lambda (year, obj): int(year) == 2015)

    year2006.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2006.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2007.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2007.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2008.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2008.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2009.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2009.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2010.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2010.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2011.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2011.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2012.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2012.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2013.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2013.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2014.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2014.gz', 'org.apache.hadoop.io.compress.GzipCodec')
#     year2015.coalesce(1).saveAsTextFile(output_directory+'/reddit_submission_2015.gz', 'org.apache.hadoop.io.compress.GzipCodec')

if __name__ == "__main__":
    main(sys.argv[1:]) # [1:] strips out [0]
