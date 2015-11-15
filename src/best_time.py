from pyspark import SparkConf, SparkContext
import sys
import json
import datetime

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))

def my_add(a,b):
    return a[0] + b[0], a[1] + b[1]


def hour_from_utc(utc):
    d = datetime.datetime
    d = d.utcfromtimestamp(int(utc))
    return d.hour


def main(inputPath, outputPath):
    conf = SparkConf().setAppName('best time to post')
    sc = SparkContext(conf=conf)

    text = sc.textFile(inputPath)

    # each: a dict representing a json object
    loadedJson = text.map(lambda line: json.loads(line)).cache()

    # each: (hour, score, count)
    hour_score_1 = loadedJson.map(lambda x: (hour_from_utc(x['created_utc']), (int(x['score']), 1)))

    # each: (hour, score, total_count)
    hour_score_count = hour_score_1.reduceByKey(add_tuples)

    #each: (hour, average_score)
    hour_avg = hour_score_count.map(lambda (hour, sum_count): (hour, float(sum_count[0])/sum_count[1]))

    hour_avg.saveAsTextFile(outputPath)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
