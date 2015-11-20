from pyspark import SparkConf, SparkContext
import sys
import json
import datetime

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))


def main(inputPath, outputPath):
    conf = SparkConf().setAppName('check reddit structure')
    sc = SparkContext(conf=conf)

    first_text = sc.textFile(inputPath).first()
    
    #repartition, hopefully, we work this better?
    #text = text.repartition(100)

    # each: a dict representing a json object
    loadedJson = first_text.map(lambda line: json.loads(line))

    print loadedJson.collect()

    # each: (hour, score, count)
    #hour_score_1 = loadedJson.map(lambda x: (hour_from_utc(x['created_utc']), (int(x['score']), 1)))

    # each: (hour, score, total_count)
    #hour_score_count = hour_score_1.reduceByKey(add_tuples)

    #each: (hour, average_score)
    #hour_avg = hour_score_count.map(lambda (hour, sum_count): (hour, float(sum_count[0])/sum_count[1]))

    #hour_avg.saveAsTextFile(outputPath)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
