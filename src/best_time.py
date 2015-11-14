from pyspark import SparkConf, SparkContext
import sys
import json


def add_pairs(p1, p2):
    return p1[0] + p2[0], p1[1] + p2[1]


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('best time to post')
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)

# Each element of loaded json is a tuple with all the attributes of the json line
loadedJson = text.map(lambda line: json.loads(line)).cache()
