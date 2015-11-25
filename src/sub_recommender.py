from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, json, re
import operator
import pprint


def regex_from_words(words):
    """Creates a regular expression string that would match one of the words from the list."""
    expression = ''
    # create the core of the expression
    for w in words:
        expression += w + '|'
    # add the endings, while removing the unwanted extra '|'
    expression = '^.*(' + expression[0:-1] + ').*$'
    return expression


def initialize_rank(jObj):
    count = 0

    if jObj['name'].startswith('t1_'):
        count = 1
    elif jObj['name'].startswith('t3_'):
        count = 2

    return (jObj['author'], jObj['subreddit']), count


def combine_join_results(((a, s), (cr, sr))):
    if cr is None:
        cr = 0
    if sr is None:
        sr = 0
    return a, s, int(cr) + int(sr)


def do_it(sc, input_directory):
    # read input
    text_rdd = sc.textFile(input_directory)
    text_rdd = text_rdd.repartition(200)

    # each: Loaded Json object
    json_rdd = text_rdd.map(lambda line: json.loads(line))

    # each: ((author, subbreddit), 1 or 2)
    comments_rdd = json_rdd.map(initialize_rank)

    # each: author
    rankings_rdd = comments_rdd.reduceByKey(operator.add)

    return rankings_rdd


def main(comment_dir, submission_dir, output_dir):
    # spark specific setup
    conf = SparkConf().setAppName('Subreddit Recommender')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # ((author, subreddit), comment_rank)
    comment_rdd = do_it(sc, comment_dir)

    # ((author, subreddit), submission_rank)
    submission_rdd = do_it(sc, submission_dir)

    # ((author, subreddit),(comment_rank, submission_rank))
    total_rdd = submission_rdd.fullOuterJoin(comment_rdd)

    # (author, subreddit, comment_rank + submission_rank)
    sum_rdd = total_rdd.map(combine_join_results)

    # pickle it!
    sum_rdd.saveAsPickleFile(output_dir + '/author_subreddit_rank_rdd.p')

    unpickled = sc.pickleFile(output_dir + '/author_subreddit_rank_rdd.p')
    pprint.pprint(unpickled.collect())

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
