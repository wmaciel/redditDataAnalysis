from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, json, re


def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))


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

    return (jObj['user'], jObj['subreddit']), count


def do_it(input_directory):
    # read input
    text_rdd = sc.textFile(input_directory)
    text_rdd = text_rdd.repartition(200)

    # each: Loaded Json object
    json_rdd = text_rdd.map(lambda line: json.loads(line))

    # each: ((user, subbreddit), 1 or 2)
    comments_rdd = json_rdd.map(initialize_rank)

    # each: user
    rankings_rdd = comments_rdd.reduceByKey(add_tuples)

    return rankings_rdd


def main(comment_dir, submission_dir, output_dir):

    # spark specific setup
    conf = SparkConf().setAppName('Subreddit Recommender')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # ((user, subreddit), comment_rank)
    comment_rdd = do_it(comment_dir)

    # ((user, subreddit), submission_rank)
    submission_rdd = do_it(submission_dir)

    # ((user, subreddit),(comment_rank, submission_rank))
    total_rdd = submission_rdd.join(comment_rdd)

    # (user, subreddit, comment_rank + submission_rank)
    sum_rdd = submission_rdd.map(lambda ((u, s), (cr, sr)): (u, s, int(cr)+int(sr)))

    # save it!
    sum_rdd.saveAsTextFile(output_dir)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
