from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, LongType
import sys, json
import operator
from pyspark.mllib.recommendation import *
import pprint
import os


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


# def list_filter


def hash_rating(author_subreddit_rating_rdd, sc):
    sql_context = SQLContext(sc)

    author_sub_schema = StructType([
        StructField("author", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("rating", LongType(), True)
    ])
    asr_df = sql_context.createDataFrame(author_subreddit_rating_rdd, author_sub_schema)

    author_rdd = author_subreddit_rating_rdd.map(lambda (a, s, r): a)
    aid_rdd = author_rdd.distinct().zipWithUniqueId().cache()
    author_id_schema = StructType([
        StructField("author", StringType(), True),
        StructField("author_id", LongType(), True)
    ])
    aid_df = sql_context.createDataFrame(aid_rdd, author_id_schema)
    aid_s_r_df = aid_df.join(asr_df, on='author').drop('author').cache()

    subreddit_rdd = author_subreddit_rating_rdd.map(lambda (a, s, r): s)
    sid_rdd = subreddit_rdd.distinct().zipWithUniqueId().cache()
    subreddit_id_schema = StructType([
        StructField("subreddit", StringType(), True),
        StructField("subreddit_id", LongType(), True)
    ])
    sid_df = sql_context.createDataFrame(sid_rdd, subreddit_id_schema)
    aid_sid_r_df = sid_df.join(aid_s_r_df, on='subreddit').drop('subreddit').cache()
    row_aid_sid_r_rdd = aid_sid_r_df.rdd
    aid_sid_r_rdd = row_aid_sid_r_rdd.map(lambda row: (row.author_id, row.subreddit_id, row.rating))

    return aid_rdd, sid_rdd, aid_sid_r_rdd


def main(comment_dir, submission_dir, output_dir):
    # spark specific setup
    conf = SparkConf().setAppName('Subreddit Recommender')
    sc = SparkContext(conf=conf)

    author_id_rdd = None
    subreddit_id_rdd = None
    translated_rdd = None

    A_ID_SUB_ID_RANK_PFP = output_dir + '/pickles/author_subreddit_rank_rdd'
    SUBREDDIT_ID_PFP = output_dir + '/pickles/subreddit_id_rdd'
    AUTHOR_ID_PFP = output_dir + '/pickles/author_id_rdd'

    if os.path.isdir(A_ID_SUB_ID_RANK_PFP) and os.path.isdir(SUBREDDIT_ID_PFP) and os.path.isdir(AUTHOR_ID_PFP):
        print 'There are pickles for that!'
        print 'Loading...',
        author_id_rdd = sc.pickleFile(A_ID_SUB_ID_RANK_PFP)
        subreddit_id_rdd = sc.pickleFile(SUBREDDIT_ID_PFP)
        translated_rdd = sc.pickleFile(AUTHOR_ID_PFP)
        print 'Done!'
    else:
        print 'Pickles not found :('
        print 'This will take a while...'

        # ((author, subreddit), comment_rank)
        comment_rdd = do_it(sc, comment_dir)

        # ((author, subreddit), submission_rank)
        submission_rdd = do_it(sc, submission_dir)

        # ((author, subreddit),(comment_rank, submission_rank))
        total_rdd = submission_rdd.fullOuterJoin(comment_rdd)

        # (author, subreddit, comment_rank + submission_rank)
        sum_rdd = total_rdd.map(combine_join_results).cache()

        author_id_rdd, subreddit_id_rdd, translated_rdd = hash_rating(sum_rdd, sc)

        print 'Pickling.',
        author_id_rdd.saveAsPickleFile(AUTHOR_ID_PFP)
        print '.',
        subreddit_id_rdd.saveAsPickleFile(SUBREDDIT_ID_PFP)
        print '.',
        translated_rdd.saveAsPickleFile(A_ID_SUB_ID_RANK_PFP)
        print 'Done!'

    model = ALS.train(translated_rdd, 1)

    wanted_author_id = 100

    products_ratings = model.recommendProducts(100, 5)

    wanted_subreddit_ids = map(lambda x: x.product, products_ratings)
    wanted_subredits = subreddit_id_rdd.filter(lambda (s, id): id in wanted_subreddit_ids).collect()
    wanted_subredits = map(lambda (s, id): str(s), wanted_subredits)

    wanted_author = author_id_rdd.filter(lambda (a, id): id == wanted_author_id).collect()
    wanted_author = str(wanted_author[0])

    print 'author:', wanted_author
    print 'Recommended subreddits:'
    print wanted_subredits





if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
