from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, LongType
import sys, json
import operator
from pyspark.mllib.recommendation import *
import pprint
import os
import pickle


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


def main(comment_dir, submission_dir, output_dir, author, n):
    # spark specific setup
    conf = SparkConf().setAppName('Subreddit Recommender')
    sc = SparkContext(conf=conf)

    model = None
    author_id_rdd = None
    subreddit_id_rdd = None

    MODEL_PFP = output_dir + '/pickles/model'
    AUTHOR_ID_PFP = output_dir + '/pickles/author_id_rdd'
    SUBREDDIT_ID_PFP = output_dir + '/pickles/subreddit_id_rdd'

    if os.path.isdir(MODEL_PFP) and os.path.isdir(SUBREDDIT_ID_PFP) and os.path.isdir(AUTHOR_ID_PFP):
        print 'Loading model...',
        model = MatrixFactorizationModel.load(sc, MODEL_PFP)
        author_id_rdd = sc.pickleFile(AUTHOR_ID_PFP)
        subreddit_id_rdd = sc.pickleFile(SUBREDDIT_ID_PFP)
        print 'Done!'
    else:
        print 'Model not found :('
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

        print 'Training...',
        model = ALS.train(translated_rdd, 1)
        print 'Saving...',
        model.save(sc, MODEL_PFP)
        author_id_rdd.saveAsPickleFile(AUTHOR_ID_PFP)
        subreddit_id_rdd.saveAsPickleFile(SUBREDDIT_ID_PFP)
        print 'Done!'

    print 'Getting recommendations...'

    wanted_author_id = author_id_rdd.filter(lambda (a, a_id): str(a) == str(author)).collect()
    wanted_author_id = int(wanted_author_id[0][1])

    products_ratings = model.recommendProducts(wanted_author_id, int(n))

    wanted_subreddit_ids = map(lambda x: x.product, products_ratings)
    wanted_subredits = subreddit_id_rdd.filter(lambda (sub, s_id): s_id in wanted_subreddit_ids).collect()
    wanted_subredits = map(lambda (sub, s_id): sub, wanted_subredits)

    print 'author:', author
    print 'Recommended subreddits:'
    print wanted_subredits

    fp_out = open(output_dir + '/recommendation.txt', 'w')
    fp_out.write('Recommendations for /u/' + author + ':\n')
    for i, s in enumerate(wanted_subredits):
        fp_out.write(str(i) + ': /r/' + str(s) + '\n')
    fp_out.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
