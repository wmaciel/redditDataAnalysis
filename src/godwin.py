from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
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
    expression  = '^.*(' + expression[0:-1] + ').*$'
    return expression

def main(argv):
    # list of words to look for!
    GODWINS_WORDS = ['hitler', 'nazi', 'nazis', 'holocaust', 'auschwitz', 'dog', 'cat']

    # setup inputs and outputs
    input_directory = argv[0]

    # spark specific setup
    conf = SparkConf().setAppName('godwin whaaa')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # read input
    text = sc.textFile(input_directory)
    text = text.repartition(300)

    # convert to magic json formatting
    loadedJson = text.map(lambda line: json.loads(line))

    # code from greg for regex to parse lines
    linere = re.compile(regex_from_words(GODWINS_WORDS))

    # make the json skinnier by removing unwanted stuff
    fullRedditJson = loadedJson.map(lambda jObj: (jObj['subreddit'], jObj['body'], jObj['name'].encode('ascii', 'ignore'), jObj['parent_id'])).cache()

    # now filter out stuff without GODWINS_WORDS "body","id", "subreddit", "parent_id" 
    godwinJsonList = fullRedditJson.filter(lambda (subreddit, body, name, parent_id): linere.match(body.lower())).cache()

    # Now we convert BOTH filteredJsonList AND loadedJson into sparkSQL
    subredditSchema = StructType([
        StructField("subreddit", StringType(), True),
        StructField("body", StringType(), True),
        StructField("name", StringType(), True),
        StructField("parent_id", StringType(), True)
    ])
    fullRedditDF = sqlContext.createDataFrame(fullRedditJson, subredditSchema)
    godwinRedditDF = sqlContext.createDataFrame(godwinJsonList, subredditSchema)

    # inner join both massive tables together to match up Fist level of ids........
    firstLevelTest = godwinRedditDF.join(fullRedditDF, [godwinRedditDF.parent_id == fullRedditDF.name], 'inner')
    firstLevelTest.show(800)


if __name__ == "__main__":
    main(sys.argv[1:]) # [1:] strips out [0]
