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

def row_into_node(r):
    body, name, parent_id = r
    return name, parent_id

def compute_average_godwin(nodes_per_depth):
    sum_of_n = 0
    sum_of_d_x_n = 0
    for d, n in nodes_per_depth.items():
        sum_of_n += n
        sum_of_d_x_n += d * n

    return float(sum_of_d_x_n)/sum_of_n


def main(argv):
    # list of words to look for!
    GODWINS_WORDS = ['hitler', 'nazi']

    # setup inputs and outputs
    input_directory = argv[0]
    output_directory = argv[1]

    # spark specific setup
    conf = SparkConf().setAppName('godwin whaaa')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # read input
    text = sc.textFile(input_directory)
    text = text.repartition(200)

    # convert to magic json formatting
    loadedJson = text.map(lambda line: json.loads(line))

    # make the json skinnier by removing unwanted stuff
    fullRedditJson = loadedJson.map(lambda jObj: (jObj['body'], jObj['name'], jObj['parent_id'])).cache()

    # code from greg for regex to parse lines
    linere = re.compile(regex_from_words(GODWINS_WORDS))

    # now filter out stuff without GODWINS_WORDS "body","id", "subreddit", "parent_id" 
    godwinJsonList = fullRedditJson.filter(lambda (body, name, parent_id): linere.match(body.lower()))
    
    # We don't need the comment body anymore...
    # We need to find the paths now...
    godwin_node_rdd = godwinJsonList.map(row_into_node).cache()
    full_node_rdd = fullRedditJson.map(row_into_node)

    # we also need a list of node names so we can later check if we already visited it.
    godwinNodes = godwin_node_rdd.map(lambda (name, parent_id): name)

    # Convert full data RDD into SQL Data Frame
    subredditSchema = StructType([
        StructField("name", StringType(), True),
        StructField("parent_id", StringType(), True)
    ])
    full_node_df = sqlContext.createDataFrame(full_node_rdd, subredditSchema)

    # Convert godwin rows RDD into SQL Data Frame
    godwinSchema = StructType([
        StructField("g_name", StringType(), True),
        StructField("g_parent_id", StringType(), True)
    ])
    godwin_node_df = sqlContext.createDataFrame(godwin_node_rdd, godwinSchema).cache()

    count_down = godwin_node_df.count()
    print 'There are', count_down, 'comments with a godwins word'
    depth = 0
    nodes_per_depth = {}
    visited_node_list_df = godwin_node_df.select(godwin_node_df.g_name)
    print 'visited_node_list_df'
    print str(visited_node_list_df.count())
    
    while count_down > 0 and depth < 100:

        depth += 1
        # Join find next layer of nodes
        joined_df = godwin_node_df.join(full_node_df,
                                        [godwin_node_df['g_parent_id'] == full_node_df['name']])
        
        # Drop the columns of the older node
        next_node_df = joined_df.select(
            joined_df['name'].alias('g_name'),
            joined_df['parent_id'].alias('g_parent_id')).cache()
        print 'next_node_df count: '+str(next_node_df.count())
        
        # Select only the ones that have NOT been visited
        # TODO: is there a better way?
        leftt = next_node_df.join(visited_node_list_df, next_node_df.g_name == visited_node_list_df.g_name, 'left')
        next_node_df = leftt.select(next_node_df.g_name, next_node_df.g_parent_id, visited_node_list_df.g_name.alias('dup'))
        next_node_df = next_node_df.fillna({'dup':'xxxxxx'})
        next_node_df = next_node_df.filter(next_node_df.dup == 'xxxxxx')
        next_node_df = next_node_df.drop(next_node_df.dup)


        # add the g_name to the list of visited nodes 
        # TODO: make more efficient!
        visited_df = next_node_df.select(next_node_df.g_name)
        visited_node_list_df = visited_node_list_df.unionAll(visited_df)
        visited_node_list_df = visited_node_list_df.dropDuplicates()
        
        count_up = next_node_df.count()
        n_nodes = count_down - count_up
        print 'number of godwin nodes of heignt', depth, '=', n_nodes
        nodes_per_depth[depth] = n_nodes
        count_down = count_up

        godwin_node_df = next_node_df

    avg = compute_average_godwin(nodes_per_depth)
    print 'The average distance to the godwin words is', avg

    fp = open(output_directory + 'average.txt')
    fp.write(str(avg) + '\n')
    fp.close()


if __name__ == "__main__":
    main(sys.argv[1:]) # [1:] strips out [0]
