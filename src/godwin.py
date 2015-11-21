from pyspark import SparkConf, SparkContext
import sys, json, datetime, re

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))


def main(argv):
    # list of words to look for!
    GODWINS_WORDS = {'hitler':True, 'nazi': True, 'natzis':True, 'holocaust':True, 'auschwitz':True}
    
    # setup inputs and outputs
    input_directory = argv[0]
    
    # spark specific setup
    conf = SparkConf().setAppName('godwin whaaa')
    sc = SparkContext(conf=conf)
    
    # read input
    text = sc.textFile(input_directory)
    text = text.repartition(300)
    
    # convert to magic json formatting
    loadedJson = text.map(lambda line: json.loads(line))
    
    # code from greg for regex to parse lines
    linere = re.compile("^.*(hitler|nazi|nazis|holocaust|auschwitz).*$")
    
    # now filter out stuff without GODWINS_WORDS "body","id", "subreddit", "parent_id" 
    filteredJsonList = loadedJson.filter(lambda jsonBody: linere.match(jsonBody['body'].lower())).cache()
    
if __name__ == "__main__":
    main(sys.argv[1:]) # [1:] strips out [0]
