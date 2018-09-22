from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName("Assig1_PageRank").getOrCreate()

fname = "./web-BerkStan.txt"
data = spark.read.text(fname).rdd.map(lambda r: r[0])

def extractIDs(x):
    temp = x.split("\t")
    return temp

def getValidIDs(x):
    if len(x) == 2:
        return x[0].isdigit() and x[1].isdigit()
    return False

def calc(page2list, rank):
    n = len(page2list)
    for page2 in page2list:
        yield (page2, rank / n)
        
linkinfo = data.map(lambda x : extractIDs(x)).filter(lambda x : getValidIDs(x)).groupByKey()#.cache()
ranks = linkinfo.map(lambda url_neighbors : (url_neighbors[0], 1.0))

n_iterations = 10
for _ in range(n_iterations):
    contributions = linkinfo.join(ranks).flatMap(
          lambda page1_page2list_rank1 : calc(page1_page2list_rank1[1][0], page1_page2list_rank1[1][1]))
    ranks = contributions.reduceByKey(add).mapValues(lambda contrib : 0.15 + (0.85 * contrib))
    
ranks.take(10)
        
