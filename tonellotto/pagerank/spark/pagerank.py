import re
import sys
from operator import add
from pyspark import SparkContext

_author__ = "Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella and Rambod Rahmani"
__copyright__ = "Copyright (C) 2021 Leonardo Turchetti, Lorenzo Tunelli, Ludovica Cocchella and Rambod Rahmani"
__license__ = "GPLv3"

def parseLine(line):
    """
    COMMENT ME
    """
    title =  re.findall(r'<title>(.*?)</title>', line)
    outlinks =  re.findall(r'\[\[([^]]*)\]\]', line)
    return title[0], outlinks

def countContributions(outlinks, pageRank):
    """
    COMMENT ME
    """
    count = len(outlinks)
    for link in outlinks:
        yield(link, pageRank/count)

if __name__ == "__main__":
    # parse command line arguments
    iterations = int(sys.argv[1])
    alfa = float(sys.argv[2])
    inputFile = sys.argv[3]

    # connect to the Spark cluster
    master = "yarn"
    sc = SparkContext(master, "PageRank")

    # create input file RDD
    inputRDD = sc.textFile(inputFile)

    # count the number of rows in the input RDD (nodes)
    N = inputRDD.count()

    # broadcast number of nodes to the workers
    broadcastN = sc.broadcast(N)

    # build hyperlink graph: graph = list(K, V), K=title[0], V=[outlinks]
    graph = inputRDD.map(lambda line: parseLine(line))
    graph.saveAsTextFile("initial-graph")

    # compute initial pageranks
    # pageRanks = list(K, V), K=title, V=initialPageRank
    pageRanks = graph.map(lambda node: (node[0], 1/float(broadcastN.value)))

    # compute pageRank iteratively
    for iteration in range(iterations):
        # graph = list(K, V), K=title[0], V=[[outlinks], initialPageRank]
        completeGraph = graph.join(pageRanks)
        completeGraph.saveAsTextFile("completeGraph" + str(iteration))

        # contributions = list(K, V)
        contributions = completeGraph.flatMap(lambda token: countContributions(token[1][0], token[1][1]))
        contributions.saveAsTextFile("contributions" + str(iteration))
        pageRanks = contributions.reduceByKey(add).mapValues(lambda r: alfa*(1/float(broadcastN.value)) + (1 - alfa)*r)
        pageRanks.saveAsTextFile("result-" + str(iteration))

    # order nodes with descending pagerank order
    pageRanksList = pageRanks.takeOrdered(broadcastN.value, key=lambda x: -x[1])
    pageRanksOrdered = pageRanks.sortBy(lambda a: -a[1])

    # print the final page ranks
    for (link, rank) in pageRanksList:
        print("Page: %s Rank: %s" % (link, rank))

    # save ordered pagerank results as text file
    pageRanksOrdered.saveAsTextFile("result-final")

    # stop spark context
    sc.stop()