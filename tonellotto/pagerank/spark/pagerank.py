import re
import sys
from operator import add
from pyspark import SparkContext

_author__ = "Leonardo Turchetti, Lorenzo Tonelli, Ludovica Cocchella and Rambod Rahmani"
__copyright__ = "Copyright (C) 2021 Leonardo Turchetti, Lorenzo Tunelli, Ludovica Cocchella and Rambod Rahmani"
__license__ = "GPLv3"

def parseLine(line):
    """
        Called once for each line of the input .xml file.
        Parses the <title></title> and <text>[[]]</text> tags
        to retrieve the page title and the outlinks.
    """
    title =  re.findall(r'<title>(.*?)</title>', line)
    outlinks =  re.findall(r'\[\[([^]]*)\]\]', line)
    return title[0], outlinks

def countContributions(outlinks, pageRank):
    """
        Computes the size of the given outlinks list and
        returns the contribution to each outlink.
    """
    count = len(outlinks)
    for link in outlinks:
        yield(link, pageRank/count)

if __name__ == "__main__":
    # parse command line arguments
    iterations = int(sys.argv[1])
    alfa = float(sys.argv[2])
    inputFile = sys.argv[3]

    # connect to the Hadoop cluster
    master = "yarn"
    sc = SparkContext(master, "PageRank")

    # create input file RDD
    inputRDD = sc.textFile(inputFile)

    # count the number of rows in the input RDD (nodes)
    N = inputRDD.count()

    # broadcast the number of nodes to the workers
    broadcastN = sc.broadcast(N)

    # build hyperlink graph: graph = list(K, V), K=title[0], V=[outlinks]
    graph = inputRDD.map(lambda line: parseLine(line))

    # compute initial pageranks
    # pageRanks = list(K, V), K=title, V=initialPageRank
    pageRanks = graph.map(lambda node: (node[0], 1/float(broadcastN.value)))

    # compute pageRank iteratively
    for iteration in range(iterations):
        # completeGraph = list(K, V), K=title, V=[[outlinks], initialPageRank]
        completeGraph = graph.join(pageRanks)

        # contributions = list(K, V), K=outlink, V=contribution
        contributions = completeGraph.flatMap(lambda token: countContributions(token[1][0], token[1][1]))

        # compute new PageRank value
        # pageRanks = list(K, V), K=outlink, V=PageRank
        pageRanks = contributions.reduceByKey(add).mapValues(lambda sum: alfa*(1/float(broadcastN.value)) + (1 - alfa)*sum)
        missingNodes = graph.map(lambda node: (node[0], alfa*(1/float(broadcastN.value)))).subtractByKey(pageRanks)
        pageRanks = pageRanks.union(missingNodes)
        pageRanks.saveAsTextFile("spark-output-" + str(iteration))

    # order nodes with descending pagerank order
    pageRanksList = pageRanks.takeOrdered(broadcastN.value, key=lambda x: -x[1])
    pageRanksOrdered = pageRanks.sortBy(lambda a: -a[1])

    # print the final page ranks
    for (link, rank) in pageRanksList:
        print("Page: %s Rank: %s" % (link, rank))

    # save ordered pagerank results as text file
    pageRanksOrdered.saveAsTextFile("spark-output-sorted")

    # stop spark context
    sc.stop()
