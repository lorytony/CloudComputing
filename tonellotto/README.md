# PageRank
PageRank implementation using Hadoop and Spark.

# What to run
#### Hadoop
```bash
hadoop jar pagerank-1.0-SNAPSHOT.jar it.unipi.cc.hadoop.Driver 1 0.15 Test.txt
```
#### Spark
```bash
spark-submit pagerank.py 1 0.15 Test.txt
```
