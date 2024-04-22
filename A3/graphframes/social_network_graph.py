# RUN COMMAND: 
# general: python social_network_graph.py graphframes-jar-path input-file-path output-file-path checkpoint-folder-path
# specific: python social_network_graph.py /opt/anaconda3/lib/python3.8/site-packages/pyspark/jars/graphframes-0.8.3-spark3.4-s_2.12.jar soc-Epinions1.txt output.txt checkpoint_graphframes

import sys
from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import desc

import findspark
findspark.init()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("""
        Usage: social_network_graph.py <graphframes-jar-path> <input-file-path> <output-file-path> <checkpoint-folder-path>
        """, file=sys.stderr)
        sys.exit(-1)

    graphframesPath = sys.argv[1]
    inputPath = sys.argv[2]
    outputPath = sys.argv[3]
    checkpointPath = sys.argv[4]

    spark = SparkSession.builder \
        .config("spark.jars", graphframesPath) \
        .getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

    sc = spark.sparkContext
    sc.setCheckpointDir(checkpointPath)
    
    rdd = sc.textFile(inputPath)
    # remove 1st 4 lines of dataset (zipWithIndex() gives each line a zero-based index)
    # then split by tab character
    rdd = rdd.zipWithIndex().filter(lambda a: a[1]>3).map(lambda a: a[0].split("\t"))
    
    # vertices must have "id" 
    # edges must have "src" and "dst"
    df = rdd.toDF(["src", "dst"])
    v = df.select("src").withColumnRenamed("src", "id").distinct()
    e = df
    g = GraphFrame(v, e)
    g.cache()

    # a.    Find the top 5 nodes with the highest outdegree and 
    #       find the count of the number of outgoing edges in each.
    outDeg = g.outDegrees
    result_a = outDeg.orderBy(desc("outDegree")).limit(5)
    result_a.toPandas().to_csv(outputPath, header=True, index=False, mode="w", lineterminator='\n')
    
    with open(outputPath, "a") as f:
        f.write('\n')
    
    # b.    Find the top 5 nodes with the highest indegree and 
    #       find the count of the number of incoming edges in each.
    inDeg = g.inDegrees
    result_b = inDeg.orderBy(desc("inDegree")).limit(5)
    result_b.toPandas().to_csv(outputPath, header=True, index=False, mode="a", lineterminator='\n')
    
    with open(outputPath, "a") as f:
        f.write('\n')

    # c.    Calculate PageRank for each of the nodes and output the top 5 nodes with the 
    #       highest PageRank values. You are free to define any suitable parameters.
    ranks = g.pageRank(resetProbability=0.15, maxIter=3)
    result_c = ranks.vertices.select("id", "pagerank").orderBy(desc("pagerank")).limit(5)
    result_c.toPandas().to_csv(outputPath, header=True, index=False, mode="a", lineterminator='\n')

    with open(outputPath, "a") as f:
        f.write('\n')

    # d.    Run the connected components algorithm on it and 
    #       find the top 5 components with the largest number of nodes.
    result_d = g.connectedComponents()
    result_d = result_d.select("id", "component").groupBy("component").count().orderBy(desc("count")).limit(5)
    result_d.toPandas().to_csv(outputPath, header=True, index=False, mode="a", lineterminator='\n')

    with open(outputPath, "a") as f:
        f.write('\n')

    # e.    Run the triangle counts algorithm on each of the vertices and 
    #       output the top 5 vertices with the largest triangle count. 
    #       In case of ties, you can randomly select the top 5 vertices.
    result_e = g.triangleCount()
    result_e = result_e.select("id", "count").orderBy(desc("count")).limit(5)
    result_e.toPandas().to_csv(outputPath, header=True, index=False, mode="a", lineterminator='\n')

