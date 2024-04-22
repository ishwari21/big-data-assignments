REQUIREMENTS

Download soc-Epinions1.txt from https://snap.stanford.edu/data/soc-Epinions1.html

-----Set JAVA_HOME-----
For example, I did the following steps:
   1. export JAVA_HOME=$(/usr/libexec/java_home)
   2. echo $JAVA_HOME
My $JAVA_HOME is set to /Library/Java/JavaVirtualMachines/jdk-13.0.2.jdk/Contents/Home

-----Install Spark and Graphframes on local machine-----
I already have Spark installed. 
After installing Spark, run the following commands on terminal:
   1. pip install -q findspark pyspark

   2. pip show pyspark
For (2), you should get something like the following:
   Name: pyspark
   Version: 3.4.1
   Summary: Apache Spark Python API
   Home-page: https://github.com/apache/spark/tree/master/python
   Author: Spark Developers
   Author-email: dev@spark.apache.org
   License: http://www.apache.org/licenses/LICENSE-2.0
   Location: /opt/anaconda3/lib/python3.8/site-packages
   Requires: py4j
   Required-by: 
The Location is important to note.

   3. pip install graphframes

-----Download Graphframes jar file----- 
Download from: https://spark-packages.org/package/graphframes/graphframes
I used "Version: 0.8.3-spark3.4-s_2.12"
The jar file version MUST match the version of Spark and Scala on local machine. Run spark-shell to see their versions. Choose the Graphframes jar version to match "Version: 0.8.3-spark<spark-version>-s_<scala_version>"

-----Copy the jar file to location of Spark jar files-----
This location is from (2) above followed by "/pyspark/jar". My location is "/opt/anaconda3/lib/python3.8/site-packages/pyspark/jar". I used the command "cp graphframes-0.8.3-spark3.4-s_2.12.jar /opt/anaconda3/lib/python3.8/site-packages/pyspark/jar"

RUN COMMAND: 
python social_network_graph.py graphframes-jar-path input-file-path output-file-path checkpoint-folder-path

The graphframes-jar-path should be given as an absolute path to the location of the Graphframes jar file. The input-file-path, output-file-path, and checkpoint-folder-path should be given as relative paths to the location of the .py file. 


For me, the command is:
python social_network_graph.py /opt/anaconda3/lib/python3.8/site-packages/pyspark/jars/graphframes-0.8.3-spark3.4-s_2.12.jar soc-Epinions1.txt output.txt checkpoint_graphframes
