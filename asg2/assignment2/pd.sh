#!/bin/bash
hdfs dfs -rm -r op
hdfs dfs -rm -r md
hadoop com.sun.tools.javac.Main ParallelDijkstra.java PDPreProcess.java PDNodeWritable.java
jar cf pd.jar P*.class
hadoop jar pd.jar ParallelDijkstra input/pdsmall op 1 5

# hdfs dfs -cat op/part-r-00000
rm -rf ./md
hdfs dfs -get md
hdfs dfs -get op ./md

cat md/*/part-r-00000
cat ../dataset/pdsmall
