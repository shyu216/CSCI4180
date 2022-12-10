#!/bin/bash
hdfs dfs -rm -r op
hdfs dfs -rm -r md
hadoop com.sun.tools.javac.Main PageRank.java PRPreProcess.java PRNodeWritable.java
jar cf pr.jar P*.class
hadoop jar pr.jar PageRank 5 0.1 input/prsmall op 

# hdfs dfs -cat op/part-r-00000
rm -rf ./md
hdfs dfs -get md
hdfs dfs -get op ./md

cat md/*/part-r-00000
cat ../dataset/prsmall
