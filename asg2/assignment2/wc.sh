#!/bin/bash
hdfs dfs -rm -r op
hdfs dfs -rm -r md
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar W*.class
hadoop jar wc.jar WordCount input/ op 

# hdfs dfs -cat op/part-r-00000
rm -rf ./md
hdfs dfs -get md
hdfs dfs -get op ./md

cat md/*/part-r-00000
cat ../dataset/prsmall
