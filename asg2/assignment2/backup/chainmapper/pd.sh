#!/bin/bash
hdfs dfs -rm -r op
hadoop com.sun.tools.javac.Main ParallelDijkstra.java PDPreProcess.java PDNodeWritable.java
jar cf pd.jar P*.class
hadoop jar pd.jar ParallelDijkstra pdsmall/pdsmall op 1 4

hdfs dfs -cat op/part-r-00000