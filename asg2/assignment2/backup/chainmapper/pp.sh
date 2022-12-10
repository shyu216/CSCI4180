#!/bin/bash
hdfs dfs -rm -r op
hadoop com.sun.tools.javac.Main  PDPreProcess.java PDNodeWritable.java
jar cf pd.jar P*.class
hadoop jar pd.jar PDPreProcess pdsmall/pdsmall op

hdfs dfs -cat op/part-r-00000