#!/bin/bash
stop-dfs.sh
stop-yarn.sh
# hadoop namenode -format
start-dfs.sh
start-yarn.sh
hdfs dfsadmin -report