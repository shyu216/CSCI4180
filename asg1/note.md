
# useful command

# connect
ssh -p 13036 hadoop@projgw.cse.cuhk.edu.hk  
scp -P 13036 -r kjv12 shakespeare   hadoop@projgw.cse.cuhk.edu.hk:~/

1

# set env
## .bashrc
```
export http_proxy="http://proxy.cse.cuhk.edu.hk:8000/"
export https_proxy="http://proxy.cse.cuhk.edu.hk:8000/"

export JAVA_HOME=/usr/lib/jvm/jdk8u342
export PATH=$PATH:$JAVA_HOME/bin

export HADOOP_HOME=/home/hadoop/hadoop-2.7.3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

sudo mv openlogic-openjdk-8u342-b07-linux-x64/ /usr/lib/jvm/jdk8u342  
cp csci4180_tuto2/csci4180_hadoop_conf/* hadoop-2.7.3/etc/hadoop  

# opts of hadoop
hdfs dfs -put ./shakespeare/ ./input/  


hadoop com.sun.tools.javac.Main WordCount.java  
jar cf wc.jar WordCount*.class  
hadoop jar wc.jar WordCount /user/hadoop/kjv12 /user/hadoop/op1

hadoop com.sun.tools.javac.Main NgramCount.java 
jar cf wc.jar NgramCount*.class
hadoop jar wc.jar NgramCount /user/hadoop/input/test /user/hadoop/op1 2

# scp send file
scp -P 13036 -r  hadoop@projgw.cse.cuhk.edu.hk:~/shyuWorkplace/op11 ./              

