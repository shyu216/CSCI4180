ssh -i "awsedu.pem" ubuntu@44.207.36.163

ssh -i "awsedu.pem" ubuntu@44.193.97.216

ssh -i "awsedu.pem" ubuntu@3.220.147.146

ssh -i "awsedu.pem" hadoop@44.207.36.163

ssh -i "awsedu.pem" hadoop@44.193.97.216

ssh -i "awsedu.pem" hadoop@3.220.147.146

ssh hadoop@44.207.36.163

172.31.17.37 vm1
172.31.29.89 vm2
172.31.31.244 vm3

scp jdk-8u144-linux-x64.tar.gz hadoop@

export JAVA_HOME=/home/hadoop/jdk1.8.0_144
export PATH=$JAVA_HOME/bin:$PATH

export HADOOP_HOME=/home/hadoop/hadoop-2.7.3
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

ssh-keygen
ssh-copy-id hadoop@vm2
ssh-copy-id hadoop@vm3

cp csci4180_hadoop_conf_fully/* hadoop-2.7.3/etc/hadoop/

sudo -i
echo 3 > /proc/sys/vm/drop_caches

hdfs dfsadmin -report

hadoop com.sun.tools.javac.Main WordCount.java  
jar cf wc.jar WordCount*.class  
hadoop jar wc.jar WordCount /user/hadoop/input/kjv12 /user/hadoop/op1
