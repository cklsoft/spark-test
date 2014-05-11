#! /bin/bash
SPARK_HADOOP_VERSION=2.2.0 
SPARK_YARN=true
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0.jar
export EXEC_JAR=$SPARK_HOME/sc.jar
#examples/target/scala-2.10/spark-examples_2.10-assembly-0.9.0-incubating.jar 

./bin/spark-class org.apache.spark.deploy.yarn.Client \
 --jar $EXEC_JAR \
 --class HdfsWordCount \
 --args  yarn-standalone \
 --args hdfs://master:9101/user/root/spam.data \
 --args hdfs://master:9101/user/root/out2 \
 --num-workers 1 \
 --master-memory 512m \
 --worker-memory 512m \
 --worker-cores 1
