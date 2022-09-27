# BDT-TwitterDataAnalysis
Big Data Technology: Using `Apache Kafka`, `Apache Zookeeper`, `Apache Spark`, `Apache Hive`, `Apache Hadoop` to process Twitter Data

```shell
### Order of Execusion
1. zookeeper
pwd: /home/kafka/kafka
sh: sudo ./bin/zookeeper-server-start.sh config/zookeeper.properties
sh: sudo lsof -i :2181

2. kafka
pwd: /home/kafka/kafka
sh: ./bin/kafka-server-start.sh config/server.properties
sh: ./bin/kafka-console-consumer.sh --topic Tweets --from-beginning --bootstrap-server localhost:9092 --property "print.value=true"
sh: ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic Tweets --describe

3. hadoop
pwd: /home/hadoop/hadoop/sbin
sh: ./start-all.sh
sh: ./stop-all.sh
sh: jps (active hadoop process)
sh: ./hadoop fs -ls -R / | grep "^d" (hdfs directories)
sh: ./hdfs dfs -ls hdfs:/ (hdfs filels)
sh: hadoop fs -chmod g+w /tmp/hive/kidusmt(permission)

4. spark
pwd: /home/kidusmt/Desktop/BDT-FinalProject/spark-3.3.0-bin-hadoop3
sh: ./sbin/start-all.sh
sh: ./sbin/stop-all.sh

5. hive
pwd: /home/kidusmt/Desktop/BDT-FinalProject/apache-hive-3.1.2-bin/bin
sh: hive --service metastore & (thrift port opener)
sh: schematool --dbType derby --initSchema --verbose
sh: hive
sh: hiveserver2
sh: sudo docker run -it -p 8888:8888 gethue/hue:latest(hue)
sh: netstat -anp | grep 10000 (troubleshoot network port)

6. derby
pwd: /home/kidusmt/Desktop/BDT-FinalProject/derby/bin
sh: startNetworkServer (starts thrift server port for connection)
```
