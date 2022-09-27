package cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static cs523.Utils.*;

public class KafkaConsumer {
    public static void main(String[] args) throws StreamingQueryException, InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("KafkaConsumer")
                .config("hive.metastore.uris", THRIFT_URL)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> ds = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC_NAME)
                .load();

        Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");
        Dataset<Row> dataset = processTwitterDataSet(lines);

        dataset.writeStream()
                .foreachBatch((rowDataset, aLong) -> rowDataset
                        .write()
                        .mode(SaveMode.Append)
                        .insertInto(TABLE_NAME))
                .option("spark.sql.streaming.checkpointLocation", WAREHOUSE_DIR)
                .start()
                .awaitTermination();
    }

//		StreamingQuery query = dataset.writeStream().outputMode("append").format("console").start();
//		query.awaitTermination();

//		dataset.coalesce(1).toDF()
//				.write()
//				.option("header", "true")
//				.csv("/home/kidusmt/Desktop/BDT-FinalProject/apache-hive-3.1.2-bin/warehouse/tweet.csv");
//				.start()
//				.awaitTermination();

//		dataset
//				.writeStream()
//				.format("csv")
//				.outputMode("append")
//				.option("path",WAREHOUSE_DIR)
//				.option("checkpointLocation", WAREHOUSE_DIR)
//				.start()
//				.awaitTermination();

}
