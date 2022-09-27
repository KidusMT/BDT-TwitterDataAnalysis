package cs523;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cs523.Utils.*;

public class SparkSQLHive {
    public static void main(String[] args) {

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", THRIFT_URL);

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName("SparkSQLHive")
                .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql(USE_DEFAULT_SQL);

        Dataset<Row> rowDataset = sparkSession.sql(LOAD_ALL_TWEETS_SQL);
        rowDataset.show();

        rowDataset = sparkSession.sql(ADVANCED_SQL);
        rowDataset.show();
    }

}
