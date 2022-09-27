package cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import twitter4j.Status;

public class Utils {

    // SQL
    public static final String USE_DEFAULT_SQL = "USE default";
    public static final String LOAD_ALL_TWEETS_SQL = "SELECT * FROM tweets";
    public static final String ADVANCED_SQL = "SELECT Source, sum(default.tweets.friendscount) AS totalFriends, SUM(default.tweets.favouritescount) AS totalFavorites FROM Tweets GROUP BY Source ORDER BY sum(default.tweets.favouritescount) DESC";

    // Spark - Hive
    public static final String TABLE_NAME = "Tweets";
    public static final String WAREHOUSE_DIR = "/home/kidusmt/Desktop/BDT-FinalProject/apache-hive-3.1.2-bin/warehouse";
    public static final String THRIFT_URL = "thrift://localhost:9083";

    // Twitter
    protected static final String[] TWITTER_HASH_TAGS = {"#influencer", "#tbt", "#love", "#competition"};
    public static final String CONSUMER_KEY = "";
    public static final String CONSUMER_SECRET = "";
    public static final String ACCESS_TOKEN = "";
    public static final String ACCESS_TOKEN_SECRET = "";

    // Kafka
    public static final String TOPIC_NAME = "Tweets";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String KAFKA_CLIENT_ID = "TweetProducer";

    // Data variables
    public static final String createdAt = "createdAt";
    public static final String UsertName = "UsertName";
    public static final String ScreenName = "ScreenName";
    public static final String FollowersCount = "FollowersCount";
    public static final String FriendsCount = "FriendsCount";
    public static final String FavouritesCount = "FavouritesCount";
    public static final String Location = "Location";
    public static final String RetweetCount = "RetweetCount";
    public static final String FavoriteCount = "FavoriteCount";
    public static final String Lang = "Lang";
    public static final String Source = "Source";

    public static String getLocation(String loc) {
        if (loc == null)
            return "null";
        else return loc.split(",")[0];
    }

    public static String twitterToString(Status ret) {
        String source = ret.getSource();
        return ret.getCreatedAt() + ", " +
                ret.getUser().getName() + ", " +
                ret.getUser().getScreenName() + ", " +
                ret.getUser().getFollowersCount() + ", " +
                ret.getUser().getFriendsCount() + ", " +
                ret.getUser().getFavouritesCount() + ", " +
                getLocation(ret.getUser().getLocation()) + ", " +
                ret.getRetweetCount() + ", " +
                ret.getFavoriteCount() + ", " +
                ret.getLang() + ", " +
                source.substring((source.indexOf('>', 5) + 1), source.indexOf('<', 5));
    }

    public static Dataset<Row> processTwitterDataSet(Dataset<Row> lines) {
        Dataset<Row> schemaDS = lines
                .selectExpr("value",
                        "split(value,',')[0] as createdAt",
                        "split(value,',')[1] as UsertName",
                        "split(value,',')[2] as ScreenName",
                        "split(value,',')[3] as FollowersCount",
                        "split(value,',')[4] as FriendsCount",
                        "split(value,',')[5] as FavouritesCount",
                        "split(value,',')[6] as Location",
                        "split(value,',')[7] as RetweetCount",
                        "split(value,',')[8] as FavoriteCount",
                        "split(value,',')[9] as Lang",
                        "split(value,',')[10] as Source")
                .drop("value");

        schemaDS = schemaDS
                .withColumn(createdAt, functions.regexp_replace(functions.col(createdAt), " ", ""))
                .withColumn(UsertName, functions.regexp_replace(functions.col(UsertName), " ", ""))
                .withColumn(ScreenName, functions.regexp_replace(functions.col(ScreenName), " ", ""))
                .withColumn(FollowersCount, functions.regexp_replace(functions.col(FollowersCount), " ", ""))
                .withColumn(FriendsCount, functions.regexp_replace(functions.col(FriendsCount), " ", ""))
                .withColumn(FavouritesCount, functions.regexp_replace(functions.col(FavouritesCount), " ", ""))
                .withColumn(Location, functions.regexp_replace(functions.col(Location), " ", ""))
                .withColumn(RetweetCount, functions.regexp_replace(functions.col(RetweetCount), " ", ""))
                .withColumn(FavoriteCount, functions.regexp_replace(functions.col(FavoriteCount), " ", ""))
                .withColumn(Lang, functions.regexp_replace(functions.col(Lang), " ", ""))
                .withColumn(Source, functions.regexp_replace(functions.col(Source), " ", ""));

        schemaDS = schemaDS
                .withColumn(createdAt, functions.col(createdAt).cast(DataTypes.StringType))
                .withColumn(UsertName, functions.col(UsertName).cast(DataTypes.StringType))
                .withColumn(ScreenName, functions.col(ScreenName).cast(DataTypes.StringType))
                .withColumn(FollowersCount, functions.col(FollowersCount).cast(DataTypes.IntegerType))
                .withColumn(FriendsCount, functions.col(FriendsCount).cast(DataTypes.IntegerType))
                .withColumn(FavouritesCount, functions.col(FavouritesCount).cast(DataTypes.IntegerType))
                .withColumn(Location, functions.col(Location).cast(DataTypes.StringType))
                .withColumn(RetweetCount, functions.col(RetweetCount).cast(DataTypes.IntegerType))
                .withColumn(FavoriteCount, functions.col(FavoriteCount).cast(DataTypes.IntegerType))
                .withColumn(Lang, functions.col(Lang).cast(DataTypes.StringType))
                .withColumn(Source, functions.col(Source).cast(DataTypes.StringType));

        return schemaDS;
    }

    //	Logger.getLogger("org.apache").setLevel(Level.WARN); -- silence apache logger output and show only WARN
}
