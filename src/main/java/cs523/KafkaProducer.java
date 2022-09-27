package cs523;

import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static cs523.Utils.*;

public class KafkaProducer {

    public static void main(String[] args) throws InterruptedException {

        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);

        TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
        LinkedBlockingQueue<Status> tweetQueues = new LinkedBlockingQueue<>(1000);
        twitterStream.addListener(new TwitterStatusListener(tweetQueues));
        twitterStream.filter(new FilterQuery().track(TWITTER_HASH_TAGS));

        Properties kafkaProp = new Properties();
        kafkaProp.put("metadata.broker.list", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProp.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProp.put("client.id", KAFKA_CLIENT_ID);

        kafkaProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int j = 0;
        for (;;) {
            Status tweets = tweetQueues.poll();
            if (tweets == null) {
                Thread.sleep(500);
            } else {
                String msg = twitterToString(tweets);
                org.apache.kafka.clients.producer.KafkaProducer kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(kafkaProp);
                kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(j++), msg), new KafkaCallBack(System.currentTimeMillis(), String.valueOf(j++), msg));
            }
        }
    }
}