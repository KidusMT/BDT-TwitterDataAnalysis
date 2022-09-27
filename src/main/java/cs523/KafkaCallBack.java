package cs523;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.logging.Logger;

public class KafkaCallBack  implements Callback {

    private static final Logger LOGGER = Logger.getLogger(KafkaCallBack.class.getName());

    private final long startTime;
    private final String key;
    private final String message;

    public KafkaCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            String formattedTweets = "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +"), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms";
            System.out.println(formattedTweets);
        } else {
            LOGGER.warning("Exception: " + exception.getMessage());
        }
    }
}