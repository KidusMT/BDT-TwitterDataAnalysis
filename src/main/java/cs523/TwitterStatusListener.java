package cs523;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class TwitterStatusListener implements StatusListener {

    private static final Logger LOGGER = Logger.getLogger(TwitterStatusListener.class.getName());

    LinkedBlockingQueue<Status> queue;

    public TwitterStatusListener(LinkedBlockingQueue<Status> queue) {
        this.queue = queue;
    }

    @Override
    public void onStatus(Status status) {
        queue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        LOGGER.info("onDeletionNotice");
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        LOGGER.info("onTrackLimitationNotice");
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        LOGGER.info("onScrubGeo");
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        LOGGER.info("onStallWarning");
    }

    @Override
    public void onException(Exception ex) {
        LOGGER.warning("Exception encountered in Twitter Status Listener: "+ ex.getMessage());
    }
}
