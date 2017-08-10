package com.amazonaws.services.kinesis.connectors.v2;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;

/**
 *
 */
@Data
public abstract class KinesisConnectorsResourceGenerator {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorsResourceGenerator.class);

    @NonNull
    protected final KinesisConnectorsConfiguration configuration;

    @NonNull
    protected final AmazonKinesis kinesisClient;

    public void generateKinesisResources() {
        createStreamIfNotCreated(configuration.getKinesisInputStream(), configuration.getKinesisInputStreamShardCount());
        createStreamIfNotCreated(configuration.getKinesisOutputStream(), configuration.getKinesisOutputStreamShardCount());
    }

    public abstract void generateOtherResources();

    protected void createStreamIfNotCreated(final String streamName, final int shardCount) {
        Validate.notEmpty(streamName, "kinesisOutputStream can't be null or empty");
        if (shardCount > 0) {
            Optional<StreamStatus> streamStatus = getStreamStatus(streamName);

            if (streamStatus.isPresent()) {
                final StreamStatus status = streamStatus.get();
                final String statusMessage = "Stream: %s is in %s status.";
                switch (status) {
                    case DELETING:
                        LOG.info(String.format(statusMessage, streamName, status));
                        waitForDeletionAndCreateStreamAgain(streamName, shardCount);
                        break;
                    case ACTIVE:
                        LOG.info(String.format(statusMessage, streamName, status));
                        // Return because the stream is already available
                        return;
                    case CREATING:
                        LOG.info(String.format(statusMessage, streamName, status));
                        break;
                    case UPDATING:
                        LOG.info(String.format(statusMessage, streamName, status));
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unknown state for a stream: %s", status));
                }
            } else {
                // Stream is not present, create stream.
                createKinesisStream(streamName, shardCount);
            }
            waitForStreamToBeCreated(streamName);

        } else {
            throw new IllegalArgumentException("kinesisOutputStreamShardCount can't be 0 or less than 0.");
        }
    }

    protected void createKinesisStream(final String streamName, final int shardCount) {
        LOG.info(String.format("Creating new Kinesis Stream with the name: %s, with %d shards", streamName, shardCount));
        CreateStreamRequest request = new CreateStreamRequest();
        request.setStreamName(streamName);
        request.setShardCount(shardCount);
        kinesisClient.createStream(request);
        LOG.info("Stream created successfully");
    }

    protected Optional<StreamStatus> getStreamStatus(final String streamName) {
        Optional<DescribeStreamResult> result = getStreamDescription(streamName);

        if (result.isPresent()) {
            return Optional.of(StreamStatus.fromValue(result.get().getStreamDescription().getStreamStatus()));
        }

        return Optional.empty();
    }

    protected Optional<DescribeStreamResult> getStreamDescription(final String streamName) {
        DescribeStreamRequest request = new DescribeStreamRequest();
        request.setStreamName(streamName);

        try {
            return Optional.of(kinesisClient.describeStream(request));
        } catch (ResourceNotFoundException e) {
            LOG.error(String.format("ResourceNotFoundException for %s stream", streamName), e);
        }

        return Optional.empty();
    }

    protected void waitForStreamToBeCreated(final String streamName) {
        long endTime = System.currentTimeMillis() + 600_000;

        while (System.currentTimeMillis() < endTime) {
            sleep(10_000);

            Optional<StreamStatus> status = getStreamStatus(streamName);
            if (status.isPresent() && status.get().equals(StreamStatus.ACTIVE)) {
                LOG.info("Stream was created and is now ACTIVE.");
                return;
            }
        }
    }

    protected void waitForDeletionAndCreateStreamAgain(final String streamName, final int shardCount) {
        Optional<StreamStatus> streamStatus = getStreamStatus(streamName);

        long endTime = System.currentTimeMillis() + 120_000;

        while (System.currentTimeMillis() < endTime) {
            if (streamStatus.isPresent()) {
                if (streamStatus.get().equals(StreamStatus.DELETING)) {
                    LOG.info(String.format("...DELETING Stream: %s ...", streamName));
                    sleep(10_000);
                }
                streamStatus = getStreamStatus(streamName);
            } else {
                break;
            }
        }

        if (!streamStatus.isPresent()) {
            createKinesisStream(streamName, shardCount);
        } else {
            throw new IllegalStateException(String.format("Stream found in an illegal state: %s", streamStatus.get()));
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.error("Error while sleeping", e);
        }
    }

}
