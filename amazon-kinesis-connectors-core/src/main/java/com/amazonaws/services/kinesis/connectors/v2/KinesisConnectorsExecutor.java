package com.amazonaws.services.kinesis.connectors.v2;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import lombok.NonNull;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class KinesisConnectorsExecutor<T, U> implements Runnable {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorsExecutor.class);

    private Worker worker;

    private KinesisConnectorsConfiguration kinesisConnectorsConfiguration;

    public KinesisConnectorsExecutor(KinesisConnectorsConfiguration configuration) {
        this.kinesisConnectorsConfiguration = configuration;
    }

    public void initialize(@NonNull KinesisConnectorsBuffer<T> buffer,
                           @NonNull KinesisConnectorsEmitter<U> emitter,
                           @NonNull KinesisConnectorsTransformer<T, U> transformer,
                           IMetricsFactory metricsFactory) {
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration(
                kinesisConnectorsConfiguration.getAppName(),
                kinesisConnectorsConfiguration.getKinesisInputStream(),
                kinesisConnectorsConfiguration.getAwsCredentialsProvider(),
                kinesisConnectorsConfiguration.getWorkerID())
                .withKinesisEndpoint(kinesisConnectorsConfiguration.getKinesisEndpoint())
                .withFailoverTimeMillis(kinesisConnectorsConfiguration.getFailoverTime())
                .withMaxRecords(kinesisConnectorsConfiguration.getMaxRecords())
                .withInitialPositionInStream(kinesisConnectorsConfiguration.getInitialPositionInStream())
                .withIdleTimeBetweenReadsInMillis(kinesisConnectorsConfiguration.getIdleTimeBetweenReads())
                .withCallProcessRecordsEvenForEmptyRecordList(kinesisConnectorsConfiguration.isCallProcessRecordsEvenForEmptyList())
                .withCleanupLeasesUponShardCompletion(kinesisConnectorsConfiguration.isCleanupTerminatedShardsBeforeExpiry())
                .withParentShardPollIntervalMillis(kinesisConnectorsConfiguration.getParentShardPollInterval())
                .withShardSyncIntervalMillis(kinesisConnectorsConfiguration.getShardSyncInterval())
                .withTaskBackoffTimeMillis(kinesisConnectorsConfiguration.getBackoffInterval())
                .withMetricsBufferTimeMillis(kinesisConnectorsConfiguration.getCloudWatchBufferTime())
                .withMetricsMaxQueueSize(kinesisConnectorsConfiguration.getCloudWatchMaxQueueSize())
                .withUserAgent(String.format("%s,%s,%s",
                        kinesisConnectorsConfiguration.getAppName(),
                        kinesisConnectorsConfiguration.getConnectorDestination(),
                        kinesisConnectorsConfiguration.KINESIS_CONNECTOR_USER_AGENT))
                .withRegionName(kinesisConnectorsConfiguration.getRegionName());

        if (!kinesisConnectorsConfiguration.isCallProcessRecordsEvenForEmptyList()) {
            LOG.warn("The false value of callProcessRecordsEvenForEmptyList will be ignored."
                    +" It must be set to true for the bufferTimeMillisecondsLimit to work correctly.");
        }

        if (kinesisConnectorsConfiguration.getIdleTimeBetweenReads() > kinesisConnectorsConfiguration.getBufferMillisecondsLimit()) {
            LOG.warn("idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit." +
                    " For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads ");
        }

        KinesisConnectorsRecordProcessorFactory<T, U> factory = new KinesisConnectorsRecordProcessorFactory<>(buffer, emitter, transformer);

        if (metricsFactory == null) {
            worker = new Worker(factory, configuration);
        } else {
            worker = new Worker(factory, configuration, metricsFactory);
        }
    }

    @Override
    public void run() {
        Validate.notNull(worker, "Worker can not be null, please make sure to initialize the KinesisConnectorExecutor.");
        try {
            LOG.info(String.format("Worker running in class %s", getClass().getName()));
            worker.run();
        } finally {
            LOG.info(String.format("Worker shut down in class %s", getClass().getName()));
        }
    }
}
