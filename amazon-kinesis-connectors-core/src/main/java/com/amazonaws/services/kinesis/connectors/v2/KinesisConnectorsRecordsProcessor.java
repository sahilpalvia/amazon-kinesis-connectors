package com.amazonaws.services.kinesis.connectors.v2;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class KinesisConnectorsRecordsProcessor<T, U> implements IRecordProcessor {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorsRecordsProcessor.class);

    private final KinesisConnectorsBuffer<T> buffer;

    private final KinesisConnectorsEmitter<U> emitter;

    private final KinesisConnectorsTransformer<T, U> transformer;

    private String shardId;

    private KinesisConnectorsWorkerStatus workerStatus = KinesisConnectorsWorkerStatus.NOT_SHUTDOWN;

    public KinesisConnectorsRecordsProcessor(final KinesisConnectorsBuffer<T> buffer,
                                             final KinesisConnectorsEmitter<U> emitter,
                                             final KinesisConnectorsTransformer<T, U> transformer) {
        this.buffer = buffer;
        this.emitter = emitter;
        this.transformer = transformer;
    }

    @Override
    public void initialize(final String shardId) {
        Validate.notEmpty(shardId, "ShardId can't be empty or null");
        this.shardId = shardId;
    }

    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        if (workerStatus.equals(KinesisConnectorsWorkerStatus.SHUTDOWN)) {
            LOG.warn(String.format("processRecords called on shardId: %s. Shutdown on this shard was already called", shardId));
            return;
        }

        Validate.notEmpty(shardId, "RecordProcessor not initialized!");

        records.forEach(record -> {
            try {
                final T transformedRecord = transformer.toClass(record);
                final int recordSize = record.getData().array().length;
                final String sequenceNumber = record.getSequenceNumber();

                buffer.consumeRecord(transformedRecord, recordSize, sequenceNumber);
            } catch (IOException e) {
                LOG.error("Error while transforming the records", e);
            }
        });

        if (buffer.shouldFlush()) {
            transformAndEmit(checkpointer, buffer.getRecords());
        }
    }

    protected void transformAndEmit(final IRecordProcessorCheckpointer checkpointer, final List<T> items) {
        if (CollectionUtils.isNotEmpty(items)) {
            List<U> unprocessedItemsToEmit = items.stream().map(transformer::fromClass).collect(Collectors.toList());

            for (int i = 0; i < emitter.getRetryLimit(); i++) {
                final KinesisConnectorsBuffer<U> buf = new KinesisConnectorsBuffer<>(buffer.getBufferByteSizeLimit(),
                        buffer.getBufferRecordCountLimit(), buffer.getBufferMillisecondsLimit(), unprocessedItemsToEmit);
                unprocessedItemsToEmit = emitter.emit(buf);

                if (CollectionUtils.isEmpty(unprocessedItemsToEmit)) {
                    break;
                }

                try {
                    Thread.sleep(emitter.backoffInterval);
                } catch (InterruptedException e) {
                    LOG.warn("Exception encountered while sleeping", e);
                }
            }

            final String lastSequenceNumberProcessed = buffer.getLastSequenceNumber();
            buffer.clear();

            if (lastSequenceNumberProcessed != null) {
                checkpoint(checkpointer, lastSequenceNumberProcessed);
            }

            emitter.fail(unprocessedItemsToEmit);

        } else {
            LOG.info("Not emitting any items, the list is empty or null.");
        }
    }

    @Override
    public void shutdown(final IRecordProcessorCheckpointer checkpointer, final ShutdownReason shutdownReason) {
        LOG.info(String.format("Shutdown has been called on shardId: %s, with reason %s. Initiating shutdown procedure.",
                shardId, shutdownReason));

        if (workerStatus.equals(KinesisConnectorsWorkerStatus.SHUTDOWN)) {
            LOG.warn(String.format("Record processor for shardId: %s has already been shutdown.", shardId));
            return;
        }
        try {
            switch (shutdownReason) {
                case TERMINATE:
                    transformAndEmit(checkpointer, buffer.getRecords());
                    checkpoint(checkpointer, null);
                    break;
                case ZOMBIE:
                    break;
                case REQUESTED:
                    break;
                default:
                    LOG.error(String.format("Reason: %s not known", shutdownReason));
                    throw new IllegalStateException("Illegal reason for shutdown");
            }
        } finally {
            emitter.shutdownClient();
            workerStatus = KinesisConnectorsWorkerStatus.SHUTDOWN;
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        try {
            if (StringUtils.isEmpty(sequenceNumber)) {
                checkpointer.checkpoint();
            } else {
                LOG.info(String.format("Checkpointing with sequenceNumber %s", sequenceNumber));
                checkpointer.checkpoint(sequenceNumber);
            }
        } catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException e) {
            LOG.error("Error while checkpointing", e);
        }
    }
}
