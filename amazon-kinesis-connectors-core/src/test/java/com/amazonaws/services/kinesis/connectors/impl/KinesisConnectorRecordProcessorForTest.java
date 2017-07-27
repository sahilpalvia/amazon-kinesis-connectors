package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessor;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class KinesisConnectorRecordProcessorForTest<T, U> extends KinesisConnectorRecordProcessor<T, U> {
    private final Log LOG = LogFactory.getLog(KinesisConnectorRecordProcessorForTest.class);
    private String shardId;

    public KinesisConnectorRecordProcessorForTest(final IKinesisConnectorPipeline kinesisConnectorPipeline,
                                                  final KinesisConnectorConfiguration configuration) {
        super(kinesisConnectorPipeline, configuration);
    }

    @Override
    public void initialize(final String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        if (isShutdown) {
            LOG.warn("processRecords called on shutdown record processor for shardId: " + shardId);
            return;
        }

        if (StringUtils.isEmpty(shardId)) {
            throw new IllegalStateException("RecordProcessor not initialized!");
        }

        // Transform each Amazon Kinesis Record and add the result to the buffer
        for (Record record : records) {
            try {
                if (transformer instanceof ITransformer) {
                    ITransformer<T, U> singleTransformer = (ITransformer<T, U>) transformer;
                    filterAndBufferRecord(singleTransformer.toClass(record), record);
                } else if (transformer instanceof ICollectionTransformer) {
                    ICollectionTransformer<T, U> listTransformer = (ICollectionTransformer<T, U>) transformer;
                    Collection<T> transformedRecords = listTransformer.toClass(record);
                    for (T transformedRecord : transformedRecords) {
                        filterAndBufferRecord(transformedRecord, record);
                    }
                } else {
                    throw new RuntimeException("Transformer must implement ITransformer or ICollectionTransformer");
                }
            } catch (IOException e) {
                LOG.error(e);
            }
        }

        if (buffer.shouldFlush()) {
            List<U> emitItems = transformToOutput(buffer.getRecords());
            emit(checkpointer, emitItems);
        }
    }

    private void filterAndBufferRecord(T transformedRecord, Record record) {
        if (filter.keepRecord(transformedRecord)) {
            buffer.consumeRecord(transformedRecord, record.getData().array().length, record.getSequenceNumber());
        }
    }

    private List<U> transformToOutput(List<T> items) {
        List<U> emitItems = new ArrayList<U>();
        for (T item : items) {
            try {
                emitItems.add(transformer.fromClass(item));
            } catch (IOException e) {
                LOG.error("Failed to transform record " + item + " to output type", e);
            }
        }
        return emitItems;
    }

    private void emit(IRecordProcessorCheckpointer checkpointer, List<U> emitItems) {
        List<U> unprocessed = new ArrayList<U>(emitItems);
        try {
            for (int numTries = 0; numTries < getRetryLimit(); numTries++) {
                unprocessed = emitter.emit(new UnmodifiableBuffer<U>(buffer, unprocessed));
                if (unprocessed.isEmpty()) {
                    break;
                }
                try {
                    Thread.sleep(getBackoffInterval());
                } catch (InterruptedException e) {
                }
            }
            if (!unprocessed.isEmpty()) {
                emitter.fail(unprocessed);
            }
            final String lastSequenceNumberProcessed = buffer.getLastSequenceNumber();
            buffer.clear();
            // checkpoint once all the records have been consumed
            if (lastSequenceNumberProcessed != null) {
                checkpointer.checkpoint(lastSequenceNumberProcessed);
            }
        } catch (IOException | KinesisClientLibDependencyException | InvalidStateException | ThrottlingException
                | ShutdownException e) {
            LOG.error(e);
            emitter.fail(unprocessed);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor with shardId: " + shardId + " with reason " + reason);
        if (isShutdown) {
            LOG.warn("Record processor for shardId: " + shardId + " has been shutdown multiple times.");
            return;
        }
        switch (reason) {
            case TERMINATE:
                emit(checkpointer, transformToOutput(buffer.getRecords()));
                try {
                    checkpointer.checkpoint();
                } catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException e) {
                    LOG.error(e);
                }
                break;
            case ZOMBIE:
                break;
            default:
                throw new IllegalStateException("invalid shutdown reason");
        }
        emitter.shutdown();
        isShutdown = true;
    }
}
