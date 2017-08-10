package com.amazonaws.services.kinesis.connectors.v2;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

/**
 *
 */
public class KinesisConnectorsBuffer<T> {
    @Getter
    private final long bufferByteSizeLimit;
    @Getter
    private final long bufferRecordCountLimit;
    @Getter
    private final long bufferMillisecondsLimit;
    @Getter
    private final List<T> records;
    @Getter
    private String firstSequenceNumber;
    @Getter
    private String lastSequenceNumber;

    private final AtomicLong bufferCount;

    private long previousBufferFlushTimeInMillis;

    public KinesisConnectorsBuffer(final long bufferByteSizeLimit,
                                   final long bufferRecordCountLimit,
                                   final long bufferMillisecondsLimit) {
        this(bufferByteSizeLimit, bufferRecordCountLimit, bufferMillisecondsLimit, new LinkedList<>());
    }

    public KinesisConnectorsBuffer(final long bufferByteSizeLimit,
                                   final long bufferRecordCountLimit,
                                   final long bufferMillisecondsLimit,
                                   final List<T> records) {
        this.bufferByteSizeLimit = bufferByteSizeLimit;
        this.bufferRecordCountLimit = bufferRecordCountLimit;
        this.bufferMillisecondsLimit = bufferMillisecondsLimit;
        this.records = records;
        bufferCount = new AtomicLong();
        previousBufferFlushTimeInMillis = getCurrentTimeMillis();
    }

    public void consumeRecord(T record, int recordSize, String lastSequenceNumber) {
        if (this.records.isEmpty()) {
            this.firstSequenceNumber = lastSequenceNumber;
        }
        this.lastSequenceNumber = lastSequenceNumber;
        this.records.add(record);
        bufferCount.addAndGet(recordSize);
    }

    public void clear() {
        records.clear();
        bufferCount.set(0);
        previousBufferFlushTimeInMillis = getCurrentTimeMillis();
    }

    public boolean shouldFlush() {
        long timeSinceLastFlush = getCurrentTimeMillis() - previousBufferFlushTimeInMillis;
        return (!records.isEmpty()) &&
                ((records.size() >= bufferByteSizeLimit) ||
                        (bufferCount.get() >= bufferRecordCountLimit) ||
                        (timeSinceLastFlush >= bufferMillisecondsLimit));
    }

    protected static long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }
}
