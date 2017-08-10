package com.amazonaws.services.kinesis.connectors.v2;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.sun.istack.internal.NotNull;

import lombok.Data;

/**
 *
 */
@Data
public class KinesisConnectorsRecordProcessorFactory<T, U> implements IRecordProcessorFactory {
    @NotNull
    private final KinesisConnectorsBuffer<T> buffer;

    @NotNull
    private final KinesisConnectorsEmitter<U> emitter;

    @NotNull
    private final KinesisConnectorsTransformer<T, U> transformer;

    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisConnectorsRecordsProcessor<>(buffer, emitter, transformer);
    }
}
