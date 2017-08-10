package com.amazonaws.services.kinesis.connectors.v2;

import com.amazonaws.services.kinesis.model.Record;

import java.io.IOException;

/**
 *
 */
public interface KinesisConnectorsTransformer<T, U> {
    U fromClass(T record);

    T toClass(Record record) throws IOException;
}
