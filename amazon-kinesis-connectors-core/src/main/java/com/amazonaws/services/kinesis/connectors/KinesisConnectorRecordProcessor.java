/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.amazonaws.services.kinesis.connectors.impl.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This is the base class for any KinesisConnector. It is configured by a constructor that takes in
 * as parameters implementations of the IBuffer, ITransformer, and IEmitter dependencies defined in
 * a IKinesisConnectorPipeline. It is typed to match the class that records are transformed into for
 * filtering and manipulation. This class is produced by a KinesisConnectorRecordProcessorFactory.
 * <p>
 * When a Worker calls processRecords() on this class, the pipeline is used in the following way:
 * <ol>
 * <li>Records are transformed into the corresponding data model (parameter type T) via the ITransformer.</li>
 * <li>Transformed records are passed to the IBuffer.consumeRecord() method, which may optionally filter based on the
 * IFilter in the pipeline.</li>
 * <li>When the buffer is full (IBuffer.shouldFlush() returns true), records are transformed with the ITransformer to
 * the output type (parameter type U) and a call is made to IEmitter.emit(). IEmitter.emit() returning an empty list is
 * considered a success, so the record processor will checkpoint and emit will not be retried. Non-empty return values
 * will result in additional calls to emit with failed records as the unprocessed list until the retry limit is reached.
 * Upon exceeding the retry limit or an exception being thrown, the IEmitter.fail() method will be called with the
 * unprocessed records.</li>
 * <li>When the shutdown() method of this class is invoked, a call is made to the IEmitter.shutdown() method which
 * should close any existing client connections.</li>
 * </ol>
 *
 */
public abstract class KinesisConnectorRecordProcessor<T, U> implements IRecordProcessor {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorRecordProcessor.class);

    protected final IKinesisConnectorPipeline<T, U> kinesisConnectorPipeline;
    protected final KinesisConnectorConfiguration configuration;
    protected final IEmitter<U> emitter;
    protected final ITransformerBase<T, U> transformer;
    protected final IFilter<T> filter;
    protected final IBuffer<T> buffer;

    protected boolean isShutdown = false;

    public KinesisConnectorRecordProcessor(IKinesisConnectorPipeline<T, U> kinesisConnectorPipeline,
                                           KinesisConnectorConfiguration configuration) {
        Validate.notNull(kinesisConnectorPipeline, "IKinesisConnectorPipeline must not be null");
        Validate.notNull(configuration, "KinesisConnectorConfiguration must not be null");
        this.kinesisConnectorPipeline = kinesisConnectorPipeline;
        this.configuration = configuration;
        this.buffer = kinesisConnectorPipeline.getBuffer(configuration);
        this.filter = kinesisConnectorPipeline.getFilter(configuration);
        this.emitter = kinesisConnectorPipeline.getEmitter(configuration);
        this.transformer = kinesisConnectorPipeline.getTransformer(configuration);
    }

    protected int getRetryLimit() {
        return configuration.RETRY_LIMIT > 0 ? configuration.RETRY_LIMIT : 1;
    }

    protected long getBackoffInterval() {
        return configuration.BACKOFF_INTERVAL;
    }

}
