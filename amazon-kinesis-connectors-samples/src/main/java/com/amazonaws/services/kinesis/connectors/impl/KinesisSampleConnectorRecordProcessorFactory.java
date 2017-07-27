package com.amazonaws.services.kinesis.connectors.impl;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessor;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;

/**
 *
 */
public class KinesisSampleConnectorRecordProcessorFactory<T, U> extends KinesisConnectorRecordProcessorFactory<T, U> {

    public KinesisSampleConnectorRecordProcessorFactory(final IKinesisConnectorPipeline<T, U> pipeline,
                                                        final KinesisConnectorConfiguration configuration) {
        super(pipeline, configuration);
    }

    @Override
    public KinesisConnectorRecordProcessor<T, U> createKinesisConnectorRecordProcessor(
            final IKinesisConnectorPipeline<T, U> pipeline,
            final KinesisConnectorConfiguration configuration) {
        return new KinesisSampleConnectorRecordProcessor<>(pipeline, configuration);
    }
}
