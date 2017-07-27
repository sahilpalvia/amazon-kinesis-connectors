/*
 * Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import lombok.Data;
import lombok.NonNull;

/**
 * This class is used to generate KinesisConnectorRecordProcessors that operate using the user's
 * implemented classes. The createProcessor() method sets the dependencies of the
 * KinesisConnectorRecordProcessor that are specified in the KinesisConnectorPipeline argument,
 * which accesses instances of the users implementations.
 */
@Data
public abstract class KinesisConnectorRecordProcessorFactory<T, U> implements IRecordProcessorFactory {
    @NonNull
    private IKinesisConnectorPipeline<T, U> pipeline;
    @NonNull
    private KinesisConnectorConfiguration configuration;

    @Override
    public IRecordProcessor createProcessor() {
        return createKinesisConnectorRecordProcessor(pipeline, configuration);
    }

    public abstract KinesisConnectorRecordProcessor<T, U> createKinesisConnectorRecordProcessor(
            IKinesisConnectorPipeline<T, U> pipeline,
            KinesisConnectorConfiguration configuration);
}
