/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.connectors.impl.KinesisConnectorRecordProcessorForTest;
import com.amazonaws.services.kinesis.connectors.impl.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisConnectorRecordProcessorTests {
    // Dependencies to be mocked
    @Mock
    IKinesisConnectorPipeline<Object, Object> pipeline;
    @Mock
    IEmitter<Object> emitter;
    @Mock
    ITransformer<Object, Object> transformer;
    @Mock
    IBuffer<Object> buffer;
    @Mock
    IFilter<Object> filter;
    @Mock
    IRecordProcessorCheckpointer checkpointer;

    KinesisConnectorConfiguration configuration;

    // used when generating dummy records and verifying behavior
    int DEFAULT_RECORD_BYTE_SIZE = 4;
    String DEFAULT_PARTITION_KEY = "";
    String DEFAULT_SEQUENCE_NUMBER = "";

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws InvalidStateException, ShutdownException {
        configuration = new KinesisConnectorConfiguration(new Properties(), new DefaultAWSCredentialsProviderChain());

        setupRecordProcessor(configuration);

        when(buffer.getLastSequenceNumber()).thenReturn(DEFAULT_SEQUENCE_NUMBER);
        when(buffer.shouldFlush()).thenReturn(true);

        doNothing().when(checkpointer).checkpoint(eq(DEFAULT_SEQUENCE_NUMBER));
        doNothing().when(buffer).clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPipeline() {
        new KinesisConnectorRecordProcessorForTest<>(null, configuration);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testNullConfiguration() {
        new KinesisConnectorRecordProcessorForTest<>(pipeline, null);
    }

    /**
     * Test exception thrown when calling processRecords before initialize(
     */
    @Test(expected = IllegalStateException.class)
    public void testProcessRecordsCalledBeforeInitialize() {
        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.processRecords(Collections.EMPTY_LIST, checkpointer);
    }

    /**
     * Test process records under normal conditions.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessRecords() throws ThrottlingException, ShutdownException, IOException,
            KinesisClientLibDependencyException, InvalidStateException {
        Object dummyRecord = new Object();
        int numRecords = 5;
        String shardId = "shardId";
        Object item = new Object();
        List<Object> objects = new ArrayList<>();

        objects.add(item);

        when(transformer.toClass(any(Record.class))).thenReturn(dummyRecord);
        when(filter.keepRecord(eq(dummyRecord))).thenReturn(true);
        when(buffer.getRecords()).thenReturn(objects);
        when(transformer.fromClass(item)).thenReturn(item);
        when(emitter.emit(any(UnmodifiableBuffer.class))).thenReturn(Collections.emptyList());

        doNothing().when(buffer).consumeRecord(any(), anyInt(), anyString());

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        verify(transformer, times(numRecords)).toClass(any(Record.class));
        verify(buffer, times(1)).getLastSequenceNumber();
        verify(buffer, times(1)).clear();
        verify(emitter, times(1)).emit(any(UnmodifiableBuffer.class));
        verify(checkpointer, times(1)).checkpoint(eq(DEFAULT_SEQUENCE_NUMBER));
    }

    /**
     * Test emitter throws exception upon processing.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailRecords() throws IOException, KinesisClientLibDependencyException,
            InvalidStateException, ThrottlingException, ShutdownException {
        Object dummyRecord = new Object();
        int numRecords = 5;
        String shardId = "shardId";

        when(transformer.toClass(any(Record.class))).thenReturn(dummyRecord);
        when(filter.keepRecord(dummyRecord)).thenReturn(true);
        when(buffer.getRecords()).thenReturn(Collections.emptyList());
        when(emitter.emit(any(UnmodifiableBuffer.class))).thenThrow(new IOException());

        doNothing().when(buffer).consumeRecord(any(), anyInt(), anyString());
        doNothing().when(emitter).fail(anyList());

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        verify(transformer, times(numRecords)).toClass(any(Record.class));
        verify(filter, times(numRecords)).keepRecord(eq(dummyRecord));
        verify(buffer, times(numRecords)).consumeRecord(any(), anyInt(), anyString());
        verify(emitter, times(1)).fail(anyList());
    }

    /**
      * Test process records under normal conditions but with batch processor.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessBatchedRecords() throws ThrottlingException, ShutdownException,
            IOException, KinesisClientLibDependencyException, InvalidStateException {
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> dummyCollection = new ArrayList<>();
        int numRecords = 5;
        int numTotalRecords = numRecords * 2;
        String shardId = "shardId";
        ICollectionTransformer<Object,Object> collectionTransformer = mock(ICollectionTransformer.class);

        dummyCollection.add(dummyRecord1);
        dummyCollection.add(dummyRecord2);

        when(pipeline.getTransformer(eq(configuration))).thenReturn(collectionTransformer);
        when(collectionTransformer.toClass(any(Record.class))).thenReturn(dummyCollection);
        when(filter.keepRecord(dummyRecord1)).thenReturn(true);
        when(filter.keepRecord(dummyRecord2)).thenReturn(true);
        when(buffer.getRecords()).thenReturn(Collections.emptyList());
        when(emitter.emit(any(UnmodifiableBuffer.class))).thenReturn(Collections.emptyList());

        doNothing().when(buffer).consumeRecord(any(), anyInt(), anyString());

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);

        verify(collectionTransformer, times(numRecords)).toClass(any(Record.class));
        verify(filter, times(numRecords)).keepRecord(eq(dummyRecord1));
        verify(filter, times(numRecords)).keepRecord(eq(dummyRecord2));
        verify(buffer, times(numTotalRecords)).consumeRecord(any(), anyInt(), anyString());
        verify(buffer, times(1)).getLastSequenceNumber();
        verify(buffer, times(1)).clear();
        verify(checkpointer, times(1)).checkpoint(eq(DEFAULT_SEQUENCE_NUMBER));
    }

    /**
     * Test retry logic only retries unprocessed/failed records
     */
    @Test
    public void testRetryBehavior() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> objectsAsList = new ArrayList<>();
        List<Object> singleObjectAsList = new ArrayList<>();
        String shardId = "shardId";
        Properties props = new Properties();

        objectsAsList.add(dummyRecord2);
        objectsAsList.add(dummyRecord1);
        singleObjectAsList.add(dummyRecord1);
        props.setProperty(KinesisConnectorConfiguration.PROP_BACKOFF_INTERVAL, String.valueOf(0));
        props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, String.valueOf(2));
        configuration = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain());

        setupRecordProcessor(configuration);

        when(transformer.toClass(any(Record.class))).thenReturn(dummyRecord1, dummyRecord2);
        when(filter.keepRecord(dummyRecord1)).thenReturn(true);
        when(filter.keepRecord(dummyRecord2)).thenReturn(true);
        when(buffer.getRecords()).thenReturn(objectsAsList);
        when(transformer.fromClass(dummyRecord1)).thenReturn(dummyRecord1);
        when(transformer.fromClass(dummyRecord2)).thenReturn(dummyRecord2);

        UnmodifiableBuffer<Object> unmodBuffer = new UnmodifiableBuffer<>(buffer, objectsAsList);
        when(emitter.emit(eq(unmodBuffer))).thenReturn(singleObjectAsList);

        unmodBuffer = new UnmodifiableBuffer<>(buffer, singleObjectAsList);
        when(emitter.emit(eq(unmodBuffer))).thenReturn(Collections.emptyList());

        doNothing().when(buffer).consumeRecord(eq(dummyRecord1), eq(DEFAULT_RECORD_BYTE_SIZE), eq(DEFAULT_SEQUENCE_NUMBER));
        doNothing().when(buffer).consumeRecord(eq(dummyRecord2), eq(DEFAULT_RECORD_BYTE_SIZE), eq(DEFAULT_SEQUENCE_NUMBER));

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(objectsAsList.size()), checkpointer);

        verify(buffer, times(1)).getLastSequenceNumber();
        verify(buffer, times(1)).clear();
        verify(checkpointer, times(1)).checkpoint(DEFAULT_SEQUENCE_NUMBER);
    }
    /**
     * Test fail called when all retries done.
     */
    @Test
    public void testFailAfterRetryLimitReached() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        Object dummyRecord1 = new Object();
        Object dummyRecord2= new Object();
        List<Object> objectsAsList = new ArrayList<>();
        List<Object> singleObjectAsList = new ArrayList<>();
        String shardId = "shardId";
        Properties props = new Properties();
        UnmodifiableBuffer<Object> unmodBuffer = new UnmodifiableBuffer<>(buffer, objectsAsList);

        objectsAsList.add(dummyRecord1);
        objectsAsList.add(dummyRecord2);
        singleObjectAsList.add(dummyRecord1);
        props.setProperty(KinesisConnectorConfiguration.PROP_BACKOFF_INTERVAL, String.valueOf(0));
        props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, String.valueOf(1));
        configuration = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain());

        setupRecordProcessor(configuration);

        when(transformer.toClass(any(Record.class))).thenReturn(dummyRecord1);
        when(filter.keepRecord(dummyRecord1)).thenReturn(true);
        when(transformer.toClass(any(Record.class))).thenReturn(dummyRecord2);
        when(filter.keepRecord(dummyRecord2)).thenReturn(true);
        when(buffer.getRecords()).thenReturn(objectsAsList);
        when(transformer.fromClass(dummyRecord1)).thenReturn(dummyRecord1);
        when(emitter.emit(eq(unmodBuffer))).thenReturn(singleObjectAsList);
        when(transformer.fromClass(dummyRecord2)).thenReturn(dummyRecord2);

        doNothing().when(buffer).consumeRecord(eq(dummyRecord1), eq(DEFAULT_RECORD_BYTE_SIZE), eq(DEFAULT_SEQUENCE_NUMBER));
        doNothing().when(buffer).consumeRecord(eq(dummyRecord2), eq(DEFAULT_RECORD_BYTE_SIZE), eq(DEFAULT_SEQUENCE_NUMBER));
        doNothing().when(emitter).fail(eq(singleObjectAsList));

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(objectsAsList.size()), checkpointer);

        verify(buffer, times(1)).getLastSequenceNumber();
        verify(buffer, times(1)).clear();
        verify(checkpointer, times(1)).checkpoint(eq(DEFAULT_SEQUENCE_NUMBER));
    }
    /**
     * Test process records with ITransformerBase should throw exception.
    */
    @Test(expected = RuntimeException.class)
    @SuppressWarnings("unchecked")
    public void testBadTransformer() throws ThrottlingException, ShutdownException, IOException,
    KinesisClientLibDependencyException, InvalidStateException {
        String shardId = "shardId";
        int numRecords = 5;

        ITransformerBase<Object,Object> baseTransformer = mock(ITransformerBase.class);

        when(pipeline.getTransformer(eq(configuration))).thenReturn(baseTransformer);

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.initialize(shardId);
        kcrp.processRecords(getDummyRecordList(numRecords), checkpointer);
    }

    /**
     * expect nothing to happen on ShutdownReason.ZOMBIE
     */
    @Test
    public void testShutdownZombie() {
        doNothing().when(emitter).shutdown();

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.shutdown(checkpointer, ShutdownReason.ZOMBIE);

        verify(emitter, times(1)).shutdown();
    }

    /**
     * expect buffer flush, emit and checkpoint to happen on ShutdownReason.TERMINATE
     */
    @Test
    public void testShutdownTerminate() throws IOException, KinesisClientLibDependencyException, InvalidStateException,
            ThrottlingException, ShutdownException {
        when(buffer.getRecords()).thenReturn(Collections.emptyList());
        when(emitter.emit(any(UnmodifiableBuffer.class))).thenReturn(Collections.emptyList());
        when(buffer.getLastSequenceNumber()).thenReturn(null);

        doNothing().when(checkpointer).checkpoint();
        doNothing().when(emitter).shutdown();

        KinesisConnectorRecordProcessor<Object, Object> kcrp = new KinesisConnectorRecordProcessorForTest<>(pipeline, configuration);
        kcrp.shutdown(checkpointer, ShutdownReason.TERMINATE);

        verify(buffer, times(1)).clear();
        verify(checkpointer, times(1)).checkpoint();
        verify(emitter, times(1)).shutdown();
        verify(buffer, times(1)).getLastSequenceNumber();
    }

    private List<Record> getDummyRecordList(int length) {
        ArrayList<Record> list = new ArrayList<Record>();
        for (int i = 0; i < length; i++) {
            Record dummyRecord = new Record();
            ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_RECORD_BYTE_SIZE);
            dummyRecord.setData(byteBuffer);
            dummyRecord.setPartitionKey(DEFAULT_PARTITION_KEY);
            dummyRecord.setSequenceNumber(DEFAULT_SEQUENCE_NUMBER);
            list.add(dummyRecord);
        }
        return list;
    }

    private void setupRecordProcessor(KinesisConnectorConfiguration configuration) {
        when(pipeline.getBuffer(eq(configuration))).thenReturn(buffer);
        when(pipeline.getFilter(eq(configuration))).thenReturn(filter);
        when(pipeline.getEmitter(eq(configuration))).thenReturn(emitter);
        when(pipeline.getTransformer(eq(configuration))).thenReturn(transformer);
    }

}
