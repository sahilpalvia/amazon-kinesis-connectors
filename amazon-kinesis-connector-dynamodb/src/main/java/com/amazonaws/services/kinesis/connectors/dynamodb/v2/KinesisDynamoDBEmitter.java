package com.amazonaws.services.kinesis.connectors.dynamodb.v2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsBuffer;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsEmitter;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class KinesisDynamoDBEmitter extends KinesisConnectorsEmitter<Map<String, AttributeValue>> {
    private static final Log LOG = LogFactory.getLog(KinesisDynamoDBEmitter.class);

    @Getter
    private final AmazonDynamoDB client;
    private final String dynamoDBTableName;

    protected int batchWriteRecordsSize = 16;

    public KinesisDynamoDBEmitter(final AmazonDynamoDB client, final String serviceEndpoint, final long backoffInterval, final int retryLimit, final String dynamoDBTableName) {
        super(serviceEndpoint, backoffInterval, retryLimit);
        this.client = client;
        this.dynamoDBTableName = dynamoDBTableName;
    }

    @Override
    public List<Map<String, AttributeValue>> emit(final KinesisConnectorsBuffer<Map<String, AttributeValue>> buffer) {
        Map<WriteRequest, Map<String, AttributeValue>> writeRequestMap = new HashMap<>();
        List<Map<String, AttributeValue>> unprocessedRecords = new ArrayList<>();

        Set<Map<String, AttributeValue>> uniqueItems = getUniqueRecords(buffer.getRecords());

        uniqueItems.forEach(item -> {
            writeRequestMap.put(new WriteRequest().withPutRequest(new PutRequest().withItem(item)), item);

            if (writeRequestMap.size() == batchWriteRecordsSize) {
                LOG.info("Batch records size reached, will write records to DynamoDB.");
                unprocessedRecords.addAll(batchWriteRecords(writeRequestMap));
                writeRequestMap.clear();
            }
        });
        unprocessedRecords.addAll(batchWriteRecords(writeRequestMap));

        LOG.info(String.format("Successfully emitted %d records into DynamoDB", buffer.getRecords().size() - unprocessedRecords.size()));

        return unprocessedRecords;
    }

    private List<Map<String, AttributeValue>> batchWriteRecords(Map<WriteRequest, Map<String, AttributeValue>> writeRequestMap) {
        List<WriteRequest> writeRequests = new ArrayList<>(writeRequestMap.keySet());

        if (CollectionUtils.isEmpty(writeRequests)) {
            return Collections.emptyList();
        }

        Map<String, List<WriteRequest>> requestMap = new HashMap<>();
        requestMap.put(dynamoDBTableName, writeRequests);

        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest().withRequestItems(requestMap);

        BatchWriteItemResult result = client.batchWriteItem(batchWriteItemRequest);
        Collection<List<WriteRequest>> unprocessedWriteRequests = result.getUnprocessedItems().values();

        List<Map<String, AttributeValue>> unprocessedList = new ArrayList<>();


        unprocessedWriteRequests.parallelStream()
                .forEach(unprocessedWrite -> {
                    unprocessedList.addAll(unprocessedWrite.parallelStream()
                            .map(writeRequestMap::get).collect(Collectors.toCollection(ArrayList::new)));
                });

        LOG.info(String.format("Number of unprocessed DynamoDB records %d", unprocessedList.size()));

        return unprocessedList;
    }

    @Override
    public void shutdownClient() {
        client.shutdown();
    }
}
