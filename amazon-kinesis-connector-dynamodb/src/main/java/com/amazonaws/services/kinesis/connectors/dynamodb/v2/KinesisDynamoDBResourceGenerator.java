package com.amazonaws.services.kinesis.connectors.dynamodb.v2;

import java.util.List;
import java.util.Optional;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsResourceGenerator;

import lombok.Data;
import lombok.NonNull;

/**
 *
 */
public class KinesisDynamoDBResourceGenerator extends KinesisConnectorsResourceGenerator {
    private static final Log LOG = LogFactory.getLog(KinesisDynamoDBResourceGenerator.class);

    @NonNull
    protected final AmazonDynamoDB dynamoDBClient;

    public KinesisDynamoDBResourceGenerator(final KinesisConnectorsConfiguration configuration, final AmazonKinesis kinesisClient, final AmazonDynamoDB dynamoDBClient) {
        super(configuration, kinesisClient);
        this.dynamoDBClient = dynamoDBClient;
    }

    @Override
    public void generateOtherResources() {
        createDynamoDBTableIfMissing(configuration.getDynamoDBDataTableName(), configuration.getDynamoDBKey(),
                configuration.getDynamoDBReadCapacityUnits(), configuration.getDynamoDBWriteCapacityUnits());
    }

    protected void createDynamoDBTableIfMissing(final String dynamoDBTableName, final String dynamoDBKey, final long readCapacity, final long writeCapacity) {
        Optional<DescribeTableResult> describeTableResult = getTableDescription(dynamoDBTableName);

        if (describeTableResult.isPresent()) {
            verifyTableSchema(describeTableResult.get(), dynamoDBKey);
        }
    }

    protected Optional<DescribeTableResult> getTableDescription(@NonNull final String tableName) {
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);

        try {
            return Optional.of(dynamoDBClient.describeTable(request));
        } catch (ResourceNotFoundException e) {
            LOG.error(String.format("ResourceNotFoundException for table %s", tableName), e);
        }

        return Optional.empty();
    }

    protected Optional<TableStatus> getTableStatus(@NonNull final String tableName) {
        Optional<DescribeTableResult> describeTableResult = getTableDescription(tableName);

        if (describeTableResult.isPresent()) {
            return Optional.of(TableStatus.fromValue(describeTableResult.get().getTable().getTableStatus()));
        }

        return Optional.empty();
    }

    protected void verifyTableSchema(@NonNull final DescribeTableResult describeTableResult, @NonNull final String tableKey) {
        TableDescription tableDescription = describeTableResult.getTable();
        List<AttributeDefinition> attributeDefinitionList = tableDescription.getAttributeDefinitions();

        if (CollectionUtils.isEmpty(attributeDefinitionList)) {
            LOG.error("Table doesn't match the definition ");
        }

    }

}
