package com.amazonaws.services.kinesis.connectors.v2.dynamodb;

import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import org.apache.commons.lang.Validate;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.dynamodb.v2.KinesisDynamoDBEmitter;
import com.amazonaws.services.kinesis.connectors.dynamodb.v2.KinesisDynamoDBTransformer;
import com.amazonaws.services.kinesis.connectors.samples.KinesisMessageModel;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsBuffer;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsConfiguration;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsExecutor;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 *
 */
public class KinesisDynamoDBConnector {
    @Parameter(names = {"--config", "-c"})
    private static String config;

    public static void main(String[] args) {
        KinesisDynamoDBConnector kinesisDynamoDBConnector = new KinesisDynamoDBConnector();
        JCommander.newBuilder().addObject(kinesisDynamoDBConnector).build().parse(args);

        Validate.notEmpty(config, "Configuration file can't be empty/null. Make sure to pass the -c option.");

        KinesisConnectorsConfiguration configuration = new KinesisConnectorsConfiguration(kinesisDynamoDBConnector.getResoucePath(config));

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(configuration.getAwsCredentialsProvider())
                .withRegion(configuration.getRegionName())
                .build();

        KinesisDynamoDBEmitter emitter = new KinesisDynamoDBEmitter(dynamoDBClient,
                configuration.getDynamoDBEndpoint(), configuration.getBackoffInterval(),
                configuration.getRetryLimit(), configuration.getDynamoDBDataTableName());

        KinesisConnectorsBuffer<KinesisMessageModel> buffer = new KinesisConnectorsBuffer<>(configuration.getBufferByteSizeLimit(),
                configuration.getBufferRecordCountLimit(), configuration.getBufferMillisecondsLimit());

        KinesisDynamoDBTransformer<KinesisMessageModel> transformer = new KinesisDynamoDBTransformer<>(KinesisMessageModel.class);

        KinesisConnectorsExecutor<KinesisMessageModel, Map<String, AttributeValue>> executor = new KinesisConnectorsExecutor<>(configuration);
        executor.initialize(buffer, emitter, transformer, null);

        Thread t = new Thread(executor);
        t.start();
    }

    private String getResoucePath(String filename) {
        return KinesisDynamoDBConnector.class.getClassLoader().getResource(filename).getPath();
    }
}
