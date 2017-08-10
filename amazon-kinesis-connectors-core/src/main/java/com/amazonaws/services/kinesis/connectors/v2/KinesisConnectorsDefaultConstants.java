package com.amazonaws.services.kinesis.connectors.v2;

import java.rmi.dgc.VMID;
import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang.Validate;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 *
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KinesisConnectorsDefaultConstants {
    private static final HashMap<String, String> defaultTable = new HashMap<>();

    private static final String APP_NAME = "KinesisConnector";

    static {
        defaultTable.put("appName", APP_NAME);
        defaultTable.put("connectorDestination", "generic");
        defaultTable.put("retryLimit", "3");
        defaultTable.put("backoffInterval", Long.toString(10_000L));
        defaultTable.put("batchRecordsInPutRequest", "false");
        defaultTable.put("batchRecordSizeLimit", "50000");
        defaultTable.put("bufferRecordCountLimit", Long.toString(1000L));
        defaultTable.put("bufferByteSizeLimit", Long.toString(1024 * 1024));
        defaultTable.put("bufferMillisecondsLimit", Long.toString(Long.MAX_VALUE));

        // Default Amazon Kinesis Constants
        defaultTable.put("kinesisEndpoint", null);
        defaultTable.put("kinesisInputStream", "kinesisInputStream");
        defaultTable.put("kinesisInputStreamShardCount", "1");
        defaultTable.put("kinesisOutputStream", "kinesisOutputStream");
        defaultTable.put("kinesisOutputStreamShardCount", "1");

        // Default Amazon Kinesis Client Library Constants
        defaultTable.put("workerID", new VMID().toString());
        defaultTable.put("failoverTime", Long.toString(30_000L));
        defaultTable.put("maxRecords", Long.toString(10_000));
        defaultTable.put("initialPositionInStream", "TRIM_HORIZON");
        defaultTable.put("idleTimeBetweenReads", Long.toString(1000));
        defaultTable.put("parentShardPollInterval", Long.toString(10_000));
        defaultTable.put("shardSyncInterval", Long.toString(60_000));

        // CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST must be set to true for bufferMillisecondsLimit to work
        defaultTable.put("callProcessRecordsEvenForEmptyList", "true");
        defaultTable.put("cleanupTerminatedShardsBeforeExpiry", "false");
        defaultTable.put("regionName", "us-east-1");

        // Default Amazon S3 Constants
        defaultTable.put("s3Endpoint", "https://s3.amazonaws.com");
        defaultTable.put("s3Bucket", "kinesis-bucket");

        // Default Amazon Redshift Constants
        defaultTable.put("redshiftEndpoint", "https://redshift.us-east-1.amazonaws.com");
        defaultTable.put("redshiftUsername", null);
        defaultTable.put("redshiftPassword", null);
        defaultTable.put("redshiftURL", null);
        defaultTable.put("redshiftDataTable", "users");
        defaultTable.put("redshiftFileTable", "files");
        defaultTable.put("redshiftFileKeyColumn", "file");
        defaultTable.put("redshiftDataDelimiter", "|");
        defaultTable.put("redshiftCopyMandatory", "true");

        // Default Amazon DynamoDB Constants
        defaultTable.put("dynamoDBEndpoint", "dynamodb.us-east-1.amazonaws.com");
        defaultTable.put("dynamoDBDataTableName", "dynamodb_emitter_test");
        defaultTable.put("dynamoDBWriteCapacityUnits", "10");
        defaultTable.put("dynamoDBReadCapacityUnits", "10");

        // Default Amazon CloudWatch Constants
        defaultTable.put("cloudWatchNamespace", APP_NAME);
        defaultTable.put("cloudWatchBufferTime", Long.toString(10_000));
        defaultTable.put("cloudWatchMaxQueueSize", Long.toString(10_000));

        // Default Amazon Elasticsearch Constraints
        defaultTable.put("elasticsearchClusterName", "elasticsearch");
        defaultTable.put("elasticsearchEndpoint", "localhost");
        defaultTable.put("elasticsearchPort", Long.toString(9300));
        defaultTable.put("clientTransportSniff", "false");
        defaultTable.put("clientTransportIgnoreClusterName", "false");
        defaultTable.put("clientTransportPingTimeout", "5s");
        defaultTable.put("clientTransportNodesSamplerInterval", "5s");
        defaultTable.put("elasticsearchDefaultIndexName", "index");
        defaultTable.put("elasticsearchDefaultTypeName", "type");
        defaultTable.put("elasticsearchCloudFormationTemplateUrl", "Elasticsearch.template");
        defaultTable.put("elasticsearchCloudFormationStackName", "kinesisElasticsearchSample");
        defaultTable.put("elasticsearchVersionNumber", "1.2.1");
        defaultTable.put("elasticsearchCloudFormationKeyPairName", "");
        defaultTable.put("elasticsearchCloudFormationClusterInstanceType", "m1.small");
        defaultTable.put("elasticsearchCloudFormationSSHLocation", "0.0.0.0/0");
        defaultTable.put("elasticsearchCloudFormationClusterSize", "3");
    }

    public static Optional<String> get(String key) {
        Validate.notEmpty(key, "Key can't be empty or null");
        return Optional.ofNullable(defaultTable.get(key));
    }
}
