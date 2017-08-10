package com.amazonaws.services.kinesis.connectors.v2.dynamodb;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.connectors.dynamodb.v2.KinesisFileInputStreamRunner;
import com.amazonaws.services.kinesis.connectors.samples.KinesisMessageModel;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsConfiguration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang.Validate;

/**
 *
 */
public class KinesisDynamoDBInjector {
    @Parameter(names = {"--config", "-c"})
    private static String config;

    @Parameter(names = {"--input-stream-file", "-i"})
    private static String entityFile;

    public static void main(String[] args) {
        KinesisDynamoDBInjector injector = new KinesisDynamoDBInjector();
        JCommander.newBuilder().addObject(injector).build().parse(args);

        Validate.notEmpty(config, "Configuration file can't be empty/null. Make sure to pass the -c option.");
        Validate.notEmpty(entityFile, "Entity file can't be empty/null. Make sure to pass the -i option.");

        KinesisConnectorsConfiguration configuration = new KinesisConnectorsConfiguration(injector.getResoucePath(config));

        AmazonKinesis client = AmazonKinesisClientBuilder.standard()
                .withCredentials(configuration.getAwsCredentialsProvider())
                .withRegion(configuration.getRegionName())
                .build();

        KinesisFileInputStreamRunner<KinesisMessageModel> runner = new KinesisFileInputStreamRunner<>(client,
                configuration, injector.getResoucePath(entityFile), true, KinesisMessageModel.class);

        Thread t = new Thread(runner);
        t.start();
    }

    private String getResoucePath(String filename) {
        return KinesisDynamoDBConnector.class.getClassLoader().getResource(filename).getPath();
    }
}
