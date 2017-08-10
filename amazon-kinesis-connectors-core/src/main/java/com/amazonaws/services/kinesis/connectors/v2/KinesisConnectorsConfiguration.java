package com.amazonaws.services.kinesis.connectors.v2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lombok.Getter;
import lombok.Setter;

/**
 *
 */
public class KinesisConnectorsConfiguration {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorsConfiguration.class);
    private static final String SET_PREFIX = "set";
    private static final String GET_PREFIX = "get";
    private static final String IS_PREFIX = "is";
    public static final String KINESIS_CONNECTOR_USER_AGENT = "amazon-kinesis-connector-java-1.3.0";

    private final Properties properties;

    private final Hashtable<String, Method> namesOfSetMethods = new Hashtable<>();
    private final Hashtable<String, Method> namesOfGetOrIsMethods = new Hashtable<>();

    @Getter @Setter
    private String appName;
    @Getter @Setter
    private String connectorDestination;
    @Getter @Setter
    private int retryLimit;
    @Getter @Setter
    private long backoffInterval;
    @Getter @Setter
    private boolean batchRecordsInPutRequest;
    @Getter @Setter
    private long batchRecordSizeLimit;
    @Getter @Setter
    private long bufferRecordCountLimit;
    @Getter @Setter
    private long bufferByteSizeLimit;
    @Getter @Setter
    private long bufferMillisecondsLimit;

    @Getter @Setter
    private String kinesisEndpoint;
    @Getter @Setter
    private String kinesisInputStream;
    @Getter @Setter
    private int kinesisInputStreamShardCount;
    @Getter @Setter
    private String kinesisOutputStream;
    @Getter @Setter
    private int kinesisOutputStreamShardCount;

    @Getter @Setter
    private String workerID;
    @Getter @Setter
    private long failoverTime;
    @Getter @Setter
    private int maxRecords;
    @Getter @Setter
    private InitialPositionInStream initialPositionInStream;
    @Getter @Setter
    private long idleTimeBetweenReads;
    @Getter @Setter
    private long parentShardPollInterval;
    @Getter @Setter
    private long shardSyncInterval;
    @Getter @Setter
    private boolean callProcessRecordsEvenForEmptyList;
    @Getter @Setter
    private boolean cleanupTerminatedShardsBeforeExpiry;
    @Getter @Setter
    private String regionName;

    @Getter @Setter
    private String s3Endpoint;
    @Getter @Setter
    private String s3Bucket;

    @Getter @Setter
    private String redshiftEndpoint;
    @Getter @Setter
    private String redshiftUsername;
    @Getter @Setter
    private String redshiftPassword;
    @Getter @Setter
    private String redshiftURL;
    @Getter @Setter
    private String redshiftDataTable;
    @Getter @Setter
    private String redshiftFileTable;
    @Getter @Setter
    private String redshiftFileKeyColumn;
    @Getter @Setter
    private Character redshiftDataDelimiter;
    @Getter @Setter
    private boolean redshiftCopyMandatory;

    @Getter @Setter
    private String dynamoDBEndpoint;
    @Getter @Setter
    private String dynamoDBDataTableName;
    @Getter @Setter
    private String dynamoDBKey;
    @Getter @Setter
    private long dynamoDBReadCapacityUnits;
    @Getter @Setter
    private long dynamoDBWriteCapacityUnits;


    @Getter @Setter
    private String cloudWatchNamespace;
    @Getter @Setter
    private long cloudWatchBufferTime;
    @Getter @Setter
    private int cloudWatchMaxQueueSize;

    @Getter @Setter
    private String elasticsearchClusterName;
    @Getter @Setter
    private String elasticsearchEndpoint;
    @Getter @Setter
    private int elasticsearchPort;
    @Getter @Setter
    private boolean clientTransportSniff;
    @Getter @Setter
    private boolean clientTransportIgnoreClusterName;
    @Getter @Setter
    private String clientTransportPingTimeout;
    @Getter @Setter
    private String clientTransportNodesSamplerInterval;
    @Getter @Setter
    private String elasticsearchDefaultIndexName;
    @Getter @Setter
    private String elasticsearchDefaultTypeName;
    @Getter @Setter
    private String elasticsearchCloudFormationTemplateUrl;
    @Getter @Setter
    private String elasticsearchCloudFormationStackName;
    @Getter @Setter
    private String elasticsearchVersionNumber;
    @Getter @Setter
    private String elasticsearchCloudFormationKeyPairName;
    @Getter @Setter
    private String elasticsearchCloudFormationClusterInstanceType;
    @Getter @Setter
    private String elasticsearchCloudFormationSSHLocation;
    @Getter @Setter
    private String elasticsearchCloudFormationClusterSize;

    @Getter
    private AWSCredentialsProvider awsCredentialsProvider;

    public KinesisConnectorsConfiguration(String configFile) {
        this(configFile, new DefaultAWSCredentialsProviderChain());
    }

    public KinesisConnectorsConfiguration(String configFile, AWSCredentialsProvider awsCredentialsProvider) {
        if (StringUtils.isEmpty(configFile)) throw new IllegalArgumentException("Config can not be null, please pass the -c/--config option");
        Validate.notNull(awsCredentialsProvider, "AWSCredentialProvider can't be null");

        this.awsCredentialsProvider = awsCredentialsProvider;

        this.properties = new Properties();

        try {
            InputStream configStream = new FileInputStream(configFile);

            try {
                properties.load(configStream);
            } catch (IOException e) {
                LOG.error("Error while loading properties from the config file", e);
            } finally {
                try {
                    configStream.close();
                } catch (IOException e) {
                    LOG.error("Error while closing the config stream", e);
                }
            }
            if (!checkForEmptyKeys()) {
                throw new IllegalArgumentException("Properties can not be empty string.");
            }
        } catch (FileNotFoundException e) {
            LOG.error(String.format("Could not locate the config file %s, make sure it exists", configFile), e);
        }

        initializeNamesOfMethods();

        setKeysFromPropertyFile();

        setDefaultsForMissingProperties();
    }

    private void initializeNamesOfMethods() {
        List<Method> methods = Arrays.asList(this.getClass().getDeclaredMethods());

        methods.parallelStream()
                .filter(method -> method.getName().startsWith(SET_PREFIX))
                .forEach(method -> {
                    addMethodToTable(method, namesOfSetMethods, SET_PREFIX);
                });

        methods.parallelStream()
                .filter(method -> method.getName().startsWith(GET_PREFIX))
                .forEach(method -> {
                    addMethodToTable(method, namesOfGetOrIsMethods, GET_PREFIX);
                });

        methods.parallelStream()
                .filter(method -> method.getName().startsWith(IS_PREFIX))
                .forEach(method -> {
                    addMethodToTable(method, namesOfGetOrIsMethods, IS_PREFIX);
                });
    }

    private void addMethodToTable(Method method, Hashtable<String, Method> hashtable, String prefix) {
        final String tempPropName = method.getName().replaceFirst(prefix, "");
        final String propName = Character.toLowerCase(tempPropName.charAt(0)) + tempPropName.substring(1);

        hashtable.put(propName, method);
    }

    private boolean checkForEmptyKeys() {
        return this.properties.keySet()
                .parallelStream()
                .map(key -> (String) key)
                .filter(StringUtils::isEmpty)
                .count() <= 0;
    }

    private void setKeysFromPropertyFile() {
        properties.keySet().stream()
                .map(key -> (String) key)
                .filter(namesOfSetMethods::containsKey)
                .forEach(key -> {
                    LOG.info(String.format("Setting value for key: %s", key));
                    callMethod(namesOfSetMethods.get(key), key, (String) properties.get(key));
                });
    }

    private void setDefaultsForMissingProperties() {
        namesOfGetOrIsMethods.entrySet().stream()
                .filter(entry -> !properties.keySet().contains(entry.getKey()))
                .forEach(entry -> {
                    final String key = entry.getKey();
                    Method method = namesOfSetMethods.get(key);
                    if (method != null) {
                        Optional<String> optionalValue = KinesisConnectorsDefaultConstants.get(key);
                        String value = optionalValue.isPresent() ? optionalValue.get() : null;

                        callMethod(method, key, value);
                    }
                });
    }

    private void callMethod(Method method, String key, String value) {
        Class<?> parameter = method.getParameterTypes()[0];
        try {
            if (value == null) {
                method.invoke(this, value);
            } else {
                String trimmedValue = value.trim();
                if (parameter.equals(String.class)) {
                    method.invoke(this, trimmedValue);
                } else if (parameter.equals(int.class)) {
                    method.invoke(this, Integer.parseInt(trimmedValue));
                } else if (parameter.equals(boolean.class)) {
                    method.invoke(this, Boolean.parseBoolean(trimmedValue));
                } else if (parameter.equals(InitialPositionInStream.class)) {
                    if (trimmedValue.equals("LATEST")) {
                        method.invoke(this, InitialPositionInStream.LATEST);
                    } else if (trimmedValue.equals("TRIM_HORIZON")) {
                        method.invoke(this, InitialPositionInStream.TRIM_HORIZON);
                    } else if (trimmedValue.equals("AT_TIMESTAMP")) {
                        method.invoke(this, InitialPositionInStream.AT_TIMESTAMP);
                    }
                } else if (parameter.equals(Character.class)) {
                    method.invoke(this, trimmedValue.charAt(0));
                } else if (parameter.equals(long.class)) {
                    method.invoke(this, Long.parseLong(trimmedValue));
                } else {
                    LOG.warn(String.format("Could not find varibale %s to be set.", key));
                }
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.error(String.format("Exception thrown while setting the key %s", key), e);
        } catch (IllegalArgumentException e) {
            LOG.error(String.format("Key: %s, value: %s", key, value));
            throw e;
        }
    }
}
