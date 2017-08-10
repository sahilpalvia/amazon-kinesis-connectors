package com.amazonaws.services.kinesis.connectors.elasticsearch.v2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsBuffer;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsEmitter;

/**
 *
 */
public class KinesisElasticSearchEmitter extends KinesisConnectorsEmitter<ElasticsearchObject> {
    private static final Log LOG = LogFactory.getLog(KinesisElasticSearchEmitter.class);

    private static final String CLUSTER_NAME_KEY = "cluster.name";
    private static final String CLIENT_TRANSPORT_SNIFF_KEY = "client.transport.sniff";
    private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY = "client.transport.ignore_cluster_name";
    private static final String CLIENT_TRANSPORT_PING_TIMEOUT_KEY = "client.transport.ping_timeout";
    private static final String CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY = "client.transport.nodes_sampler_interval";

    private final TransportClient client;

    private final int elasticsearchPort;

    private static final long BACKOFF_PERIOD = 10_000;

    private int numberOfSkippedRecords;

    public KinesisElasticSearchEmitter(final String serviceEndpoint,
                                       final String clusterName,
                                       final String clientTransportSniff,
                                       final String clientTransportIgnoreClusterName,
                                       final String clientTransportPingTimeout,
                                       final String clientTransportNodesSamplerInterval,
                                       final int elasticsearchPort) {
        super(serviceEndpoint, BACKOFF_PERIOD, 0);
        this.elasticsearchPort = elasticsearchPort;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put(CLUSTER_NAME_KEY, clusterName)
                .put(CLIENT_TRANSPORT_SNIFF_KEY, clientTransportSniff)
                .put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY, clientTransportIgnoreClusterName)
                .put(CLIENT_TRANSPORT_PING_TIMEOUT_KEY, clientTransportPingTimeout)
                .put(CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY, clientTransportNodesSamplerInterval)
                .build();

        this.client = new TransportClient(settings);
        LOG.info(String.format("Emitter using the elasticsearch endpoint %s:%d", serviceEndpoint, elasticsearchPort));
        client.addTransportAddress(new InetSocketTransportAddress(serviceEndpoint, elasticsearchPort));
    }

    @Override
    public List<ElasticsearchObject> emit(final KinesisConnectorsBuffer<ElasticsearchObject> buffer) {
        List<ElasticsearchObject> records = buffer.getRecords();

        if (CollectionUtils.isEmpty(records)) {
            return Collections.emptyList();
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        KinesisElasticsearchIndexRequestBuilder builder = new KinesisElasticsearchIndexRequestBuilder(client);
        List<IndexRequestBuilder> indexRequestBuilders = records.parallelStream()
                .map(builder::build)
                .collect(Collectors.toList());

        indexRequestBuilders.parallelStream().forEach(bulkRequestBuilder::add);

        while (true) {
            try {
                BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

                List<BulkItemResponse> bulkItemResponses = Arrays.asList(bulkResponse.getItems());

                List<ElasticsearchObject> failedRecords = new ArrayList<>();

                numberOfSkippedRecords = 0;

                IntStream.range(0, bulkItemResponses.size())
                        .filter(i -> bulkItemResponses.get(i).isFailed())
                        .forEach(i -> {
                            BulkItemResponse response = bulkItemResponses.get(i);
                            LOG.error(String.format("Record failed with message: %s", response.getFailureMessage()));

                            BulkItemResponse.Failure failure = response.getFailure();

                            if (failure != null) {
                                String failureMessage = failure.getMessage();
                                if (failureMessage.contains("DocumentAlreadyExistsException") || failureMessage.contains("VersionConflictEngineException")) {
                                    numberOfSkippedRecords++;
                                } else {
                                    failedRecords.add(records.get(i));
                                }
                            }
                        });

                LOG.info(String.format("Emitted: %d records to Elasticsearch", (records.size() - failedRecords.size() - numberOfSkippedRecords)));

                if (CollectionUtils.isNotEmpty(failedRecords)) {
                    printClusterStatus();
                    LOG.warn(String.format("Returning %d records as failed", failedRecords.size()));
                }

                return failedRecords;
            } catch (NoNodeAvailableException e) {
                LOG.error(String.format("No nodes found at %s:%d. Retrying in %d millis.", serviceEndpoint, elasticsearchPort, BACKOFF_PERIOD), e);
                sleep(BACKOFF_PERIOD);
            } catch (Exception e) {
                LOG.error("ElasticsearchEmitter threw an unexpected exception ", e);
                sleep(BACKOFF_PERIOD);
            }
        }
    }

    @Override
    public void shutdownClient() {
        client.close();
    }

    private final class KinesisElasticsearchIndexRequestBuilder {
        private final TransportClient transportClient;

        public KinesisElasticsearchIndexRequestBuilder(TransportClient transportClient) {
            this.transportClient = transportClient;
        }

        public IndexRequestBuilder build(final ElasticsearchObject elasticsearchObject) {
            IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(elasticsearchObject.getIndex(),
                    elasticsearchObject.getType(),
                    elasticsearchObject.getId());

            Long version = elasticsearchObject.getVersion();
            if (version != null) {
                indexRequestBuilder.setVersion(version);
            }

            Long ttl = elasticsearchObject.getTtl();
            if (ttl != null) {
                indexRequestBuilder.setTTL(ttl);
            }

            Boolean create = elasticsearchObject.getCreate();
            if (create != null) {
                indexRequestBuilder.setCreate(create);
            }

            return indexRequestBuilder;
        }
    }

    private void printClusterStatus() {
        ClusterHealthRequestBuilder builder = client.admin().cluster().prepareHealth();
        ClusterHealthResponse response = builder.execute().actionGet();
        switch (response.getStatus()) {
            case RED:
                LOG.error("Cluster health is RED. Indexing ability will be limited.");
                break;
            case YELLOW:
                LOG.error("Cluster health is YELLOW.");
                break;
            case GREEN:
                LOG.info("Cluster health is GREEN.");
        }
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
    }
}
