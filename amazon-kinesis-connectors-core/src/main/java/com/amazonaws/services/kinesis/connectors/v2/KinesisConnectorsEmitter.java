package com.amazonaws.services.kinesis.connectors.v2;

import lombok.Data;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonWebServiceClient;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
@Data
public abstract class KinesisConnectorsEmitter<T> {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorsEmitter.class);
    @Getter
    protected final String serviceEndpoint;
    @Getter
    protected final long backoffInterval;
    @Getter
    protected final int retryLimit;

    public abstract List<T> emit(KinesisConnectorsBuffer<T> buffer);

    public void fail(List<T> records) {
        records.forEach(record -> {
            LOG.error(String.format("Record failed: %s", record));
        });
    }

    public abstract void shutdownClient();

    public Set<T> getUniqueRecords(List<T> records) {
        return new HashSet<>(records);
    }
}
