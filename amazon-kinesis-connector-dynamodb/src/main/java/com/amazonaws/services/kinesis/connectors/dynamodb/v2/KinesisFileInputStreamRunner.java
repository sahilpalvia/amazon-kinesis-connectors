package com.amazonaws.services.kinesis.connectors.dynamodb.v2;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 *
 */
@Data
public class KinesisFileInputStreamRunner<T> implements Runnable {
    private static final Log LOG = LogFactory.getLog(KinesisFileInputStreamRunner.class);

    @NonNull
    protected final AmazonKinesis client;
    @NonNull
    protected final KinesisConnectorsConfiguration configuration;
    @NonNull
    protected final String fileName;
    protected final boolean loopOverFile;
    @NonNull
    protected final Class<T> t;
    protected ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run() {
        do {
            try {
                LOG.info(String.format("Loading data from file: %s", fileName));
                InputStream inputStream = new FileInputStream(fileName);

                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                long numberOfLines = 0;
                List<T> records = new ArrayList<>();

                while (StringUtils.isNotEmpty(line = reader.readLine())) {
                    if (configuration.isBatchRecordsInPutRequest()) {
                        // Batch records.
                        T record = objectMapper.readValue(line, t);
                        records.add(record);

                        if (numberOfBytesInList(records) >= configuration.getBufferByteSizeLimit()) {
                            records.remove(records.size() - 1);
                            putRecords(records);
                            LOG.info(String.format("Added %d records to the stream", records.size()));
                            records.clear();
                            records.add(record);
                        }
                    } else {
                        // Single records.
                        putRecord(line);
                        LOG.info(String.format("Added %d lines to the stream", ++numberOfLines));
                    }
                }

                if (CollectionUtils.isNotEmpty(records)) {
                    putRecords(records);
                    LOG.info(String.format("Added %d records to the stream", records.size()));
                }
            } catch (FileNotFoundException e) {
                LOG.error(String.format("Could not locate file: %s", fileName), e);
                return;
            } catch (IOException e) {
                LOG.error(String.format("Error while reading the file: %s", fileName), e);
                return;
            }
        } while (loopOverFile);
    }

    private byte[] getListInByteArray(final List<T> list) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(list);
        return bos.toByteArray();
    }

    private int numberOfBytesInList(final List<T> list) throws IOException {
        return getListInByteArray(list).length;
    }

    private void putRecords(final List<T> records) throws IOException {
        PutRecordRequest request = new PutRecordRequest();
        request.setStreamName(configuration.getKinesisInputStream());
        request.setData(ByteBuffer.wrap(getListInByteArray(records)));
        request.setPartitionKey(getPartitionKeyForRecords());
        client.putRecord(request);
    }

    private void putRecord(final String line) {
        PutRecordRequest request = new PutRecordRequest();
        request.setStreamName(configuration.getKinesisInputStream());
        request.setData(ByteBuffer.wrap(line.getBytes()));
        request.setPartitionKey(getPartitionKeyForRecords());
        client.putRecord(request);
    }

    private String getPartitionKeyForRecords() {
        return String.valueOf(UUID.randomUUID());
    }
}
