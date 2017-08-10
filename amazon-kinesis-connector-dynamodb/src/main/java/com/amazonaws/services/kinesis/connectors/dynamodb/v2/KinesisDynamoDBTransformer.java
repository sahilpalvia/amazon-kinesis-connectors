package com.amazonaws.services.kinesis.connectors.dynamodb.v2;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.v2.KinesisConnectorsTransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 */
public class KinesisDynamoDBTransformer<T> implements KinesisConnectorsTransformer<T, Map<String, AttributeValue>> {
    private static final Log LOG = LogFactory.getLog(KinesisDynamoDBTransformer.class);
    private static final String GET_PREFIX = "get";
    private static final String IS_PREFIX = "is";

    private final Class<T> t;

    @SuppressWarnings("unchecked")
    public KinesisDynamoDBTransformer(Class<T> t) {
        this.t = t;
    }

    @Override
    public Map<String, AttributeValue> fromClass(T record) {
        Map<String, AttributeValue> map = new HashMap<>();

        for (Method method : record.getClass().getDeclaredMethods()) {
            String name = null;
            if (method.getName().startsWith(GET_PREFIX)) {
                name = method.getName().replaceFirst(GET_PREFIX, "");
            } else if (method.getName().startsWith(IS_PREFIX)) {
                name = method.getName().replaceFirst(IS_PREFIX, "");
            }

            try {
                if (StringUtils.isNotEmpty(name) && method.invoke(record) != null) {
                    map.put(name.toLowerCase(), new AttributeValue().withS(method.invoke(record).toString()));
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                LOG.error(String.format("Could not access %s to get the value", method.getName()), e);
            }
        }

        LOG.info("Returning MAP: " + map.toString());
        return map;
    }

    @Override
    public T toClass(final Record record) throws IOException {
        try {
            return new ObjectMapper().readValue(record.getData().array(), t);
        } catch (IOException e) {
            LOG.error(String.format("Error while converting recods to class %s", t));
            throw e;
        }
    }
}
