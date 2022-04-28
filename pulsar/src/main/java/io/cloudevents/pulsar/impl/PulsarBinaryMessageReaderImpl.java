package io.cloudevents.pulsar.impl;

import io.cloudevents.SpecVersion;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.message.impl.BaseGenericBinaryMessageReaderImpl;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class PulsarBinaryMessageReaderImpl<T> extends BaseGenericBinaryMessageReaderImpl<String, byte[]> {
    Map<String,String> properties;
    public PulsarBinaryMessageReaderImpl(SpecVersion version, Map<String,String> properties, byte[] data) {
        super(version, data != null && data.length > 0 ? BytesCloudEventData.wrap(data) : null);
        Objects.requireNonNull(data);
        this.properties = properties;
    }

    @Override
    protected boolean isContentTypeHeader(String key) {
        return key.equals(PulsarHeaders.CONTENT_TYPE);
    }

    @Override
    protected boolean isCloudEventsHeader(String key) {
        return key.length() > 3 && key.substring(0, PulsarHeaders.CE_PREFIX.length()).startsWith(PulsarHeaders.CE_PREFIX);
    }

    @Override
    protected String toCloudEventsKey(String key) {
        return key.substring(PulsarHeaders.CE_PREFIX.length()).toLowerCase();
    }

    @Override
    protected void forEachHeader(BiConsumer<String, byte[]> fn) {
        this.properties.forEach((k,v) -> fn.accept(k, v.getBytes()));
    }

    @Override
    protected String toCloudEventsValue(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
