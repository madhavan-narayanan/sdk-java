package io.cloudevents.pulsar.impl;

import io.cloudevents.CloudEventData;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.message.MessageWriter;
import io.cloudevents.rw.CloudEventRWException;
import io.cloudevents.rw.CloudEventWriter;

import java.util.HashMap;
import java.util.Map;

abstract class BasePulsarMessageWriterImpl<R> implements MessageWriter<CloudEventWriter<R>, R>, CloudEventWriter<R> {
    byte[] value;
    Map<String, String> contextAttributes = new HashMap<>();

    public BasePulsarMessageWriterImpl()
    {
    }

    @Override
    public BasePulsarMessageWriterImpl<R> withContextAttribute(String name, String value) throws CloudEventRWException {
        String headerName = PulsarHeaders.ATTRIBUTES_TO_HEADERS.get(name);
        if (headerName == null) {
            headerName = PulsarHeaders.CE_PREFIX + name;
        }
        contextAttributes.put(headerName,value);
        return this;
    }

    @Override
    public R end(CloudEventData value) throws CloudEventRWException {
        this.value = value.toBytes();
        return this.end();
    }

    @Override
    public R setEvent(EventFormat format, byte[] value) throws CloudEventRWException {
        this.contextAttributes.put(PulsarHeaders.CONTENT_TYPE, format.serializedContentType());
        this.value = value;
        return this.end();
    }
}
