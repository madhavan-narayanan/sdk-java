package io.cloudevents.pulsar.impl;

import io.cloudevents.SpecVersion;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.pulsar.CloudEventsSchema;

public final class PulsarSerializerMessageWriterImpl extends BasePulsarMessageWriterImpl<byte[]> {

    CloudEventsSchema.ProducerInterceptor interceptor;
    public PulsarSerializerMessageWriterImpl(CloudEventsSchema.ProducerInterceptor interceptor) {
        super();
        this.interceptor = interceptor;
    }

    @Override
    public PulsarSerializerMessageWriterImpl create(SpecVersion version) {
        this.withContextAttribute(CloudEventV1.SPECVERSION, version.toString());
        return this;
    }

    @Override
    public byte[] end() {
        if(this.interceptor != null){
            this.interceptor.headersToAdd(this.contextAttributes);
        }
        return this.value;
    }

}
