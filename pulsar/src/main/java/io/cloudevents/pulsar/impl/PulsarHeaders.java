package io.cloudevents.pulsar.impl;

import io.cloudevents.core.message.impl.MessageUtils;
import io.cloudevents.core.v1.CloudEventV1;

import java.util.Map;

public class PulsarHeaders {
    protected static final String CE_PREFIX = "ce_";

    public static final String CONTENT_TYPE = "content-type";

    protected static final Map<String, String> ATTRIBUTES_TO_HEADERS = MessageUtils.generateAttributesToHeadersMapping(
        v -> {
            if (v.equals(CloudEventV1.DATACONTENTTYPE)) {
                return CONTENT_TYPE;
            }
            return CE_PREFIX + v;
        });

    public static final String SPEC_VERSION = ATTRIBUTES_TO_HEADERS.get(CloudEventV1.SPECVERSION);
}
