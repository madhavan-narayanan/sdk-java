/*
 * Copyright 2018-Present The CloudEvents Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.cloudevents.pulsar;

import io.cloudevents.SpecVersion;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.message.MessageWriter;
import io.cloudevents.core.message.impl.GenericStructuredMessageReader;
import io.cloudevents.core.message.impl.MessageUtils;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.pulsar.impl.PulsarBinaryMessageReaderImpl;
import io.cloudevents.pulsar.impl.PulsarHeaders;
import io.cloudevents.rw.CloudEventRWException;
import io.cloudevents.rw.CloudEventWriter;
import org.apache.pulsar.client.api.Message;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;

/**
 * This class provides a collection of methods to create {@link io.cloudevents.core.message.MessageReader}
 * and {@link io.cloudevents.core.message.MessageWriter} for Pulsar Producer and Consumer.
 * <p>
 */
@ParametersAreNonnullByDefault
public final class PulsarMessageFactory {

    private PulsarMessageFactory() {
    }

    /**
     * Create a {@link io.cloudevents.core.message.MessageReader} to read {@link Message}.
     *
     * @param data the record to convert to {@link io.cloudevents.core.message.MessageReader}
     * @param <T>    the type of the message data
     * @return the new {@link io.cloudevents.core.message.MessageReader}
     * @throws CloudEventRWException if something goes wrong while resolving the {@link SpecVersion} or if the message has unknown encoding
     */
    public static <T> MessageReader createReader(Map<String,String> properties, byte[] data) throws CloudEventRWException {
        return MessageUtils.parseStructuredOrBinaryMessage(
            () -> properties.get(PulsarHeaders.CONTENT_TYPE),
            format -> new GenericStructuredMessageReader(format, data),
            () -> properties.get(PulsarHeaders.SPEC_VERSION),
            sv -> new PulsarBinaryMessageReaderImpl(sv,properties,data)
        );
    }

    public static <T> MessageWriter<CloudEventWriter<Message<T>>, Message<T>> createWriter(String topic) {
        return null;
    }
}
