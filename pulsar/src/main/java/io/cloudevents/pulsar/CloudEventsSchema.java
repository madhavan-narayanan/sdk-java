package io.cloudevents.pulsar;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.pulsar.impl.PulsarHeaders;
import io.cloudevents.pulsar.impl.PulsarSerializerMessageWriterImpl;
import io.netty.buffer.ByteBuf;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A schema for CloudEvent object.
 */
public class CloudEventsSchema extends AbstractSchema<CloudEvent> {
    /**
     * The configuration key for the {@link Encoding} to use when serializing the event.
     */
    public final static String ENCODING_CONFIG = "cloudevents.serializer.encoding";

    /**
     * The configuration key for the {@link EventFormat} to use when serializing the event in structured mode.
     */
    public final static String EVENT_FORMAT_CONFIG = "cloudevents.serializer.event_format";

    private Encoding encoding = Encoding.STRUCTURED;
    private EventFormat format = null;
    private ConsumerInterceptor consumerInterceptor = null;
    private ProducerInterceptor producerInterceptor = null;

    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfoImpl()
            .setName("CloudEvent")
            .setType(SchemaType.BYTES)
            .setSchema(new byte[0]);
    }

    void configure(Properties configs) {
        Object encodingConfig = configs.get(ENCODING_CONFIG);
        if (encodingConfig instanceof String) {
            this.encoding = Encoding.valueOf((String) encodingConfig);
        } else if (encodingConfig instanceof Encoding) {
            this.encoding = (Encoding) encodingConfig;
        } else if (encodingConfig != null) {
            throw new IllegalArgumentException(ENCODING_CONFIG + " can be of type String or " + Encoding.class.getCanonicalName());
        }

        if (this.encoding == Encoding.STRUCTURED) {
            Object eventFormatConfig = configs.get(EVENT_FORMAT_CONFIG);
            if (eventFormatConfig instanceof String) {
                this.format = EventFormatProvider.getInstance().resolveFormat((String) eventFormatConfig);
                if (this.format == null) {
                    throw new IllegalArgumentException(EVENT_FORMAT_CONFIG + " cannot be resolved with " + eventFormatConfig);
                }
            } else if (eventFormatConfig instanceof EventFormat) {
                this.format = (EventFormat) eventFormatConfig;
            } else {
                this.format = new JsonFormat();
                if (this.format == null) {
                    throw new IllegalArgumentException(EVENT_FORMAT_CONFIG + " cannot be resolved with " + eventFormatConfig);
                }
            }
        }
    }

    protected CloudEventsSchema(Properties props) {
        if (props != null)
            configure(props);
    }

    public void registerConsumer(ConsumerBuilder<CloudEvent> cb) {
        consumerInterceptor = new ConsumerInterceptor();
        cb.intercept(consumerInterceptor);
    }

    public void registerProducer(ProducerBuilder<CloudEvent> pb) {
        producerInterceptor = new ProducerInterceptor();
        pb.intercept(producerInterceptor);
    }
    @Override
    public byte[] encode(CloudEvent message) {
        if (encoding == Encoding.STRUCTURED) {
            return new PulsarSerializerMessageWriterImpl(producerInterceptor)
                .writeStructured(message, this.format);
        } else {
            return new PulsarSerializerMessageWriterImpl(producerInterceptor)
                .writeBinary(message);
        }
    }

    @Override
    public CloudEvent decode(byte[] bytes) {
        Map<String,String> properties = null;
        if(consumerInterceptor != null)
            properties = consumerInterceptor.properties;
        MessageReader reader = PulsarMessageFactory.createReader(properties,bytes);
        return reader.toEvent();
    }

    @Override
    public CloudEvent decode(ByteBuf byteBuf) {
        if (byteBuf == null) {
            return null;
        }
        int size = byteBuf.readableBytes();
        byte[] bytes = new byte[size];

        byteBuf.getBytes(byteBuf.readerIndex(), bytes);
        return decode(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {
        Properties props = new Properties();

        public SchemaBuilder() {
        }

        public CloudEventsSchema build() {
            return new CloudEventsSchema(props);
        }

        public SchemaBuilder structuredEncoding() {
            props.setProperty(CloudEventsSchema.ENCODING_CONFIG, Encoding.STRUCTURED.name());
            return this;
        }
        public SchemaBuilder binaryEncoding() {
            props.setProperty(CloudEventsSchema.ENCODING_CONFIG, Encoding.BINARY.name());
            return this;
        }
        public SchemaBuilder eventFormat(String eventFormat) {
            props.setProperty(CloudEventsSchema.EVENT_FORMAT_CONFIG, eventFormat);
            return this;
        }
    }

    public static class ConsumerInterceptor implements org.apache.pulsar.client.api.ConsumerInterceptor<CloudEvent> {
        Map<String,String> properties = null;
        public void close() {
        }

        public Message<CloudEvent> beforeConsume(Consumer<CloudEvent> consumer, Message<CloudEvent> message) {
            properties = message.getProperties();
            return message;
        }

        public void onAcknowledge(Consumer<CloudEvent> consumer, MessageId messageId, Throwable exception) {
        }

        public void onAcknowledgeCumulative(Consumer<CloudEvent> consumer, MessageId messageId, Throwable exception) {
        }

        public void onNegativeAcksSend(Consumer<CloudEvent> consumer, Set<MessageId> messageIds) {
        }

        public void onAckTimeoutSend(Consumer<CloudEvent> consumer, Set<MessageId> messageIds) {
        }
    }

    public static class ProducerInterceptor implements org.apache.pulsar.client.api.interceptor.ProducerInterceptor {
        Map<String, String> headersToAdd = null;
        public ProducerInterceptor(){
        }
        public void headersToAdd(Map<String,String> headers){
            this.headersToAdd = headers;
        }
        public void close() {
        }

        public boolean eligible(Message message){
            return true;
        }

        public Message beforeSend(Producer producer, Message message){
            if(this.headersToAdd != null){
                if(message instanceof MessageImpl){
                    MessageMetadata metadata = ((MessageImpl)message).getMessageBuilder();
                    this.headersToAdd.forEach((k,v) -> {
                        KeyValue keyValue = metadata.addProperty();
                        keyValue.setKey(k);
                        keyValue.setValue(v);
                    });
                }
                this.headersToAdd = null;
            }
            return message;
        }

        public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception){
        }
    }
}
