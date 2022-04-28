package io.cloudevents.pulsar.samples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.pulsar.CloudEventsSchema;
import org.apache.avro.data.Json;
import org.apache.pulsar.client.api.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SampleProducer {

    public static final int MESSAGE_COUNT = 1;
    String serviceURL = null;
    boolean tlsAllowInsecureConnection = true;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

    // for tls with keystore type config
    public boolean useKeyStoreTls = false;
    String tlsTrustStoreType = "JKS";
    String tlsTrustStorePath = null;
    String tlsTrustStorePassword = null;
    String topic="test-topic";
    PulsarClient pulsarClient;
    Producer<CloudEvent> structuredModeProducer;
    Producer<CloudEvent> binaryModeProducer;

    public SampleProducer(){
        this.serviceURL = "pulsar://localhost:6650/";
        this.tlsAllowInsecureConnection = false;
        this.tlsEnableHostnameVerification = false;
    }

    void createProducers() throws Exception{
        ClientBuilder clientBuilder = PulsarClient.builder();
        clientBuilder.allowTlsInsecureConnection(tlsAllowInsecureConnection);
        clientBuilder.enableTlsHostnameVerification(tlsEnableHostnameVerification);
        clientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
        clientBuilder.serviceUrl(serviceURL);
        clientBuilder.statsInterval(30, TimeUnit.SECONDS);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
            .tlsTrustStoreType(tlsTrustStoreType)
            .tlsTrustStorePath(tlsTrustStorePath)
            .tlsTrustStorePassword(tlsTrustStorePassword);
        pulsarClient = clientBuilder.build();

        CloudEventsSchema structuredSchema = CloudEventsSchema.builder().structuredEncoding().build();
        ProducerBuilder<CloudEvent> structuredBuilder = pulsarClient.newProducer(structuredSchema).topic(topic);
        structuredSchema.registerProducer(structuredBuilder);
        structuredModeProducer = structuredBuilder.create();

        CloudEventsSchema binarySchema = CloudEventsSchema.builder().binaryEncoding().build();
        ProducerBuilder<CloudEvent> binaryBuilder = pulsarClient.newProducer(binarySchema).topic(topic);
        binarySchema.registerProducer(binaryBuilder);
        binaryModeProducer = binaryBuilder.create();
    }
    private CloudEventData createUser(Long id) {
        User user = new User()
            .setAge(id.intValue())
            .setUsername("user" + id)
            .setFirstName("firstName" + id)
            .setLastName("lastName" + id);
        ObjectMapper mapper = new ObjectMapper();
        return PojoCloudEventData.wrap(user, mapper::writeValueAsBytes);
    }

    public void produceMessages() {
        try{
            createProducers();
            CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(URI.create("https://www.intuit.com"))
                .withType("pulsar.producer.example");

            String jsonData = "{\"msgNum\": 0,\"msgText\": \"This is test message for json event format\"}";
            JsonNode dataNode = new ObjectMapper().readTree(jsonData);
            CloudEvent event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withData("application/json", JsonCloudEventData.wrap(dataNode))
                .build();
            TypedMessageBuilder<CloudEvent> message = structuredModeProducer.newMessage();
            message.value(event).send();
            System.out.println(event);

            message = binaryModeProducer.newMessage();
            dataNode = new ObjectMapper().readTree(jsonData);
            event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withData(dataNode.toString().getBytes())
                .build();
            System.out.println(event);
            message.value(event).send();

            String plainText = "Just some plain text";
            event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withData("text/plain", plainText.getBytes())
                .build();
            message = structuredModeProducer.newMessage();
            System.out.println(event);
            message.value(event).send();

            message = binaryModeProducer.newMessage();
            event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withData(plainText.getBytes())
                .build();
            System.out.println(event);
            message.value(event).send();

            event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withData("object/user", createUser(1L))
                .build();
            message = structuredModeProducer.newMessage();
            System.out.println(event);
            message.value(event).send();
            System.exit(1);

            event = eventTemplate.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withoutData()
                .build();
            message = structuredModeProducer.newMessage();
            System.out.println(event);
            message.value(event).send();
        }catch(Exception e){
            System.out.println("Got exception "+e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void produce() {
        ClientBuilder clientBuilder = PulsarClient.builder();
        clientBuilder.allowTlsInsecureConnection(tlsAllowInsecureConnection);
        clientBuilder.enableTlsHostnameVerification(tlsEnableHostnameVerification);
        clientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
        clientBuilder.serviceUrl(serviceURL);
        clientBuilder.statsInterval(30, TimeUnit.SECONDS);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
            .tlsTrustStoreType(tlsTrustStoreType)
            .tlsTrustStorePath(tlsTrustStorePath)
            .tlsTrustStorePassword(tlsTrustStorePassword);
        try{
            PulsarClient client = clientBuilder.build();
            CloudEventsSchema schema = CloudEventsSchema.builder().structuredEncoding().build();
            ProducerBuilder<CloudEvent> producerBuilder = client.newProducer(schema).topic(topic);
            schema.registerProducer(producerBuilder);
            Producer<CloudEvent> producer = producerBuilder.create();
            // Create an event template to set basic CloudEvent attributes
            CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(URI.create("https://www.intuit.com"))
                .withType("pulsar.producer.example");

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                try {
                    String id = UUID.randomUUID().toString();
                    String data = "{\"msgIndex\": "+i+",\"msgValue\": \"Message "+i+"\"}";

                    JsonNode dataNode = new ObjectMapper().readTree(data);
                    CloudEventData myData = JsonCloudEventData.wrap(dataNode);
                    // Create the event starting from the template
                    CloudEvent event = eventTemplate.newBuilder()
                        .withId(id)
//                        .withData("text/plain", data.getBytes())
                        .withData("application/json",myData)
                        .build();
                    System.out.println("producing message " + event);
                    TypedMessageBuilder<CloudEvent> message = producer.newMessage();
                    message.value(event).send();
                } catch (Exception e) {
                    System.out.println("Error while trying to send the record");
                    e.printStackTrace();
                    return;
                }
            }
            producer.close();
            client.close();
        }catch(Exception e){

        }
    }
    public static void main(String[] args) {
/*
        if (args.length < 2) {
            System.out.println("Usage: sample_producer <bootstrap_server> <topic>");
            return;
        }

 */
        SampleProducer producer = new SampleProducer();
        producer.produceMessages();
    }
}
