package io.cloudevents.pulsar.samples;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventAttributes;
import io.cloudevents.CloudEventContext;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import io.cloudevents.pulsar.CloudEventsSchema;
import org.apache.pulsar.client.api.*;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class SampleConsumer {
    public static final int MESSAGE_COUNT = 100;
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
    String subscriptionName="default";

    public SampleConsumer(){
        this.serviceURL = "pulsar://localhost:6650/";
        this.tlsAllowInsecureConnection = false;
        this.tlsEnableHostnameVerification = false;
    }

    public void consume() {
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
        ObjectMapper mapper = new ObjectMapper();
        try{
            PulsarClient client = clientBuilder.build();
            CloudEventsSchema schema = CloudEventsSchema.builder().build();
            ConsumerBuilder<CloudEvent> consumerBuilder = client.newConsumer(schema).topic(topic);
            schema.registerConsumer(consumerBuilder);
            Consumer<CloudEvent> consumer = consumerBuilder.subscriptionName(this.subscriptionName)
                .subscriptionType(SubscriptionType.Shared).replicateSubscriptionState(false).subscribe();
            System.out.println("Consumer  is " + consumer);
            int count=0;
            System.out.println("Waiting for message");
            while(count < 100) {
                Message<CloudEvent> msg = consumer.receive(5, TimeUnit.SECONDS);
                if(msg != null) {
                    System.out.println("Got message " + msg);
                    consumer.acknowledge(msg);
                    try {
                        CloudEvent event = msg.getValue();
                        String contentType = "";
                        if(event.getAttribute(CloudEventV1.DATACONTENTTYPE) != null)
                            contentType = event.getAttribute(CloudEventV1.DATACONTENTTYPE).toString();
                        if(contentType.equals("object/user"))
                        {
                            User user = PojoCloudEventDataMapper
                                .from(mapper, User.class)
                                .map(event.getData())
                                .getValue();
                            System.out.println(user);
                        }
                        System.out.println(event);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                    count++;
                }
            }
            consumer.close();
            client.close();
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Got exception "+ e);
        }
    }
    public static void main(String[] args) {
/*
        if (args.length < 2) {
            System.out.println("Usage: sample_producer <bootstrap_server> <topic>");
            return;
        }

 */
        SampleConsumer consumer = new SampleConsumer();
        consumer.consume();
    }
}
