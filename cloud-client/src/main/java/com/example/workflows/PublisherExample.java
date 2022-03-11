package com.example.workflows;


import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.LinkedHashMap;
import java.util.Map;


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PublisherExample {
  public static void main(String... args) throws Exception {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "gifted-kit-342306";
    String topicId = "pushpoc2";

    publisherExample(projectId, topicId);
  }

  public static void publisherExample(String projectId, String topicId)
      throws IOException, ExecutionException, InterruptedException {
    TopicName topicName = TopicName.of(projectId, topicId);

    Publisher publisher = null;

      // Create a publisher instance with default settings bound to the topic
      publisher = Publisher.newBuilder(topicName)
              .setEndpoint("us-east1-pubsub.googleapis.com:443")
              .setEnableMessageOrdering(true)
              .build();

      // String message = "Hello World!";
    try {
      // String message = "{\r\n\"id\":\"1\",\r\n\"text\":\"helllo-1\"\r\n}";
      Map<String, String> messages = new LinkedHashMap<String, String>();
      messages.put("message1", "key7");
      messages.put("message2", "key8");
      messages.put("message3", "key9");
      messages.put("message4", "key10");

      for (Map.Entry<String, String> entry : messages.entrySet()) {
        ByteString data = ByteString.copyFromUtf8(entry.getKey());


        // ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setOrderingKey(entry.getValue())
                .setData(data)
                .putAllAttributes(ImmutableMap.of("corelationId", "message1"))
                .build();
        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        System.out.println("Published message ID: " + messageId + data);
      }} finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }
}