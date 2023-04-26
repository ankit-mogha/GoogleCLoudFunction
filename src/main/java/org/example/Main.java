package org.example;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {
        String projectId = "de-da-ml";
        String topicId = "pharmacy-medication-form-data";
        BlobId blobId = BlobId.of("pharamacy-deda", "test/medication_form-0.json");

        publisherExample(projectId, topicId, blobId);
    }

    private static void publisherExample(String projectId, String topicId, BlobId blobId) throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);

        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            String message = readFromGCSBucket(blobId);
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            // Once published, returns a server-assigned message id (unique within the topic)
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            System.out.println("Published message ID: " + messageId);
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

    private static String readFromGCSBucket(BlobId blobId) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get(blobId);
        return new String(blob.getContent());
    }
}