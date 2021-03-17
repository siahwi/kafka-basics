package kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer, key and value are object

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i <10; i++) {
            // create producer records
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", key, value);

            logger.info("Key :" +key);
            // send data  asynchronous
            // execute every time a record is successfully sent or an exception is thrown
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Receive new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .sent  to make it synchronous. not for production
            // to verify the same key go to same partition


        }
        // flush and close producer
        producer.flush();
        producer.close();

    }
}
