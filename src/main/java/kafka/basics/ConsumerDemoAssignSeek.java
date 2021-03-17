package kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest or latest

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to reploy data or fetch a specifc msg
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        // seek
        consumer.seek(partitionToReadFrom, offsetReadFrom);

        // poll for new data

        int numberOfmsgToRead = 5;
        boolean keepOnReading = true;
        int numOfMsgReadSoFar = 0;

        while(keepOnReading) {
            ConsumerRecords<String, String> records=
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numOfMsgReadSoFar +=1;
                logger.info("Key: "+ record.key() + ", Value: " + record.value());
                logger.info("Partition: "+ record.partition() + ", Offset: " + record.offset());
                if(numOfMsgReadSoFar >= numberOfmsgToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info(("Exiting the application"));
    }
}
