package kafka.tutorial1;

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

  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

  public static void main(String[] args) {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-seventh-application";
    String topic = "first_topic";

    //create Producer properties
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    //create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);


    //assign and seek are mostrly ysed to replay data or fetch a specific message

    //assign
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
    long offsetToReadFrom = 15L;

    //defining partition to read
    kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

    //defining initial offset
    kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);

    int numberOfMessagesToRead = 5;
    boolean keepOnReading = true;
    int numberOfMessagesReadSoFar = 0;


    //pool for new data
    while(keepOnReading) {

      ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

      for(ConsumerRecord record : consumerRecords) {
        numberOfMessagesReadSoFar+= 1;
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
        if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false;
          break;
        }
      }

      logger.info("Existing application");
    }



  }
}
