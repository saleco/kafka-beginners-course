package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {

      String bootstrapServers = "127.0.0.1:9092";

      //create Producer properties
      Properties properties = new Properties();

      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      //create the producer
      KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

      for (int i = 0; i < 10000; i++) {

        String topic = "first_topic";
        String value = "hello world " + Integer.toString(i);
        String key = "id_" + Integer.toString(i);
        //create a producer record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

        logger.info("Key: " + key);
        //in every execution the partitions will be the same (using same key)
        //id_0 is going to partition 1
        //id_1 partition 0
        //id_2 partition 2

        //send data -asynchronous
        producer.send(record, new Callback() {
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            //executes every time record is successfully sent or an exception is thrown
            if(e == null) {

              logger.info("Received new metadata. \n " +
                  "Topic: " + recordMetadata.topic() + "\n" +
                  "Partition: " + recordMetadata.partition() + "\n" +
                  "Offset: " + recordMetadata.offset() + "\n" +
                  "Timestamp: " + recordMetadata.timestamp() + "\n");

            } else {
              logger.error("Error while producing", e);
            }

          }
        }).get(); //block the .send() to make it synchronous - dont do this in production!
      }


      producer.flush();
      producer.close();
    }
  }

