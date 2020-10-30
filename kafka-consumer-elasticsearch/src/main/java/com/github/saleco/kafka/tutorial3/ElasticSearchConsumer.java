package com.github.saleco.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

  private static JsonParser jsonParser = new JsonParser();

  public static void main(String[] args) throws IOException {
    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    //pool for new data
    while(true) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

      logger.info("Received " + consumerRecords.count() + " records");

      for(ConsumerRecord<String, String> record : consumerRecords) {
        //2 strategies to create ID
           // kafka generic ID
          //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

        //twitter feed specific id
        String id = extractIdFromTweet(record.value());

        //insert data into elastic search
        IndexRequest indexRequest = new IndexRequest(
            "twitter",
            "tweets",
                 id) //id to make consumer idempotent
            .source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        logger.info(indexResponse.getId());

        try {
          Thread.sleep(1000); //introduce a small delay
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      logger.info("Comitting the offsets...");
      consumer.commitSync();
      logger.info("Offsets have been commited");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }

//    client.close();
  }


  private static String extractIdFromTweet(String tweetJson) {
    //gson library to get id from tweet
    return jsonParser.parse(tweetJson)
      .getAsJsonObject().get("id_str")
      .getAsString();
  }

  public static RestHighLevelClient createClient() {

    String hostname = "kafka-course-4340324096.eu-central-1.bonsaisearch.net";
    String username = "szotsd0yym";
    String password = "vg1tzn30e";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    HttpHost[] hosts;
    RestClientBuilder builder = RestClient.builder(
        new HttpHost(hostname, 443, "https")
    ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-demo-elasticsearch";

    //create Producer properties
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offsets
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); //setup max records for each fetch

    //create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
    kafkaConsumer.subscribe(Arrays.asList(topic));
    return kafkaConsumer;

  }
}
