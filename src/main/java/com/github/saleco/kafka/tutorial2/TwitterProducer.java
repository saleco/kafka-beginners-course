package com.github.saleco.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  private String consumerKey = "XXXXXXXXXXXX";
  private String consumerSecret = "XXXXXXXXXXXXXXXXXXXXXX";
  private String token = "XXXXXXXXXXXXXXXXXXXXXX";
  private String secret = "XXXXXXXXXXXXXXXXXXXXXXXXXX";

  private List<String> terms = Lists.newArrayList("kafka");

  public TwitterProducer() {

  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {

    logger.info("Setup");

    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

    //create a twitter client
    Client client = createTwitterClient(msgQueue);
    client.connect();

    //create a kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    //add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Stopping application...");
      logger.info("Shuttin down client from twitter...");
      client.stop();

      logger.info("closing producer");
      producer.close();
      logger.info("done!");

    }));

    // loop to send tweets to kafka
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e != null) {
              logger.error("Something bad happened", e);
            }
          }
        });
      }
    }
    logger.info("End of application");
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    String bootstrapServers = "127.0.0.1:9092";

    //create Producer properties
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    return producer;
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }
}
