package com.github.saleco.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

public class ElasticSearchConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

  public static void main(String[] args) throws IOException {
    RestHighLevelClient client = createClient();

    String jsonString = "{\"foo\": \"bar\"}";
    IndexRequest indexRequest = new IndexRequest("twitter","tweets")
        .source(jsonString, XContentType.JSON);

    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

    String id = indexResponse.getId();

    logger.info(id);

    client.close();
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
}
