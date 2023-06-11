### Kafka AdminClient

```java
package com.djaytech.kafkaadmin.client;
```
This code is part of the `com.djaytech.kafkaadmin.client` package.

```java
import com.djaytech.appconfigdata.config.KafkaConfigData;
import com.djaytech.appconfigdata.config.RetryConfigData;
import com.djaytech.kafkaadmin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
```
These are the import statements required for various classes and interfaces used in the code.

```java
@Component
public class KafkaAdminClient {
```
This class is marked with the `@Component` annotation, indicating that it is a Spring component and can be automatically detected and instantiated by the Spring framework.

```java
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;
```
These are the member variables of the `KafkaAdminClient` class. The `Logger` object `LOG` is used for logging purposes. The other variables hold instances of various configuration data and dependencies required by the KafkaAdminClient.

```java
    public KafkaAdminClient(KafkaConfigData config,
                            RetryConfigData retryConfigData,
                            AdminClient client,
                            RetryTemplate template,
                            WebClient webClient) {
        this.kafkaConfigData = config;
        this.retryConfigData = retryConfigData;
        this.adminClient = client;
        this.retryTemplate = template;
        this.webClient = webClient;
    }
```
This is the constructor of the `KafkaAdminClient` class. It receives instances of `KafkaConfigData`, `RetryConfigData`, `AdminClient`, `RetryTemplate`, and `WebClient` as parameters and initializes the corresponding member variables.

```java
    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
            LOG.info("Create topic result {}", createTopicsResult.values().values());
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for creating Kafka topic(s)!", t);
        }
        checkTopicsCreated();
    }
```
This method is responsible for creating Kafka topics. It uses the `retryTemplate` to execute the `doCreateTopics` method, which actually performs the topic creation. If the topic creation fails after the maximum number of retries, a `KafkaClientException` is thrown. After successfully creating the topics, it calls the `checkTopicsCreated` method to verify that the topics were created.

```java
    public void checkTopicsCreated() {
        Collection<TopicListing> topics

 = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }
```
This method checks whether the topics specified in the configuration data (`kafkaConfigData`) have been created. It retrieves the current list of topics using the `getTopics` method, and then iterates over the topic names to check if each topic exists. It performs retries according to the configured retry settings (`maxRetry`, `multiplier`, `sleepTimeMs`). If the topic creation fails after reaching the maximum number of retries, a `KafkaClientException` is thrown.

```java
    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }
```
This method checks the status of the Kafka Schema Registry. It uses the `getSchemaRegistryStatus` method to make an HTTP GET request to the Schema Registry URL specified in the configuration data (`kafkaConfigData`). It performs retries according to the configured retry settings (`maxRetry`, `multiplier`, `sleepTimeMs`). If the Schema Registry is not available after reaching the maximum number of retries, a `KafkaClientException` is thrown.

```java
    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }
```
This private method sends an HTTP GET request to the Kafka Schema Registry URL using the `WebClient` instance. It retrieves the HTTP status code of the response and returns it. If an exception occurs during the request, it returns `HttpStatus.SERVICE_UNAVAILABLE`.

```java
    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping for waiting newly created topics!!");
        }
    }
```
This private method is responsible for pausing the execution of the current thread for the specified number of milliseconds (`sleepTimeMs`). If an `InterruptedException` occurs during the sleep, a `KafkaClientException` is thrown.

```java
    private void checkMaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry) {
            throw new KafkaClientException("Reached max number of retry for reading Kafka topic(s)!");
        }
    }
```
This private method checks if the maximum number of retries (`maxRetry`) has been reached. If the current retry count (`retry`) is greater than `maxRetry`, a `KafkaClientException` is thrown.

```java
    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null) {
            return false;
        }
        return topics.stream().anyMatch(topic

 -> topic.name().equals(topicName));
    }
```
This private method checks if a given topic name (`topicName`) exists in the collection of `TopicListing` objects (`topics`). It returns `true` if the topic is found, or `false` otherwise.

```java
    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating {} topic(s), attempt {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);
    }
```
This private method is responsible for actually creating the Kafka topics. It receives a `RetryContext` object, which contains information about the current retry attempt. It retrieves the topic names from the configuration data (`kafkaConfigData`), creates a list of `NewTopic` objects using the topic names, number of partitions, and replication factor specified in the configuration data, and then calls the `createTopics` method of the `AdminClient` instance (`adminClient`) to create the topics. It returns the `CreateTopicsResult` object.

```java
    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for reading Kafka topic(s)!", t);
        }
        return topics;
    }
```
This private method retrieves the current list of Kafka topics. It uses the `retryTemplate` to execute the `doGetTopics` method, which retrieves the topics using the `listTopics` method of the `AdminClient` instance (`adminClient`). If the topic retrieval fails after the maximum number of retries, a `KafkaClientException` is thrown. The method returns the collection of `TopicListing` objects.

```java
    private Collection<TopicListing> doGetTopics(RetryContext retryContext)
            throws ExecutionException, InterruptedException {
        LOG.info("Reading Kafka topic(s), attempt {}", retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null) {
            topics.forEach(topic -> LOG.debug("Topic with name {}", topic.name()));
        }
        return topics;
    }
```
This private method is responsible for actually retrieving the list of Kafka topics. It receives a `RetryContext` object, which contains information about the current retry attempt. It calls the `listTopics` method of the `AdminClient` instance (`adminClient`) to get a `ListTopicsResult`, and then retrieves the collection of `TopicListing` objects using the `listings` method. It logs the names of the retrieved topics at the debug level. The method returns the collection of `TopicListing` objects.

I hope this explanation helps you understand the functionality of the `KafkaAdminClient` class.