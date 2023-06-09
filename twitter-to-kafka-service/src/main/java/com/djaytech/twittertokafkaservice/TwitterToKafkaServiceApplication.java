package com.djaytech.twittertokafkaservice;

import com.djaytech.twittertokafkaservice.config.TwitterToKafkaServiceConfigData;
import com.djaytech.twittertokafkaservice.runner.StreamRunner;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import org.slf4j.Logger;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger LOG =  LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

	private final StreamRunner streamRunner;

	public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData, StreamRunner runner) {
		this.twitterToKafkaServiceConfigData = configData;
		this.streamRunner = runner;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("App starts...");
		LOG.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {})));
		LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
		streamRunner.start();
	}
}
