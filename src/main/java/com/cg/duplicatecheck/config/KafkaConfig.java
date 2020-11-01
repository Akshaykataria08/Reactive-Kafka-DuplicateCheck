package com.cg.duplicatecheck.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.cg.duplicatecheck.deserializer.TransactionDeserializer;
import com.cg.duplicatecheck.domain.Transaction;

import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaConfig {

	private String bootstrapServers = "localhost:9092";
	private static String CLIENT_ID = "duplicate-check-client-id";
	private static String CLIENT_GROUP = "duplicate-check-client-group";
	
	@Bean
	public KafkaSender<String, String> duplicateCheckResultSender() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		SenderOptions<String, String> senderOptions = SenderOptions.create(props);
		return KafkaSender.create(senderOptions);
	}

	@Bean
	public ReceiverOptions<String, Transaction> duplicateCheckTransactionsReceiverOptions() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_GROUP);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return ReceiverOptions.create(props);
	}
}
