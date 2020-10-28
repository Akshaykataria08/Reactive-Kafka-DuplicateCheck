package com.cg.duplicatecheck;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.cg.duplicatecheck.deserializer.TransactionDeserializer;
import com.cg.duplicatecheck.domain.Transaction;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Log4j2
@SpringBootApplication
public class DuplicateCheckApplication {

	private KafkaSender<String, String> duplicateCheckResultSender;
	private String duplicateCheckAckTopic;

	private ReceiverOptions<String, Transaction> duplicateCheckTransactionsReceiverOptions;
	private String duplicateCheckReceiverTopic;
	
	private String bootstrapServers;
	
	public DuplicateCheckApplication() {
		super();
		this.bootstrapServers = "localhost:9092";
		this.duplicateCheckReceiverTopic = "duplicate-check-topic";
		this.duplicateCheckAckTopic = "duplicate-check-ack-topic";
		this.duplicateCheckTransactionsReceiverOptions = this.consumerProperties();
		this.duplicateCheckResultSender = this.producerProperties();
	}

	public static void main(String[] args) {
		SpringApplication.run(DuplicateCheckApplication.class, args);

		DuplicateCheckApplication obj = new DuplicateCheckApplication();
		obj.doDuplicateCheck(obj.duplicateCheckReceiverTopic);
	}

	private void doDuplicateCheck(String topic) {
		KafkaReceiver<String, Transaction> duplicateCheckReceiver = createKafkaReceiver(topic,
				this.duplicateCheckTransactionsReceiverOptions);

		duplicateCheckReceiver.receive().doOnNext(msg -> {
			this.sendAck(this.duplicationChecker(msg), msg.key(), duplicateCheckAckTopic);
			msg.receiverOffset().acknowledge();
		}).subscribe();
	}

	private void sendAck(boolean duplicationChecker, String key, String topic) {

		duplicateCheckResultSender.createOutbound()
		.send(Flux.just(new ProducerRecord<String, String>(topic, key, String.valueOf(duplicationChecker))))
		.then()
		.doOnError(e -> {
			log.error("Sending failed {}", e);
		}).doOnSuccess(s -> {
			log.info("Msg sent to Kafka successfully {}", s);
		}).subscribe();
	}

	private boolean duplicationChecker(ReceiverRecord<String, Transaction> msg) {
		System.out.println(msg.key());
		if (msg.value().getAmount() > 500) {
			return false;
		}
		return true;
	}

	private KafkaReceiver<String, Transaction> createKafkaReceiver(String topic,
			ReceiverOptions<String, Transaction> receiverOptions) {
		ReceiverOptions<String, Transaction> options = receiverOptions.subscription(Collections.singleton(topic))
				.addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
				.addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

		return KafkaReceiver.create(options);
	}

	public ReceiverOptions<String, Transaction> consumerProperties() {
		Map<String, Object> props = new HashMap<>();
		
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "duplicate-check");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "duplicate-check-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return ReceiverOptions.create(props);
	}
	
	public KafkaSender<String, String> producerProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "duplicate-check-client-id");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		SenderOptions<String, String> senderOptions = SenderOptions.create(props);
		return KafkaSender.create(senderOptions);
	}

}
