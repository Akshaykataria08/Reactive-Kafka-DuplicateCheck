package com.cg.duplicatecheck.service;

import java.util.Collections;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cg.duplicatecheck.domain.Transaction;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

@Log4j2
@Service
public class DuplicateCheckServiceImpl implements DuplicateCheckService{

	@Autowired
	private KafkaSender<String, String> duplicateCheckResultSender;

	@Autowired
	private ReceiverOptions<String, Transaction> duplicateCheckReceiverOptions;

	private static String DUPLICATE_CHECK_ACK_TOPIC = "duplicate-check-ack-topic";
	private static String DUPLICATE_CHECK_TOPIC = "duplicate-check-topic";
	private KafkaReceiver<String, Transaction> duplicateCheckReceiver;
	
	
	@PostConstruct
	public void createKafkaReceiver() {
		this.duplicateCheckReceiver = createKafkaReceiver(DUPLICATE_CHECK_TOPIC,
				this.duplicateCheckReceiverOptions);
	}
	
	@Override
	public void start() {

		doDuplicateCheck(DUPLICATE_CHECK_ACK_TOPIC);
	}

	private void doDuplicateCheck(String senderTopic) {
		this.duplicateCheckReceiver.receive().doOnNext(msg -> {
			this.sendAck(this.duplicationChecker(msg), msg.key(), senderTopic);
			msg.receiverOffset().acknowledge();
		}).subscribe();
	}

	private KafkaReceiver<String, Transaction> createKafkaReceiver(String topic,
			ReceiverOptions<String, Transaction> receiverOptions) {
		ReceiverOptions<String, Transaction> options = receiverOptions.subscription(Collections.singleton(topic))
				.addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
				.addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

		return KafkaReceiver.create(options);
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
}
