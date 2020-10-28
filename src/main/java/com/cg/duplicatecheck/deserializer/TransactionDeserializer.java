package com.cg.duplicatecheck.deserializer;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;

import com.cg.duplicatecheck.domain.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class TransactionDeserializer implements Deserializer<Transaction>{

	@Override
	public Transaction deserialize(String topic, byte[] data) {

		ObjectMapper mapper = new ObjectMapper();
		Transaction transaction = null;
		
		try {
			transaction = mapper.readValue(data, Transaction.class);
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		return transaction;
	}

}
