package com.cg.duplicatecheck.domain;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Transaction {

	@NonNull
	private String fromAccount;
	@NonNull
	private String toAccount;
	@NonNull
	private Double amount;
	@NonNull
	private String status;
	@NonNull
	private Timestamp timestamp;
	@NonNull
	private String transactionId;
	
}
