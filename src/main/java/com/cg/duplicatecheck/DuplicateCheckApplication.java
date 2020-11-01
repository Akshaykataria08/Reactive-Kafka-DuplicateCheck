package com.cg.duplicatecheck;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.cg.duplicatecheck.service.DuplicateCheckService;

@SpringBootApplication
public class DuplicateCheckApplication {

	@Autowired
	private DuplicateCheckService duplicateCheckService;

	public static void main(String[] args) {
		SpringApplication.run(DuplicateCheckApplication.class, args);
	}
	
	@PostConstruct
	public void ListenForMessages() {
		duplicateCheckService.start();
	}

}
