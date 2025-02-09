package com.example.app;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableBatchProcessing // Añadido para trabajar con procesamiento de lotes
@ComponentScan({"com.example.config", "com.example.service", 
	"com.example.listener", "com.example.reader", "com.example.processor", "com.example.writer", "com.example.controller", "com.example.service", "com.example.listener"}) // Se escanea componentes, configuraciones y otros beans.
@EnableAsync
// @EnableScheduling
public class SpringBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchApplication.class, args);
	}

}
