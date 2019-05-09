package com.aureum.stream.flinkfilter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"com.aureum.stream.flinkfilter.producer"})
public class FlinkFilterApplicationProducer {

	public static void main(String[] args) {
		SpringApplication.run(FlinkFilterApplicationProducer.class, args);
	}

}
