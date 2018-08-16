package org.hasadna.analyze2;

import org.hasadna.log.LogAnalyzer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Analyze2Application {

	public static void main(String[] args) {
		SpringApplication.run(Analyze2Application.class, args);
		//new LogAnalyzer().start(args);
		Main mainClass = new Main();
		mainClass.start(args);
	}
}
