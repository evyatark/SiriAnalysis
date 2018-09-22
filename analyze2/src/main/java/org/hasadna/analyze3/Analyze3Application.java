package org.hasadna.analyze3;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Analyze3Application {



	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(Analyze3Application.class, args);
		Main main3 = ctx.getBean(Main.class);
		main3.start(args);

		//main3.closeApplication();
	}
}
