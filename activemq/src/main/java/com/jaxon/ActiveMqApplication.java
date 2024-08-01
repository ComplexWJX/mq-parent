package com.jaxon;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * @author rukawa
 * Created on 2022/11/02 17:29 by rukawa
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class ActiveMqApplication {

    public static void main(String[] args) {
        SpringApplication.run(ActiveMqApplication.class, args);
    }

}
