package cl.duoc.ms_vidasalud_kafka_admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MsVidasaludKafkaAdminApplication {

	public static void main(String[] args) {
		SpringApplication.run(MsVidasaludKafkaAdminApplication.class, args);
	}

}
