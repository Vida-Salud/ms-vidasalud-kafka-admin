package cl.duoc.ms_vidasalud_kafka_admin.config;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Cliente de administración de Kafka (AdminClient). Reutiliza la configuración que Spring Boot ya armó
 * en {@link KafkaAdmin} a partir de {@code spring.kafka} (brokers, client-id y {@code admin.properties}).
 * Es perezoso: se conecta en la primera operación, no al arrancar.
 */
@Configuration
public class ClienteKafkaConfig {

    @Bean(destroyMethod = "close")
    @Lazy
    public Admin kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
