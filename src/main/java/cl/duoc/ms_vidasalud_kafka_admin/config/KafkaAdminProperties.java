package cl.duoc.ms_vidasalud_kafka_admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Propiedades {@code vidasalud.kafka-admin} de application.yaml.
 *
 * @param timeout                    espera máxima de cada operación contra el clúster
 * @param protegidos                 tópicos del sistema: no se pueden eliminar desde esta API
 * @param configuracionesPermitidas  configuraciones de tópico que se pueden fijar o quitar
 */
@ConfigurationProperties("vidasalud.kafka-admin")
public record KafkaAdminProperties(Duration timeout, List<String> protegidos, List<String> configuracionesPermitidas) {
}
