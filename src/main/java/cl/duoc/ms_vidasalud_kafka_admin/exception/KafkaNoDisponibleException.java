package cl.duoc.ms_vidasalud_kafka_admin.exception;

public class KafkaNoDisponibleException extends RuntimeException {

    public KafkaNoDisponibleException(String mensaje) {
        super(mensaje);
    }

    public KafkaNoDisponibleException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
