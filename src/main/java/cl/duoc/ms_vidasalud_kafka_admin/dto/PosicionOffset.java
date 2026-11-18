package cl.duoc.ms_vidasalud_kafka_admin.dto;

/** EARLIEST: reprocesa todo lo retenido. LATEST: salta lo pendiente. */
public enum PosicionOffset {
    EARLIEST,
    LATEST
}
