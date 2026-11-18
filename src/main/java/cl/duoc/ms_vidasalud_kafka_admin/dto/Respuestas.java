package cl.duoc.ms_vidasalud_kafka_admin.dto;

import java.util.List;
import java.util.Map;

/** DTOs de respuesta de la API de administración. */
public final class Respuestas {

    private Respuestas() {
    }

    public record TopicoResumen(String nombre, int particiones, int replicas, boolean protegido) {
    }

    public record ParticionInfo(int particion, Integer lider, List<Integer> replicas, List<Integer> isr) {
    }

    /** {@code configs}: solo los valores que no son el default de Kafka (fijados en el tópico o en el broker). */
    public record TopicoDetalle(String nombre, boolean protegido, List<ParticionInfo> particiones,
                                Map<String, String> configs) {
    }

    public record GrupoResumen(String grupoId, String estado, String tipo) {
    }

    public record MiembroGrupo(String consumerId, String clientId, String host, List<String> particiones) {
    }

    /** {@code lag} = offset final de la partición - offset confirmado por el grupo. */
    public record OffsetParticion(String topico, int particion, Long offsetConfirmado, Long offsetFinal, Long lag) {
    }

    public record GrupoDetalle(String grupoId, String estado, String coordinador, List<MiembroGrupo> miembros,
                               List<OffsetParticion> offsets, long lagTotal) {
    }

    public record BrokerInfo(int id, String host, int puerto) {
    }

    public record ClusterInfo(String clusterId, Integer controlador, List<BrokerInfo> brokers) {
    }
}
