package cl.duoc.ms_vidasalud_kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Kafka solo permite aumentar particiones: reducirlas exigiría recrear el tópico. */
public record ActualizarParticionesRequest(
        @NotNull(message = "Indique el nuevo total de particiones")
        @Min(value = 2, message = "El nuevo total debe ser mayor que el actual (mínimo 2)")
        @Max(value = 48, message = "Máximo 48 particiones")
        Integer particiones
) {
}
