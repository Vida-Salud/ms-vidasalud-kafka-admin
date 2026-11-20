package cl.duoc.ms_vidasalud_kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * @param configs opcional; solo claves de {@code vidasalud.kafka-admin.configuraciones-permitidas}
 */
public record CrearTopicoRequest(

        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$",
                message = "El nombre solo admite letras, números, punto, guion y guion bajo (máximo 249 caracteres)")
        String nombre,

        @NotNull(message = "Las particiones son obligatorias")
        @Min(value = 1, message = "Debe tener al menos 1 partición")
        @Max(value = 48, message = "Máximo 48 particiones")
        Integer particiones,

        @NotNull(message = "El factor de replicación es obligatorio")
        @Min(value = 1, message = "El factor de replicación mínimo es 1")
        @Max(value = 3, message = "El clúster tiene 3 brokers: el factor de replicación máximo es 3")
        Short replicas,

        @Size(max = 20, message = "Máximo 20 configuraciones")
        Map<@NotBlank(message = "La clave no puede ser vacía") String,
            @NotBlank(message = "El valor no puede ser vacío") @Size(max = 100) String> configs
) {
}
