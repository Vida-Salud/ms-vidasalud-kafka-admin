package cl.duoc.ms_vidasalud_kafka_admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.Map;

public record ActualizarConfigRequest(
        @NotEmpty(message = "Indique al menos una configuración")
        @Size(max = 20, message = "Máximo 20 configuraciones")
        Map<@NotBlank(message = "La clave no puede ser vacía") String,
            @NotBlank(message = "El valor no puede ser vacío") @Size(max = 100) String> configs
) {
}
