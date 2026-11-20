package cl.duoc.ms_vidasalud_kafka_admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record ReiniciarOffsetsRequest(
        @NotBlank(message = "El tópico es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]{1,249}$", message = "Nombre de tópico inválido")
        String topico,

        @NotNull(message = "La posición es obligatoria")
        PosicionOffset posicion
) {
}
