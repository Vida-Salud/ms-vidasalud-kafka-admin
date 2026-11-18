package cl.duoc.ms_vidasalud_kafka_admin.controller;

import cl.duoc.ms_vidasalud_kafka_admin.exception.ConflictoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.DatoInvalidoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.KafkaNoDisponibleException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.RecursoNoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.exc.InvalidFormatException;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mismo formato de error (ProblemDetail, RFC 9457) que el resto de la API v1. Va antes del handler de
 * Spring Boot para poder agregar "errores" por campo.
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice
public class ManejadorErrores {

    private static final Logger log = LoggerFactory.getLogger(ManejadorErrores.class);

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ProblemDetail noEncontrado(RecursoNoEncontradoException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ConflictoException.class)
    public ProblemDetail conflicto(ConflictoException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(DatoInvalidoException.class)
    public ProblemDetail datoInvalido(DatoInvalidoException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(KafkaNoDisponibleException.class)
    public ProblemDetail kafkaNoDisponible(KafkaNoDisponibleException e) {
        log.error("Kafka no disponible: {}", e.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail validacion(MethodArgumentNotValidException e) {
        ProblemDetail problema = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "La petición tiene campos inválidos");
        Map<String, String> errores = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(error -> errores.putIfAbsent(error.getField(), error.getDefaultMessage()));
        problema.setProperty("errores", errores);
        return problema;
    }

    /** Parámetros de ruta inválidos (por ejemplo, un nombre de tópico con espacios). */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public ProblemDetail validacionParametros(HandlerMethodValidationException e) {
        String detalle = e.getAllErrors().stream()
                .map(error -> error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detalle);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail cuerpoIlegible(HttpMessageNotReadableException e) {
        if (e.getCause() instanceof InvalidFormatException invalido
                && invalido.getTargetType() != null
                && invalido.getTargetType().isEnum()
                && !invalido.getPath().isEmpty()) {
            ProblemDetail problema = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                    "La petición tiene campos inválidos");
            problema.setProperty("errores", Map.of(nombreCampo(invalido.getPath()),
                    "Valor inválido. Valores permitidos: " + Arrays.stream(invalido.getTargetType().getEnumConstants())
                            .map(Object::toString).collect(Collectors.joining(", "))));
            return problema;
        }
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "El cuerpo de la petición no es un JSON válido");
    }

    private static String nombreCampo(List<JacksonException.Reference> ruta) {
        return ruta.get(ruta.size() - 1).getPropertyName();
    }
}
