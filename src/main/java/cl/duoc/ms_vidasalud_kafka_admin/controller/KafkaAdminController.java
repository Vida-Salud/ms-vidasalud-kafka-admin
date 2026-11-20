package cl.duoc.ms_vidasalud_kafka_admin.controller;

import cl.duoc.ms_vidasalud_kafka_admin.dto.ActualizarConfigRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.ActualizarParticionesRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.ReiniciarOffsetsRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.ClusterInfo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoResumen;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.OffsetParticion;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoResumen;
import cl.duoc.ms_vidasalud_kafka_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API REST para administrar Kafka (solo Admin). Valida el formato de la petición y delega en
 * {@link KafkaAdminService}; no conoce el AdminClient.
 * <p>
 * Las restricciones sobre {@code @PathVariable} las aplica la validación de métodos integrada en Spring MVC
 * (sin {@code @Validated}): un nombre inválido responde 400 con {@code HandlerMethodValidationException}.
 */
@RestController
@RequestMapping("/api/{version}/kafka")
public class KafkaAdminController {

    private static final String NOMBRE = "^[a-zA-Z0-9._-]{1,249}$";
    private static final String NOMBRE_INVALIDO = "Nombre inválido: solo letras, números, punto, guion y guion bajo";

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() {
        return service.cluster();
    }

    @GetMapping("/topics")
    public List<TopicoResumen> listarTopicos() {
        return service.listarTopicos();
    }

    @GetMapping("/topics/{name}")
    public TopicoDetalle detalleTopico(@PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String name) {
        return service.detalleTopico(name);
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicoDetalle> crearTopico(@Valid @RequestBody CrearTopicoRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopico(request));
    }

    /** Fija o cambia configuraciones (retention.ms, cleanup.policy, ...). Las demás no se tocan. */
    @PutMapping("/topics/{name}/configs")
    public TopicoDetalle actualizarConfiguracion(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String name,
            @Valid @RequestBody ActualizarConfigRequest request) {
        return service.actualizarConfiguracion(name, request.configs());
    }

    /** Quita la configuración del tópico: vuelve a regir el valor del broker. */
    @DeleteMapping("/topics/{name}/configs/{key}")
    public TopicoDetalle eliminarConfiguracion(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String name,
            @PathVariable String key) {
        return service.eliminarConfiguracion(name, key);
    }

    @PutMapping("/topics/{name}/partitions")
    public TopicoDetalle aumentarParticiones(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String name,
            @Valid @RequestBody ActualizarParticionesRequest request) {
        return service.aumentarParticiones(name, request.particiones());
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Void> eliminarTopico(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String name) {
        service.eliminarTopico(name);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/consumer-groups")
    public List<GrupoResumen> listarGrupos() {
        return service.listarGrupos();
    }

    /** Miembros, particiones asignadas, offsets confirmados y lag del grupo. */
    @GetMapping("/consumer-groups/{groupId}")
    public GrupoDetalle detalleGrupo(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String groupId) {
        return service.detalleGrupo(groupId);
    }

    /** Reprocesar (EARLIEST) o saltar lo pendiente (LATEST). El grupo debe estar sin consumidores activos. */
    @PostMapping("/consumer-groups/{groupId}/offsets/reset")
    public List<OffsetParticion> reiniciarOffsets(
            @PathVariable @Pattern(regexp = NOMBRE, message = NOMBRE_INVALIDO) String groupId,
            @Valid @RequestBody ReiniciarOffsetsRequest request) {
        return service.reiniciarOffsets(groupId, request.topico(), request.posicion());
    }
}
