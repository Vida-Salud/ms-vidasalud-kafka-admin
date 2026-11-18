package cl.duoc.ms_vidasalud_kafka_admin.service;

import cl.duoc.ms_vidasalud_kafka_admin.config.KafkaAdminProperties;
import cl.duoc.ms_vidasalud_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.PosicionOffset;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.BrokerInfo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.ClusterInfo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoResumen;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.MiembroGrupo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.OffsetParticion;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.ParticionInfo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoResumen;
import cl.duoc.ms_vidasalud_kafka_admin.exception.ConflictoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.DatoInvalidoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.KafkaNoDisponibleException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Toda la administración de Kafka pasa por aquí: el controlador solo llama métodos de alto nivel y no
 * conoce el AdminClient. Cada operación espera como máximo {@code vidasalud.kafka-admin.timeout} y
 * traduce los errores de Kafka a excepciones de la API (404, 409, 400, 503).
 */
@Service
public class KafkaAdminServiceImpl implements KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminServiceImpl.class);
    private static final Set<String> CLEANUP_POLICIES = Set.of("delete", "compact", "compact,delete", "delete,compact");
    private static final String MIN_ISR = "min.insync.replicas";
    private static final String CLEANUP_POLICY = "cleanup.policy";

    private final Admin admin;
    private final KafkaAdminProperties properties;

    public KafkaAdminServiceImpl(Admin admin, KafkaAdminProperties properties) {
        this.admin = admin;
        this.properties = properties;
    }

    // ===================== Clúster =====================

    @Override
    public ClusterInfo cluster() {
        DescribeClusterResult resultado = admin.describeCluster();
        Collection<Node> nodos = esperar(resultado.nodes(), "describir el clúster");
        Node controlador = esperar(resultado.controller(), "describir el clúster");
        return new ClusterInfo(
                esperar(resultado.clusterId(), "describir el clúster"),
                controlador != null ? controlador.id() : null,
                nodos.stream()
                        .map(n -> new BrokerInfo(n.id(), n.host(), n.port()))
                        .sorted(Comparator.comparingInt(BrokerInfo::id))
                        .toList());
    }

    // ===================== Tópicos =====================

    @Override
    public List<TopicoResumen> listarTopicos() {
        Set<String> nombres = esperar(admin.listTopics().names(), "listar tópicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeTopics(nombres).allTopicNames(), "describir tópicos").values().stream()
                .map(t -> new TopicoResumen(t.name(), t.partitions().size(), replicas(t), esProtegido(t.name())))
                .sorted(Comparator.comparing(TopicoResumen::nombre))
                .toList();
    }

    @Override
    public TopicoDetalle detalleTopico(String nombre) {
        TopicDescription descripcion = describir(nombre);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all(), "leer la configuración de " + nombre)
                .get(recurso);
        Map<String, String> configs = new TreeMap<>();
        if (config != null) {
            config.entries().stream()
                    .filter(e -> !e.isDefault() && !e.isSensitive() && e.value() != null)
                    .forEach(e -> configs.put(e.name(), e.value()));
        }
        List<ParticionInfo> particiones = descripcion.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> new ParticionInfo(p.partition(),
                        p.leader() != null ? p.leader().id() : null,
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();
        return new TopicoDetalle(nombre, esProtegido(nombre), particiones, configs);
    }

    @Override
    public TopicoDetalle crearTopico(CrearTopicoRequest request) {
        String nombre = request.nombre();
        validarNoReservado(nombre);
        Map<String, String> configs = request.configs() != null ? request.configs() : Map.of();
        validarConfigs(configs, request.replicas());

        NewTopic nuevo = new NewTopic(nombre, request.particiones(), request.replicas()).configs(configs);
        esperar(admin.createTopics(List.of(nuevo)).all(), "crear el tópico " + nombre);
        log.info("Tópico creado [nombre={}, particiones={}, replicas={}, configs={}]",
                nombre, request.particiones(), request.replicas(), configs);
        return detalleTopico(nombre);
    }

    @Override
    public TopicoDetalle actualizarConfiguracion(String nombre, Map<String, String> configs) {
        validarNoReservado(nombre);
        validarConfigs(configs, replicas(describir(nombre)));

        List<AlterConfigOp> operaciones = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        alterarConfiguracion(nombre, operaciones);
        log.info("Configuración actualizada [topico={}, configs={}]", nombre, configs);
        return detalleTopico(nombre);
    }

    @Override
    public TopicoDetalle eliminarConfiguracion(String nombre, String clave) {
        validarNoReservado(nombre);
        validarClave(clave);
        describir(nombre);
        alterarConfiguracion(nombre, List.of(new AlterConfigOp(new ConfigEntry(clave, ""), AlterConfigOp.OpType.DELETE)));
        log.info("Configuración eliminada, vuelve al valor del broker [topico={}, clave={}]", nombre, clave);
        return detalleTopico(nombre);
    }

    @Override
    public TopicoDetalle aumentarParticiones(String nombre, int total) {
        validarNoReservado(nombre);
        int actuales = describir(nombre).partitions().size();
        if (total <= actuales) {
            throw new DatoInvalidoException("El tópico " + nombre + " tiene " + actuales
                    + " particiones; Kafka solo permite aumentarlas");
        }
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all(),
                "aumentar las particiones de " + nombre);
        log.warn("Particiones aumentadas [topico={}, de={}, a={}]: las claves existentes pueden cambiar de partición",
                nombre, actuales, total);
        return detalleTopico(nombre);
    }

    @Override
    public void eliminarTopico(String nombre) {
        validarNoReservado(nombre);
        if (esProtegido(nombre)) {
            throw new ConflictoException("El tópico " + nombre + " es del sistema y no se puede eliminar");
        }
        esperar(admin.deleteTopics(List.of(nombre)).all(), "eliminar el tópico " + nombre);
        log.info("Tópico eliminado [nombre={}]", nombre);
    }

    // ===================== Grupos de consumidores =====================

    @Override
    public List<GrupoResumen> listarGrupos() {
        return esperar(admin.listGroups(ListGroupsOptions.forConsumerGroups()).all(), "listar grupos").stream()
                .map(g -> new GrupoResumen(g.groupId(),
                        g.groupState().map(Enum::name).orElse("DESCONOCIDO"),
                        g.type().map(Enum::name).orElse("DESCONOCIDO")))
                .sorted(Comparator.comparing(GrupoResumen::grupoId))
                .toList();
    }

    @Override
    public GrupoDetalle detalleGrupo(String grupoId) {
        ConsumerGroupDescription grupo = describirGrupo(grupoId);
        Map<TopicPartition, OffsetAndMetadata> confirmados = esperar(
                admin.listConsumerGroupOffsets(grupoId).partitionsToOffsetAndMetadata(),
                "leer los offsets del grupo " + grupoId);
        Map<TopicPartition, Long> finales = offsets(confirmados.keySet(), OffsetSpec.latest());

        List<OffsetParticion> offsets = confirmados.entrySet().stream()
                .map(e -> {
                    Long confirmado = e.getValue() != null ? e.getValue().offset() : null;
                    Long fin = finales.get(e.getKey());
                    Long lag = confirmado != null && fin != null ? Math.max(0, fin - confirmado) : null;
                    return new OffsetParticion(e.getKey().topic(), e.getKey().partition(), confirmado, fin, lag);
                })
                .sorted(Comparator.comparing(OffsetParticion::topico).thenComparingInt(OffsetParticion::particion))
                .toList();

        List<MiembroGrupo> miembros = grupo.members().stream()
                .map(m -> new MiembroGrupo(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition())
                                .sorted()
                                .toList()))
                .sorted(Comparator.comparing(MiembroGrupo::clientId))
                .toList();

        long lagTotal = offsets.stream().map(OffsetParticion::lag).filter(l -> l != null).mapToLong(Long::longValue).sum();
        return new GrupoDetalle(grupoId, grupo.groupState().name(),
                grupo.coordinator() != null ? grupo.coordinator().host() + ":" + grupo.coordinator().port() : null,
                miembros, offsets, lagTotal);
    }

    @Override
    public List<OffsetParticion> reiniciarOffsets(String grupoId, String topico, PosicionOffset posicion) {
        ConsumerGroupDescription grupo = describirGrupo(grupoId);
        if (!grupo.members().isEmpty()) {
            throw new ConflictoException("El grupo " + grupoId + " tiene " + grupo.members().size()
                    + " consumidores activos: deténgalos antes de mover sus offsets");
        }
        List<TopicPartition> particiones = describir(topico).partitions().stream()
                .map(p -> new TopicPartition(topico, p.partition()))
                .toList();
        Map<TopicPartition, Long> destino = offsets(particiones,
                posicion == PosicionOffset.EARLIEST ? OffsetSpec.earliest() : OffsetSpec.latest());

        Map<TopicPartition, OffsetAndMetadata> nuevos = new HashMap<>();
        destino.forEach((tp, offset) -> nuevos.put(tp, new OffsetAndMetadata(offset)));
        esperar(admin.alterConsumerGroupOffsets(grupoId, nuevos).all(), "mover los offsets del grupo " + grupoId);
        log.warn("Offsets reiniciados [grupo={}, topico={}, posicion={}, offsets={}]", grupoId, topico, posicion, destino);

        return destino.entrySet().stream()
                .map(e -> new OffsetParticion(topico, e.getKey().partition(), e.getValue(), null, null))
                .sorted(Comparator.comparingInt(OffsetParticion::particion))
                .toList();
    }

    // ===================== Apoyo =====================

    private TopicDescription describir(String nombre) {
        return esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), "describir el tópico " + nombre)
                .get(nombre);
    }

    /** Kafka no responde "no existe" para un grupo desconocido: lo describe en estado DEAD. */
    private ConsumerGroupDescription describirGrupo(String grupoId) {
        ConsumerGroupDescription grupo = esperar(admin.describeConsumerGroups(List.of(grupoId)).all(),
                "describir el grupo " + grupoId).get(grupoId);
        if (grupo == null || grupo.groupState() == GroupState.DEAD) {
            throw new RecursoNoEncontradoException("No existe el grupo de consumidores " + grupoId);
        }
        return grupo;
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> particiones, OffsetSpec spec) {
        if (particiones.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> consulta = particiones.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> spec));
        Map<TopicPartition, ListOffsetsResultInfo> info = esperar(admin.listOffsets(consulta).all(), "leer offsets");
        Map<TopicPartition, Long> resultado = new LinkedHashMap<>();
        info.forEach((tp, i) -> resultado.put(tp, i.offset()));
        return resultado;
    }

    private void alterarConfiguracion(String nombre, List<AlterConfigOp> operaciones) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all(),
                "cambiar la configuración de " + nombre);
    }

    private void validarConfigs(Map<String, String> configs, int replicas) {
        configs.keySet().forEach(this::validarClave);
        String politica = configs.get(CLEANUP_POLICY);
        if (politica != null && !CLEANUP_POLICIES.contains(politica.replace(" ", ""))) {
            throw new DatoInvalidoException("cleanup.policy admite: delete, compact o compact,delete");
        }
        String minIsr = configs.get(MIN_ISR);
        if (minIsr != null) {
            int valor;
            try {
                valor = Integer.parseInt(minIsr);
            } catch (NumberFormatException e) {
                throw new DatoInvalidoException("min.insync.replicas debe ser un número");
            }
            if (valor < 1 || valor > replicas) {
                throw new DatoInvalidoException("min.insync.replicas debe estar entre 1 y el factor de replicación ("
                        + replicas + "); si no, ningún productor con acks=all podría escribir");
            }
        }
    }

    private void validarClave(String clave) {
        if (!properties.configuracionesPermitidas().contains(clave)) {
            throw new DatoInvalidoException("La configuración " + clave + " no se puede administrar desde esta API. "
                    + "Permitidas: " + properties.configuracionesPermitidas());
        }
    }

    /** Los tópicos que empiezan con "_" son internos de Kafka (__consumer_offsets, _confluent...). */
    private static void validarNoReservado(String nombre) {
        if (nombre.startsWith("_")) {
            throw new DatoInvalidoException("El tópico " + nombre + " es interno de Kafka");
        }
    }

    private boolean esProtegido(String nombre) {
        return properties.protegidos().contains(nombre);
    }

    private static int replicas(TopicDescription descripcion) {
        return descripcion.partitions().isEmpty() ? 0 : descripcion.partitions().get(0).replicas().size();
    }

    private <T> T esperar(KafkaFuture<T> futuro, String operacion) {
        try {
            return futuro.get(properties.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaNoDisponibleException("Operación interrumpida al " + operacion, e);
        } catch (TimeoutException e) {
            throw new KafkaNoDisponibleException("Kafka no respondió a tiempo al " + operacion, e);
        } catch (ExecutionException e) {
            throw traducir(e.getCause(), operacion);
        }
    }

    static RuntimeException traducir(Throwable causa, String operacion) {
        String detalle = causa != null && causa.getMessage() != null ? causa.getMessage() : "sin detalle";
        return switch (causa) {
            case TopicExistsException e -> new ConflictoException("El tópico ya existe: " + detalle, e);
            case UnknownTopicOrPartitionException e -> new RecursoNoEncontradoException("El tópico no existe", e);
            case GroupIdNotFoundException e -> new RecursoNoEncontradoException("El grupo no existe", e);
            case GroupNotEmptyException e -> new ConflictoException("El grupo tiene consumidores activos", e);
            case UnknownMemberIdException e -> new ConflictoException("El grupo tiene consumidores activos", e);
            case InvalidPartitionsException e -> new DatoInvalidoException(detalle, e);
            case InvalidConfigurationException e -> new DatoInvalidoException(detalle, e);   // incluye réplicas inválidas
            case PolicyViolationException e -> new DatoInvalidoException(detalle, e);
            case InvalidRequestException e -> new DatoInvalidoException(detalle, e);
            case RetriableException e -> new KafkaNoDisponibleException(
                    "Kafka no está disponible al " + operacion + ": " + detalle, e);
            case null, default -> new IllegalStateException("Error inesperado de Kafka al " + operacion + ": " + detalle, causa);
        };
    }
}
