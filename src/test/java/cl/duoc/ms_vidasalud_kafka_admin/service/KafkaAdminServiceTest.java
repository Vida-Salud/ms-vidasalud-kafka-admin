package cl.duoc.ms_vidasalud_kafka_admin.service;

import cl.duoc.ms_vidasalud_kafka_admin.config.KafkaAdminProperties;
import cl.duoc.ms_vidasalud_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_vidasalud_kafka_admin.exception.ConflictoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.DatoInvalidoException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.KafkaNoDisponibleException;
import cl.duoc.ms_vidasalud_kafka_admin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaAdminServiceTest {

    private final Admin admin = mock(Admin.class);
    private final KafkaAdminService service = new KafkaAdminServiceImpl(admin, new KafkaAdminProperties(
            Duration.ofSeconds(2),
            List.of("appointments.events", "audit.timeline"),
            List.of("cleanup.policy", "retention.ms", "min.insync.replicas")));

    @Test
    void noSeEliminaUnTopicoDelSistema() {
        assertThatThrownBy(() -> service.eliminarTopico("appointments.events")).isInstanceOf(ConflictoException.class);
        verifyNoInteractions(admin);
    }

    @Test
    void noSeTocanTopicosInternos() {
        assertThatThrownBy(() -> service.eliminarTopico("__consumer_offsets")).isInstanceOf(DatoInvalidoException.class);
        verifyNoInteractions(admin);
    }

    @Test
    void unaConfiguracionNoPermitidaSeRechazaAntesDeLlamarAKafka() {
        var request = new CrearTopicoRequest("demo", 3, (short) 3, Map.of("unclean.leader.election.enable", "true"));
        assertThatThrownBy(() -> service.crearTopico(request)).isInstanceOf(DatoInvalidoException.class);
        verifyNoInteractions(admin);
    }

    @Test
    void minInsyncMayorQueLasReplicasSeRechaza() {
        var request = new CrearTopicoRequest("demo", 3, (short) 2, Map.of("min.insync.replicas", "3"));
        assertThatThrownBy(() -> service.crearTopico(request))
                .isInstanceOf(DatoInvalidoException.class)
                .hasMessageContaining("min.insync.replicas");
    }

    @Test
    void cleanupPolicyDesconocidaSeRechaza() {
        var request = new CrearTopicoRequest("demo", 3, (short) 3, Map.of("cleanup.policy", "borrar"));
        assertThatThrownBy(() -> service.crearTopico(request)).isInstanceOf(DatoInvalidoException.class);
    }

    @Test
    void elTopicoSeCreaConParticionesReplicasYConfiguracion() {
        CreateTopicsResult resultado = mock(CreateTopicsResult.class);
        KafkaFutureImpl<Void> futuro = new KafkaFutureImpl<>();
        futuro.completeExceptionally(new TopicExistsException("Topic 'demo' already exists."));
        when(resultado.all()).thenReturn(futuro);
        when(admin.createTopics(anyCollection())).thenReturn(resultado);

        var request = new CrearTopicoRequest("demo", 3, (short) 3, Map.of("retention.ms", "86400000"));
        assertThatThrownBy(() -> service.crearTopico(request)).isInstanceOf(ConflictoException.class);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic enviado = captor.getValue().iterator().next();
        assertThat(enviado.name()).isEqualTo("demo");
        assertThat(enviado.numPartitions()).isEqualTo(3);
        assertThat(enviado.replicationFactor()).isEqualTo((short) 3);
        assertThat(enviado.configs()).containsEntry("retention.ms", "86400000");
    }

    @Test
    void losErroresDeKafkaSeTraducenACodigosHttp() {
        assertThat(KafkaAdminServiceImpl.traducir(new TopicExistsException("x"), "crear"))
                .isInstanceOf(ConflictoException.class);
        assertThat(KafkaAdminServiceImpl.traducir(new UnknownTopicOrPartitionException("x"), "describir"))
                .isInstanceOf(RecursoNoEncontradoException.class);
        assertThat(KafkaAdminServiceImpl.traducir(new InvalidReplicationFactorException("x"), "crear"))
                .isInstanceOf(DatoInvalidoException.class);
        assertThat(KafkaAdminServiceImpl.traducir(new TimeoutException("x"), "listar"))
                .isInstanceOf(KafkaNoDisponibleException.class);
    }
}
