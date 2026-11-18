package cl.duoc.ms_vidasalud_kafka_admin.controller;

import cl.duoc.ms_vidasalud_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.exception.ConflictoException;
import cl.duoc.ms_vidasalud_kafka_admin.service.KafkaAdminService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class KafkaAdminControllerTest {

    @Autowired MockMvc mockMvc;
    @MockitoBean KafkaAdminService service;

    @Test
    void sinTokenResponde401YSinRolAdmin403() throws Exception {
        mockMvc.perform(get("/api/v1/kafka/topics")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/kafka/topics").with(rol("Operador"))).andExpect(status().isForbidden());
        mockMvc.perform(delete("/api/v1/kafka/topics/demo").with(rol("Auditor"))).andExpect(status().isForbidden());
        verify(service, never()).eliminarTopico(any());
    }

    @Test
    void crearTopicoValidoResponde201() throws Exception {
        CrearTopicoRequest esperado = new CrearTopicoRequest("demo.eventos", 3, (short) 3,
                Map.of("retention.ms", "86400000"));
        when(service.crearTopico(esperado)).thenReturn(new TopicoDetalle("demo.eventos", false, List.of(),
                Map.of("retention.ms", "86400000")));

        mockMvc.perform(post("/api/v1/kafka/topics").with(rol("Admin"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"nombre":"demo.eventos","particiones":3,"replicas":3,"configs":{"retention.ms":"86400000"}}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.configs['retention.ms']").value("86400000"));
    }

    @Test
    void crearTopicoInvalidoInformaCadaCampo() throws Exception {
        mockMvc.perform(post("/api/v1/kafka/topics").with(rol("Admin"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"nombre\":\"con espacios\",\"particiones\":0,\"replicas\":5}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errores.nombre").exists())
                .andExpect(jsonPath("$.errores.particiones").exists())
                .andExpect(jsonPath("$.errores.replicas").exists());
        verify(service, never()).crearTopico(any());
    }

    @Test
    void unNombreInvalidoEnLaRutaResponde400() throws Exception {
        mockMvc.perform(get("/api/v1/kafka/topics/con$simbolos").with(rol("Admin")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void eliminarUnTopicoDelSistemaResponde409() throws Exception {
        doThrow(new ConflictoException("El tópico appointments.events es del sistema y no se puede eliminar"))
                .when(service).eliminarTopico("appointments.events");

        mockMvc.perform(delete("/api/v1/kafka/topics/appointments.events").with(rol("Admin")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.detail").value("El tópico appointments.events es del sistema y no se puede eliminar"));
    }

    @Test
    void reiniciarOffsetsValidaLaPosicion() throws Exception {
        mockMvc.perform(post("/api/v1/kafka/consumer-groups/report-kpis/offsets/reset").with(rol("Admin"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topico\":\"appointments.events\",\"posicion\":\"AYER\"}"))
                .andExpect(status().isBadRequest());
    }

    private static RequestPostProcessor rol(String rol) {
        return jwt().authorities(new SimpleGrantedAuthority("APPROLE_" + rol));
    }
}
