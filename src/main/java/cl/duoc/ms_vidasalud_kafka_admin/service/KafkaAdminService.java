package cl.duoc.ms_vidasalud_kafka_admin.service;

import cl.duoc.ms_vidasalud_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_vidasalud_kafka_admin.dto.PosicionOffset;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.ClusterInfo;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.GrupoResumen;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.OffsetParticion;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoDetalle;
import cl.duoc.ms_vidasalud_kafka_admin.dto.Respuestas.TopicoResumen;

import java.util.List;
import java.util.Map;

public interface KafkaAdminService {

    ClusterInfo cluster();

    List<TopicoResumen> listarTopicos();

    TopicoDetalle detalleTopico(String nombre);

    TopicoDetalle crearTopico(CrearTopicoRequest request);

    TopicoDetalle actualizarConfiguracion(String nombre, Map<String, String> configs);

    TopicoDetalle eliminarConfiguracion(String nombre, String clave);

    TopicoDetalle aumentarParticiones(String nombre, int total);

    void eliminarTopico(String nombre);

    List<GrupoResumen> listarGrupos();

    GrupoDetalle detalleGrupo(String grupoId);

    List<OffsetParticion> reiniciarOffsets(String grupoId, String topico, PosicionOffset posicion);
}
