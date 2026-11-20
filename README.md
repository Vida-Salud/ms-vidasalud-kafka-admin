# VidaSalud - Microservicio administrador de Kafka

API REST (rol Admin) para administrar el clúster Kafka con el AdminClient, encapsulado en `KafkaAdminService`.

| Operación | Endpoint |
| --- | --- |
| Clúster (brokers, controlador) | `GET /api/v1/kafka/cluster` |
| Listar / describir tópicos | `GET /api/v1/kafka/topics` · `GET /api/v1/kafka/topics/{name}` |
| Crear tópico | `POST /api/v1/kafka/topics` `{"nombre":"x","particiones":3,"replicas":3,"configs":{"retention.ms":"86400000"}}` |
| Cambiar configuración | `PUT /api/v1/kafka/topics/{name}/configs` `{"configs":{"retention.ms":"172800000"}}` |
| Quitar configuración | `DELETE /api/v1/kafka/topics/{name}/configs/{key}` |
| Aumentar particiones | `PUT /api/v1/kafka/topics/{name}/partitions` `{"particiones":6}` |
| Eliminar tópico | `DELETE /api/v1/kafka/topics/{name}` (409 si es del sistema) |
| Consumer groups | `GET /api/v1/kafka/consumer-groups` · `GET /api/v1/kafka/consumer-groups/{groupId}` (miembros, offsets, lag) |
| Reprocesar / saltar | `POST /api/v1/kafka/consumer-groups/{groupId}/offsets/reset` `{"topico":"appointments.events","posicion":"EARLIEST"}` |

Validaciones: nombres `[a-zA-Z0-9._-]`, 1 a 48 particiones, factor de replicación 1 a 3, solo las configuraciones
de `vidasalud.kafka-admin.configuraciones-permitidas`, `min.insync.replicas` menor o igual a las réplicas,
`cleanup.policy` válida, tópicos internos (`_...`) intocables, tópicos del sistema no eliminables.
Errores de Kafka traducidos: 404 (no existe), 409 (ya existe / grupo activo), 400 (configuración inválida), 503 (sin respuesta).

Puerto 8095. Variables: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `KAFKA_BOOTSTRAP_SERVERS`.
