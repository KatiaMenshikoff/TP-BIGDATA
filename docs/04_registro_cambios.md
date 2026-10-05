# Registro de cambios

Los cambios de la versión 1.1 surgen de una revisión interna del equipo, no del feedback del docente.

## Versión 1.1 (05/10)

Revisión interna del equipo. No cambia el alcance ni las decisiones de arquitectura.

### Correcciones técnicas

| Ubicación | Cambio | Motivo |
| --- | --- | --- |
| Documento de diseño, Sección 10.4 (tabla Q2) | Se agrega `as_of_date` a la partición. La partición se borra y se vuelve a escribir en cada corrida. | En Cassandra las columnas de la clave no se actualizan. Un cambio de costo generaría filas duplicadas. |
| Documento de diseño, Sección 8.3 (punto 3) | Se aclara que cada partición de Silver se reconstruye con todos los eventos de Bronze de esa fecha. | El texto no decía con qué datos se reescribía la partición. Leído literalmente, se perderían los eventos anteriores. |
| Documento de diseño, Sección 7.1 (promoción de Silver a Gold) | Gold se recalcula en cada micro-batch y el dato no es definitivo hasta superar el watermark. | Con un watermark de 62 días y 60 días de datos, ninguna fecha se cerraría y Gold no se publicaría. |

### Ajustes de documentación

| Ubicación | Cambio |
| --- | --- |
| Documento de diseño, Sección 1.2 | El usuario "Ingeniería de datos (autora)" pasa a "Ingeniería de datos". |
| Documento de diseño, Sección 7.3 | "prefijo fijo" pasa a "nombres fijos". |
| Documento de diseño, Sección 11.1 | Los roles se reparten entre los cuatro integrantes. El rol de streaming y serving se divide en dos. |
| Documento de diseño, Sección 11.3 | El repositorio figura como público. |
| README | Se lista a los integrantes del equipo. |
| Documento de diseño, encabezado y portada | La versión pasa a 1.1 y la portada lista a los cuatro integrantes. |
| Documento de diseño, Sección 8.3 (punto 3) | Se completa la última oración ("…por sí solo."). |
| Documento de diseño, Sección 11.2 | Las horas se aclaran como totales del equipo (unas 3–4 h por persona por semana); se quita la referencia a "una sola persona". |
| Plan de correcciones | Se corrige el apellido "Josephsohn". |

### Pendiente

Escuchar y corregir las correciones del docente.