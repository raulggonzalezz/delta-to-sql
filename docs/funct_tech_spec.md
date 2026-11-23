# delta-to-sql
## Especificación funcional y técnica — versión 0.1

**Estado:** Diseño acordado, pendiente de implementación e integración  
**Proyecto:** ISDF Framework 2.0  
**Componente:** `delta-to-sql`

---

## 1. Propósito

`delta-to-sql` es un componente Python reutilizable cuya responsabilidad es replicar el contenido completo de una tabla Delta Lake almacenada en S3 hacia una tabla física PostgreSQL.

El componente está pensado para publicar Data Products destinados a consumo analítico. No implementa CDC, sincronización incremental ni procesamiento de eventos: cada ejecución trabaja sobre un snapshot completo de Delta.

La ejecución será invocada desde Airflow mediante `PythonOperator`. Airflow proporciona la orquestación; `delta-to-sql` implementa la lógica de replicación.

## 2. Objetivos

- Leer tablas Delta desde S3 utilizando el entorno Spark disponible.
- Identificar y fijar la versión Delta que se va a replicar.
- Obtener automáticamente el esquema desde el snapshot Delta.
- Crear y cargar tablas PostgreSQL de forma eficiente.
- Ofrecer distintas estrategias de sustitución.
- Publicar versiones físicas de PostgreSQL y una vista lógica estable.
- Gestionar la retención de versiones físicas.
- Exponer una API Python sencilla para Airflow.
- Mantener el componente pequeño, independiente y preparado para una futura integración con Trace-core.

## 3. Fuera de alcance de la versión 0.1

- Construcción o mantenimiento de imágenes Docker.
- Instalación o administración del clúster Spark.
- Definición de DAGs de Airflow.
- Índices configurables por YAML.
- Integración efectiva con Trace-core.
- Replicación incremental, CDC o merge.
- Gestión automática de vistas, claves foráneas u otras dependencias arbitrarias de PostgreSQL.
- Conversión universal de todos los tipos específicos de Spark/Delta.

El despliegue, las dependencias del runtime Spark y la conectividad serán responsabilidad del entorno que integre el componente.

## 4. Arquitectura y responsabilidades

### 4.1 Flujo general

1. Airflow invoca la función Python del componente.
2. El componente carga la configuración global.
3. Crea u obtiene una `SparkSession` según el entorno de ejecución.
4. Consulta la versión actual de la tabla Delta.
5. Lee explícitamente ese snapshot mediante `versionAsOf`.
6. Obtiene el esquema del DataFrame.
7. Ejecuta la estrategia PostgreSQL configurada.
8. Carga los registros mediante PostgreSQL `COPY`.
9. Publica el resultado cuando la carga está completa.
10. Aplica la política de retención, si corresponde.
11. Devuelve un resultado estructurado a Airflow.

### 4.2 Responsabilidades

| Elemento | Responsabilidad |
|---|---|
| Airflow | Orquestación, calendario, reintentos y parámetros de cada ejecución |
| Spark / Delta Lake | Lectura distribuida del snapshot Delta en S3 |
| delta-to-sql | Esquema, estrategia, transferencia, publicación y retención |
| PostgreSQL | Almacenamiento físico, transacciones y vista lógica |
| Trace-core | Integración futura para trazabilidad y observabilidad |

El componente no debe asumir que Airflow comparte una transacción PostgreSQL con los ejecutores Spark. Cada partición de Spark utiliza su propia conexión PostgreSQL.

## 5. Interfaz Python

La interfaz principal será una función invocable desde `PythonOperator`.

Firma propuesta:

```python
def replicate(
    source: str,
    target_schema: str,
    target_table: str,
    config_path: str | None = None,
    strategy: str | None = None,
) -> ReplicationResult:
    ...
```

- `source`: ruta Delta, por ejemplo `s3a://bucket/data-products/customers`.
- `target_schema`: esquema PostgreSQL.
- `target_table`: nombre lógico de destino.
- `config_path`: ruta opcional al YAML global. Si se omite, se utiliza la ubicación predeterminada documentada.
- `strategy`: override opcional de la estrategia global.

La configuración y las credenciales no se pasan como argumentos sensibles en Airflow. Las credenciales PostgreSQL se obtienen de variables de entorno o del mecanismo de secretos que proporcione el entorno.

### 5.1 Resultado

La función devolverá un objeto `ReplicationResult` serializable, con al menos:

- `source`
- `target_schema`
- `target_table`
- `strategy`
- `delta_version` (cuando aplique)
- `physical_table` (cuando aplique)
- `rows_written` (si se puede obtener de forma fiable)
- `status`
- `started_at`
- `completed_at`
- `duration_seconds`

Los errores deben propagarse como excepciones para que Airflow pueda marcar la tarea como fallida y aplicar sus reintentos.

## 6. Configuración global

Un único YAML compartido por todas las tablas y Data Products. Los parámetros particulares de cada ejecución se pasan mediante la interfaz Python.

Ejemplo inicial:

```yaml
postgres:
  host: ${POSTGRES_HOST}
  port: 5432
  database: analytics
  username: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
  sslmode: prefer

replication:
  strategy: versioned

  retention:
    keep_previous_versions: true
    max_versions: 3

  performance:
    partitions: 8

logging:
  level: INFO
```

### 6.1 Estrategia

Valores admitidos:

- `truncate`
- `table_swap`
- `drop_create`
- `versioned`

La estrategia predeterminada será `versioned`.

### 6.2 Retención

- `keep_previous_versions: true`: conserva las versiones físicas anteriores.
- `keep_previous_versions: false`: después de publicar correctamente una nueva versión, elimina las versiones físicas anteriores que ya no estén activas.
- `max_versions`: número máximo de versiones físicas que se desea conservar, incluyendo la versión activa. Solo se aplica cuando `keep_previous_versions` es `true`.

La tabla física activa nunca se elimina. La limpieza se ejecuta únicamente después de una publicación satisfactoria.

### 6.3 Rendimiento

`partitions` controla el número de particiones de escritura y, por tanto, el paralelismo aproximado de conexiones PostgreSQL. Debe dimensionarse según la capacidad de PostgreSQL, no solo según los recursos Spark.

## 7. Lectura Delta y consistencia de versión

La versión Delta es la única fuente de verdad para el versionado de la estrategia `versioned`.

El proceso debe:

1. Consultar la última versión confirmada de Delta.
2. Guardar ese número como `delta_version` para toda la ejecución.
3. Leer el DataFrame utilizando `versionAsOf=delta_version`.
4. No volver a consultar la última versión a mitad de la ejecución.

Esto garantiza que el esquema y los datos proceden del mismo snapshot, aunque la tabla Delta avance mientras se está replicando.

El nombre de la tabla física será:

`<target_table>_v<delta_version>`

Ejemplo:

- Tabla Delta: versión 33.
- Tabla física PostgreSQL: `analytics.customers_v33`.
- Nombre lógico PostgreSQL: `analytics.customers`.

Los identificadores SQL deben componerse mediante APIs seguras de psycopg, nunca mediante interpolación directa de strings.

## 8. Estrategias de publicación

### 8.1 `versioned` — estrategia recomendada y predeterminada

Objetivo: crear una tabla física por versión Delta y exponer la versión activa mediante una vista PostgreSQL estable.

Ejemplo:

```text
analytics.customers       VIEW -> analytics.customers_v33
analytics.customers_v33   TABLE
analytics.customers_v32   TABLE
analytics.customers_v31   TABLE
```

Proceso:

1. Consultar y fijar la versión Delta.
2. Comprobar si la vista lógica ya apunta a la tabla física de esa versión. Si es así, la ejecución puede finalizar como `SKIPPED` sin recargar.
3. Crear una tabla de carga temporal con nombre único y tipos derivados del esquema Delta.
4. Cargar el snapshot fijado mediante `COPY` desde cada partición Spark.
5. Cuando todas las particiones terminen correctamente, convertir la tabla temporal en la tabla física versionada.
6. Publicar o actualizar la vista lógica para que apunte a la nueva tabla física.
7. Ejecutar la retención.

La tabla de carga temporal evita que una ejecución fallida deje una tabla versionada aparentemente completa. Si la carga falla, la vista activa no cambia. En un reintento, la tabla temporal abandonada puede limpiarse y la misma versión Delta se vuelve a procesar.

La conversión de tabla temporal a tabla versionada y la actualización de la vista deben coordinarse de forma transaccional siempre que PostgreSQL lo permita.

#### Vista lógica

La vista `analytics.customers` será creada y mantenida automáticamente por `delta-to-sql`.

No es una vista materializada. No almacena una copia de los datos: consulta la tabla física versionada subyacente. Los datos y sus índices residen en la tabla física.

Para cambios de columnas, `CREATE OR REPLACE VIEW` tiene restricciones en PostgreSQL. La implementación debe definir un procedimiento seguro para actualizar la vista cuando cambie el esquema. No debe eliminar y recrear una vista a ciegas si ello puede romper permisos o dependencias. Las dependencias arbitrarias de terceros quedan fuera del alcance inicial y deben documentarse.

#### Reintentos

- Si la vista ya apunta a `customers_v33`, no se vuelve a cargar v33.
- Si existe una tabla temporal incompleta para v33, se elimina y se reconstruye.
- Si la tabla versionada está completa pero la vista aún no se ha publicado, la implementación debe poder completar la publicación sin confundirla con una versión activa.
- Si Delta avanza a v34, se procesa v34. No es obligatorio publicar snapshots intermedios que nunca llegaron a PostgreSQL, porque se replica el estado completo más reciente, no eventos.

No se mantiene un contador de versiones independiente en PostgreSQL. La versión del nombre físico procede siempre de Delta.

### 8.2 `truncate`

Objetivo: conservar la tabla física existente y sustituir su contenido.

Para permitir escritura paralela sin exponer una tabla parcialmente cargada, el diseño debe usar una tabla temporal de carga:

1. Crear tabla temporal con el esquema Delta.
2. Cargar la tabla temporal mediante COPY paralelo.
3. Publicar en una transacción PostgreSQL: `TRUNCATE` destino y `INSERT INTO destino SELECT ... FROM staging`.
4. Eliminar staging al confirmar.

El `TRUNCATE` y el `INSERT ... SELECT` deben estar en la misma transacción. Si falla la publicación, PostgreSQL revierte el cambio y conserva el contenido anterior.

Esta estrategia requiere que las columnas destino sean compatibles con el esquema entrante. La evolución de esquema debe resolverse antes de la publicación y no puede implicar conversiones inseguras.

### 8.3 `table_swap`

Objetivo: sustituir la tabla física por una nueva tabla cargada previamente.

1. Crear tabla temporal de carga.
2. Cargar mediante COPY.
3. Preparar la tabla nueva.
4. En una transacción, renombrar la tabla actual y la nueva, y retirar la anterior.

PostgreSQL no dispone de una operación nativa `SWAP TABLE`. El intercambio por renombrado tiene implicaciones sobre OID, vistas, claves foráneas, permisos, triggers, secuencias y dependencias. La implementación inicial debe documentar estas limitaciones y no prometer que todas las dependencias se actualizan automáticamente.

### 8.4 `drop_create`

Objetivo: eliminar y crear de nuevo la tabla destino.

Se implementará como estrategia explícitamente destructiva. Debe crear y cargar la nueva estructura siguiendo un procedimiento que minimice el tiempo sin tabla. No se utilizará `CASCADE` implícitamente. La operación debe fallar con un error claro si existen dependencias que impidan eliminar la tabla.

## 9. Carga masiva PostgreSQL

La escritura se realizará con `psycopg` 3 y `COPY FROM STDIN`, no con INSERT fila a fila ni JDBC batch como mecanismo predeterminado.

Spark distribuirá los registros entre particiones. Cada partición:

1. Abre su propia conexión PostgreSQL.
2. Ejecuta COPY sobre la tabla temporal correspondiente.
3. Escribe sus filas.
4. Cierra o confirma su operación de carga.

El driver solo publica después de que la acción Spark haya terminado correctamente.

La cantidad de particiones debe limitarse para no saturar PostgreSQL. La carga paralela usa varias transacciones independientes en staging; la atomicidad frente a consumidores se obtiene porque staging no es la tabla publicada. La publicación final es una transacción PostgreSQL separada.

No se debe mantener una transacción abierta en el driver esperando que las conexiones de los ejecutores participen en ella: son conexiones independientes.

## 10. Mapeo de esquema

El esquema se obtiene desde `DataFrame.schema`, sin recorrer todos los registros.

Mapeo inicial previsto:

| Spark | PostgreSQL |
|---|---|
| StringType | TEXT |
| BooleanType | BOOLEAN |
| ByteType | SMALLINT |
| ShortType | SMALLINT |
| IntegerType | INTEGER |
| LongType | BIGINT |
| FloatType | REAL |
| DoubleType | DOUBLE PRECISION |
| DecimalType(p,s) | NUMERIC(p,s) |
| DateType | DATE |
| TimestampType | TIMESTAMP |
| TimestampNTZType | TIMESTAMP |
| BinaryType | BYTEA |
| ArrayType | JSONB |
| MapType | JSONB |
| StructType | JSONB |

El soporte exacto debe ajustarse a la versión real de Spark del entorno. Los tipos no soportados deben provocar un error explícito antes de iniciar la carga.

Los tipos complejos se serializarán como JSON compatible con PostgreSQL JSONB. La conversión debe preservar nulos y valores temporales de forma definida.

### 10.1 Evolución de esquema

Política inicial `evolve`:

- Crear la tabla con el esquema Delta cuando no exista.
- Añadir columnas nuevas cuando se utilice `truncate` y la tabla destino deba evolucionar.
- En `versioned`, cada versión física se crea con el esquema exacto del snapshot; la vista lógica se actualiza para exponer ese esquema.
- No eliminar columnas antiguas silenciosamente en tablas no versionadas.
- No cambiar automáticamente tipos existentes si la conversión no es inequívocamente segura.
- Los cambios incompatibles deben fallar con un mensaje que identifique columna, tipo Spark y tipo PostgreSQL.

## 11. Concurrencia y seguridad

- Debe existir exclusión mutua por combinación de `target_schema` y `target_table`, especialmente para `versioned`.
- Se recomienda un advisory lock PostgreSQL mantenido durante la coordinación y publicación.
- Los nombres de tablas temporales deben ser únicos por ejecución.
- No se deben interpolar identificadores SQL sin escape seguro.
- Las credenciales se obtienen de variables de entorno o del gestor de secretos del entorno.
- El acceso S3 se delega al mecanismo de credenciales del runtime Spark (por ejemplo, IAM role o credenciales Hadoop).
- No se registran contraseñas ni secretos en logs.

## 12. Estructura del proyecto

Estructura deliberadamente pequeña:

```text
delta-to-sql/
├── src/
│   └── app/
│       ├── __init__.py
│       ├── main.py
│       ├── config.py
│       ├── replicator.py
│       └── postgres.py
├── config/
│   └── config.yaml
├── pyproject.toml
├── requirements.txt
├── uv.lock
├── .python-version
├── .env.example
├── .gitignore
└── README.md
```

Responsabilidades:

- `main.py`: interfaz de entrada opcional para ejecución manual; no es requisito para PythonOperator.
- `config.py`: carga y validación de configuración.
- `replicator.py`: coordinación Spark, versión, estrategia y resultado.
- `postgres.py`: DDL, COPY, publicación, vistas, locks y retención.

No se añaden capas o módulos adicionales hasta que exista una necesidad real.

## 13. Dependencias previstas

Las versiones concretas se fijarán cuando se conozca el runtime Spark del entorno.

- `pyspark`: API de Spark.
- `delta-spark`: integración Delta Lake compatible con Spark.
- `psycopg[binary]`: conexión PostgreSQL y COPY.
- `PyYAML`: lectura del YAML.
- `pydantic` o `pydantic-settings`: validación de configuración, si se considera necesario.

`pyproject.toml` será la fuente principal de dependencias. `requirements.txt` será el fichero compatible con el proceso de instalación acordado. `uv.lock` fijará el entorno una vez cerradas las versiones.

No se incluirán Airflow ni Docker como dependencias del componente.

## 14. Integración con Airflow

Ejemplo conceptual:

```python
from airflow.operators.python import PythonOperator
from app.replicator import replicate

replicate_customers = PythonOperator(
    task_id="replicate_customers",
    python_callable=replicate,
    op_kwargs={
        "source": "s3a://data-products/customers",
        "target_schema": "analytics",
        "target_table": "customers",
    },
)
```

El entorno Airflow debe tener disponible el paquete `app` y sus dependencias en el proceso que ejecuta el callable. Además, el código y las dependencias requeridas por los ejecutores Spark deben estar disponibles en su runtime; esto depende del despliegue Spark y no lo resuelve automáticamente PythonOperator.

## 15. Logging y errores

El componente registrará, como mínimo:

- Inicio y fin de la ejecución.
- Origen y destino.
- Estrategia seleccionada.
- Versión Delta fijada.
- Tabla física de carga y tabla publicada.
- Número de particiones configuradas.
- Resultado de publicación.
- Duración.
- Excepción y contexto técnico sin secretos.

Los errores se propagan al llamador. No se ocultan ni se convierten en éxito.

## 16. Criterios de aceptación funcional

1. Una tabla Delta de S3 puede replicarse a PostgreSQL con la estrategia seleccionada.
2. La versión Delta queda fijada y los datos leídos corresponden a ese snapshot.
3. La carga masiva utiliza COPY y no INSERT fila a fila.
4. En `versioned`, la tabla física utiliza el sufijo de la versión Delta.
5. En `versioned`, el componente crea y mantiene automáticamente la vista lógica.
6. La vista solo cambia después de completar la carga.
7. Un fallo de carga no altera la versión lógica publicada.
8. Repetir una versión ya publicada no genera una nueva copia innecesaria.
9. La retención se ejecuta después de publicar y nunca elimina la versión activa.
10. Las estrategias `truncate`, `table_swap` y `drop_create` tienen comportamientos y limitaciones documentados.
11. La configuración es global y los parámetros de tabla se proporcionan por llamada.
12. La función principal puede invocarse desde Airflow mediante PythonOperator.
13. No se requiere instalar Airflow dentro de `delta-to-sql`.

## 17. Decisiones pendientes antes de producción

- Versión exacta de Spark y Delta Lake.
- Tipo de despliegue Spark y forma de distribuir código/dependencias a ejecutores.
- Versión de PostgreSQL.
- Política definitiva de compatibilidad y evolución de tipos.
- Comportamiento de la vista cuando cambia el esquema y existen consumidores con dependencias.
- Límites de concurrencia y número de particiones.
- Permisos PostgreSQL necesarios para crear tablas, vistas, índices futuros y advisory locks.
- Política de limpieza de tablas temporales abandonadas.

Estas decisiones no impiden implementar el núcleo, pero deben cerrarse antes de certificar el despliegue productivo.

---

**Principio de diseño:** `delta-to-sql` debe hacer una sola cosa: publicar en PostgreSQL una representación completa y consultable de un snapshot Delta, de forma eficiente, repetible y segura. Airflow orquesta; Spark lee; PostgreSQL almacena y publica.
