# Brief Técnico-Analítico del Proyecto Sello

Este documento es el hito de **S2** del cronograma (ver [Guía del Proyecto Sello](index.md), sección 4): el punto donde el equipo declara, por escrito, qué sistema Big Data va a construir durante el ciclo - antes de avanzar a S3 (calidad de datos y formatos analíticos). No reemplaza el informe final; es la ficha corta que fija el rumbo desde el principio.

`lambda26` es un entorno integrado de procesamiento distribuido en batch y streaming con Spark y Kafka. Las guías de S1 en adelante muestran **cómo** se construye cada capacidad (arquitectura Lambda/Kappa, ETL batch, ingesta y streaming, observabilidad, BI/ML, DataOps) sobre un caso de referencia (ver el caso de Uber en S1, 1.6). Tu equipo **aplica ese mismo patrón** sobre **su propio problema de datos** - no necesariamente sobre el mismo caso de `lambda26`, ni con las mismas fuentes. El repositorio se identifica con su propio `grupo-<numero>-<nombre-proyecto>` en los topics (ver [Guía del Proyecto Sello](index.md), sección 5).

Cada equipo llena una sola copia de este brief, la publica en su repositorio (o en su MkDocs) y la actualiza solo si el alcance cambia de verdad - no en cada sesión.

## 1. Datos del equipo

- Nombre del equipo:
- Sección:
- Repositorio (URL):
- Topics del repositorio configurados (sí/no) - incluye `grupo-<numero>-<nombre-proyecto>` (ver [Guía del Proyecto Sello](index.md), sección 5):

**Integrantes:**

| Integrante | Rol o énfasis previsto (ej. batch/Spark, streaming/Kafka, observabilidad, BI/ML) |
|---|---|
| | |
| | |
| | |
| | |

Este rol es un **énfasis de coordinación a nivel de equipo** (quién profundiza más en X aspecto compartido, como la observabilidad común o el empaquetado DataOps), no un reemplazo del trabajo individual: cada integrante igual construye de extremo a extremo sus dos dimensiones - U1 y U2 (sección 3) -, sin importar el rol que tenga aquí.

## 2. Dominio del proyecto

- Nombre del proyecto:
- Problema o necesidad que resuelve (2-4 líneas):
- Dominio de datos (breve - sector o rubro, ej. logística, salud, retail, IoT industrial, finanzas, y qué se mide o registra):
- **Pregunta central de negocio** que responde el sistema completo (1-2 líneas, redactada como pregunta - ej. "¿cómo optimizar la operación logística de reparto de la empresa X?"). Es el hilo conductor único del proyecto: en la sección 3, cada integrante no trae su propia pregunta independiente, trae **dos dimensiones** de esta misma pregunta central (U1 y U2), y todas convergen en un solo tablero/historia final, no en tableros sueltos:
- Usuarios / actores principales (quién consume las salidas del sistema - analista, gerente, otro sistema automatizado, etc.):
- Arquitectura Big Data prevista - Lambda o Kappa, y por qué (justificación breve, criterio de decisión en S1, 2.5.1):
- Fuente de datos batch (dataset real o realista - público, de datos abiertos de gobierno, o real de una organización; no completamente inventado sin fundamento). Indica cuál y de dónde:
- Fuente de eventos en tiempo real (empresariales y/o IoT/sensores; puede ser simulada de forma realista - a diferencia del dataset batch, aquí sí es aceptable simular, como hace `lambda26` con sensores). Indica cuál:

  La fuente batch y la fuente streaming **no tienen que ser datasets distintos** - pueden ser el mismo sensor u origen, visto en dos momentos: streaming es la lectura en vivo, tal como llega (ej. un sensor atmosférico publicando cada pocos segundos su campo eléctrico, campo magnético, temperatura, humedad, viento, lluvia); batch es la acumulación histórica de esas mismas lecturas, procesada después en bloque con Spark (limpieza, agregación diaria/mensual, formato analítico). Esto es, de hecho, el patrón típico de arquitectura Lambda: una sola fuente alimentando la capa de velocidad (streaming) y la capa batch a la vez.

- ¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo? Puede ser un proyecto propio del equipo, o el de **otro grupo** que ya llevó Big Data en un ciclo anterior, si es pertinente darle continuidad. Si continúa, indicar cuál y de qué equipo/ciclo:

## 3. Dimensiones de análisis y fuentes previstas

**Regla de asignación:** cada integrante propone (o hereda) **dos dimensiones** de la pregunta central de negocio declarada en la sección 2 - no una pregunta independiente propia, sino dos ángulos de esa misma pregunta. Todas las dimensiones del equipo deben converger en **un solo tablero o historia final** - no en tableros sueltos por integrante. Esto evita lo que el Proyecto Sello marca como inválido: "notebooks o jobs Spark aislados sin un flujo de datos común".

Esta estructura es la misma **matriz de operacionalización de variables** de metodología de la investigación, aplicada al proyecto: la pregunta central contiene la(s) **variable(s)** de interés (ej. "operación logística", "condiciones climáticas de riesgo"); cada **dimensión** es un aspecto de esa variable; cada dimensión se mide con uno o más **indicadores** (la métrica concreta, la que termina siendo el KPI); y cada indicador se recoge con un **instrumento** - que aquí es, literalmente, la fuente batch o streaming de la ficha (el sensor, el evento, el dataset).

Siguiendo el sílabo tal cual está organizado por unidad, **las dos dimensiones son descriptiva/diagnóstica + predictiva** - ninguna se queda solo en lo descriptivo. Lo que las distingue no es si predicen o no, es si esa predicción corre **en tiempo real**:

- **Dimensión U1 - descriptiva/diagnóstica + predictiva, sin inferencia en tiempo real** (¿qué pasó?, ¿cómo se compara?, y también ¿qué se puede esperar, en general?): se responde con **batch** - histórico, para describir el pasado y, sobre ese mismo histórico, entrenar un modelo predictivo (regresión, clasificación - equivalente a Spark MLlib, S4). La predicción existe, pero es una foto fija: se calcula al correr el pipeline, no se actualiza dato por dato ni reacciona a lo que llega después. Se reporta dentro del propio **notebook Jupyter/PySpark** donde corre el pipeline (tablas, gráficos con pandas/matplotlib, la predicción del modelo incluida) - es el medio ya establecido para esta unidad (S1-S4), no hace falta llevarla a Grafana.
- **Dimensión U2 - descriptiva/diagnóstica + predictiva, con inferencia de series de tiempo en tiempo real** (¿qué está pasando ahora?, y además ¿qué va a pasar?, ¿cuándo?): se responde con **streaming** - el indicador describe el estado en vivo, dato por dato (equivalente a la ingesta de eventos empresariales/IoT en `lambda26`, S6-S9), y sobre ese mismo flujo corre el modelo de series de tiempo: hay que **entrenarlo y dejarlo corriendo en Spark Structured Streaming**, aplicando la inferencia a cada dato que llega - la predicción se actualiza en vivo, junto con el indicador, no es una foto fija. Se reporta en vivo en **Grafana** - indicador y predicción actualizándose juntos, dato por dato; si el caso lo amerita, incluye alertas (ej. temperatura fuera de un rango normal).

Ambas dimensiones pueden venir del **mismo origen físico** (ver sección 2) - ej. un sensor atmosférico: U2 usa la lectura en vivo (temperatura, humedad, campo eléctrico, tal como llega), U1 usa la acumulación histórica de esas mismas variables, procesada por lotes. No es obligatorio que sean fuentes distintas - sí es obligatorio que el tratamiento (streaming vs. batch, y con o sin inferencia en tiempo real) sea real y distinto entre las dos.

Esto es, **a nivel de cada integrante** (el curso evalúa [aporte individual](index.md#subaspectos-de-la-sustentacion-integral) - subaspecto 4 de la sustentación integral -, con evidencia de U1 y U2 por separado según el [alineamiento por sesiones](index.md#alineamiento-por-sesiones)), lo que garantiza que todos, sin excepción, construyan y defiendan ambas capas del curso - no solo que "alguien del equipo" haya tocado streaming.

**Ejemplo de una pregunta central desglosada en dimensiones** (pregunta central: "¿cómo optimizar la operación logística de reparto de la empresa X?"):

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| A | Predictiva (U1, batch) | ¿Cuál es el tiempo de entrega promedio por zona, y qué tiempo se puede esperar según distancia y franja horaria? | Minutos promedio por zona; minutos estimados por un modelo de regresión (distancia, franja horaria) | Histórico de tiempos de entrega por zona | Eventos de entrega en vivo (mismo origen que U2, acumulados aquí - no se procesan en vivo) |
| A | Predictiva (U2, streaming) | ¿Cuál será el tiempo de entrega esperado en la próxima hora, por zona, según la tendencia actual? | Minutos de entrega proyectados, por zona | (mismo histórico, para entrenar) | Eventos de entrega en vivo |
| B | Predictiva (U1, batch) | ¿Qué porcentaje de recorridos se desvió de la ruta planificada, y qué unidades tienen mayor probabilidad de desviarse según su perfil? | % de desvío por unidad y por mes; probabilidad de desvío estimada por un modelo de clasificación | Histórico de recorridos vs. rutas planificadas | Ubicaciones GPS en vivo (mismo origen que U2, acumuladas aquí - no se procesan en vivo) |
| B | Predictiva (U2, streaming) | ¿Qué unidades es probable que lleguen tarde en los próximos 30 minutos, según su velocidad y trayectoria? | Probabilidad de retraso, por unidad | (mismo histórico, para entrenar) | Ubicaciones y velocidad GPS en vivo |
| C | Predictiva (U1, batch) | ¿Cuál fue la demanda histórica de pedidos por zona y por mes, y qué demanda se puede esperar el próximo mes según la tendencia? | Número de pedidos por zona y por mes; pedidos proyectados por un modelo de regresión sobre el histórico mensual | Histórico de pedidos por zona | Pedidos en vivo (mismo origen que U2, acumulados aquí - no se procesan en vivo) |
| C | Predictiva (U2, streaming) | ¿Cuál es la demanda esperada de pedidos por zona la próxima semana? | Número de pedidos proyectados, por zona y por día | (mismo histórico, para entrenar el modelo) | Pedidos en vivo (para contrastar contra lo predicho) |

Seis dimensiones, tres integrantes, todas responden a la **misma** operación logística - juntas arman un solo tablero de "salud de la operación de reparto", no proyectos sueltos. Cada integrante tiene su par completo: una predictiva sin tiempo real (U1, batch) y una predictiva con tiempo real (U2, streaming), ambas sobre el mismo ángulo de la pregunta central.

**Otro ejemplo, con sensores atmosféricos** (pregunta central: "¿cómo anticipar condiciones climáticas de riesgo en la zona monitoreada?"):

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| A | Predictiva (U1, batch) | ¿Cuál fue la temperatura mínima y máxima registrada por mes, y qué temperatura se puede esperar el próximo mes según la tendencia histórica? | Temperatura mín./máx. en °C, por mes; temperatura proyectada por un modelo de regresión sobre el histórico mensual | Histórico de temperatura de la estación | Lecturas en vivo del sensor de temperatura (mismo origen que U2, acumuladas aquí - no se procesan en vivo) |
| A | Predictiva (U2, streaming) | ¿Cuándo se espera que la temperatura supere el umbral de riesgo (helada o calor extremo)? | Temperatura proyectada en °C, próximas horas | (mismo histórico, para entrenar) | Lecturas en vivo del sensor de temperatura |
| B | Predictiva (U1, batch) | ¿Con qué frecuencia ocurrieron tormentas eléctricas en la zona, y qué condiciones históricas se asocian a mayor probabilidad de tormenta? | Número de tormentas por temporada; probabilidad estimada por un modelo de clasificación sobre condiciones históricas | Histórico de campo eléctrico y magnético asociado a tormentas pasadas | Lecturas en vivo de campo eléctrico y magnético (mismo origen que U2, acumuladas aquí - no se procesan en vivo) |
| B | Predictiva (U2, streaming) | ¿Qué tan probable es una tormenta eléctrica en las próximas horas? | Probabilidad de tormenta, próximas horas | (mismo histórico, para entrenar) | Lecturas en vivo de campo eléctrico y magnético |
| C | Predictiva (U1, batch) | ¿Cuál fue el riesgo de inundación según la lluvia acumulada por temporada, y qué riesgo se puede esperar la próxima temporada según la tendencia? | Milímetros acumulados por temporada; riesgo proyectado por un modelo de regresión sobre el histórico | Histórico de precipitación acumulada por temporada | Lecturas en vivo de lluvia (mismo origen que U2, acumuladas aquí - no se procesan en vivo) |
| C | Predictiva (U2, streaming) | ¿Cuánta lluvia se espera en las próximas horas, según la tendencia actual? | Milímetros de lluvia proyectados, próximas 3-6h | (mismo histórico, para entrenar) | Lecturas de lluvia en vivo |

Aquí las dimensiones pueden salir del **mismo sensor físico** (distintas variables que mide a la vez) - lo que las conecta no es el sensor, es que todas alimentan la misma pregunta central: el riesgo climático de la zona, visible en un solo tablero con paneles de temperatura, tormenta, inundación y su pronóstico.

**Tabla de asignación:**

| Integrante | Tipo (Predictiva U1/batch o Predictiva U2/streaming) | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| | Predictiva (U1, batch) | | | | |
| | Predictiva (U2, streaming) | | | | |
| | Predictiva (U1, batch) | | | | |
| | Predictiva (U2, streaming) | | | | |

Cada integrante ocupa dos filas seguidas - su dimensión U1 y su dimensión U2 - no una fila suelta.

En ninguna de las dos dimensiones alcanza con el indicador solo - el modelo predictivo (visto arriba) es obligatorio en ambas. Los paneles de **todas** las dimensiones U2 del equipo se muestran en el **mismo Grafana**, no en instancias separadas por integrante - el resultado visible en tiempo real del proyecto es un solo tablero con varios paneles, uno por dimensión U2; las dimensiones U1 se documentan en sus respectivos notebooks.

**Ficha por dimensión** - completa un bloque como este por cada fila de la tabla anterior (dos bloques por integrante: uno para su dimensión U1, otro para su dimensión U2):

### Dimensión: ______ (integrante: ______)

- Tipo: predictiva sin tiempo real (U1, batch) o predictiva con tiempo real (U2, streaming):
- Dimensión, redactada como pregunta completa (1-2 líneas), y cómo se relaciona con la pregunta central de la sección 2:
- Indicador(es) - la métrica concreta que responde a la dimensión (la que se convierte en el KPI del tablero), incluido el valor que entrega el modelo predictivo:
- Decisión o acción que habilita (qué hace alguien distinto gracias a esta respuesta):
- Instrumento/fuente batch - descripción, esquema (campos/columnas clave) y origen (dataset público/URL, API, exporte periódico, etc.):
- Instrumento/fuente streaming - descripción, esquema y origen (tópico Kafka, sensor simulado, evento de negocio, etc.); en la dimensión U1, si tu histórico es la acumulación del mismo streaming que usa tu U2, indícalo igual aquí (mismo instrumento, solo que aquí no se procesa en vivo); si tu fuente batch es un origen totalmente aparte (ej. un dataset público sin componente en vivo), escribe "no aplica":
- Modelo predictivo - tipo (regresión/clasificación para U1; series de tiempo para U2) y si corre una sola vez (U1) o en vivo sobre el flujo (U2):
- Salida: si es U1, cómo se reporta en el notebook (tablas/gráficos, predicción incluida); si es U2, el panel de KPI y de predicción en el Grafana común, ambos en vivo - describe lo que corresponda a tu tipo:
- ¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final? (todas deben aportar a la misma historia, no quedar sueltas). Si todavía no lo sabes, escribe "por definir".
- Lista inicial de requisitos (mínimo 3, redactados como "el sistema debe..."):
    1.
    2.
    3.

### Dimensión: ______ (integrante: ______)

- Tipo: predictiva sin tiempo real (U1) o predictiva con tiempo real (U2):
- Dimensión, redactada como pregunta completa, y relación con la pregunta central:
- Indicador(es), incluido el valor del modelo predictivo:
- Decisión o acción que habilita:
- Instrumento/fuente batch:
- Instrumento/fuente streaming:
- Modelo predictivo (tipo y si corre una vez o en vivo):
- Salida (notebook si es U1; Grafana en vivo si es U2):
- ¿Con qué otra(s) dimensión(es) del equipo se combina?
- Lista inicial de requisitos:
    1.
    2.
    3.

*(repite este bloque por cada fila restante de la tabla, hasta cubrirla completa - dos bloques por integrante)*

- Qué SÍ cubre este proyecto en conjunto:
- Qué NO cubre - fuera de alcance, explícito:

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana **y estimación de costos operacionales**, prácticas de DataOps) sigue el mismo patrón que enseña cada sesión sobre `lambda26` (S8-S9) - no se declara aquí porque aplica igual sin importar el dominio de cada equipo; se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- Docente:
- Fecha:

