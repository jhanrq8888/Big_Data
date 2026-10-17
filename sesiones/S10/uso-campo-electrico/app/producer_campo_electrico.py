import json
import os
import random
import time

from kafka import KafkaProducer


TOPIC_CAMPO_ELECTRICO = os.getenv("KAFKA_TOPIC_CAMPO_ELECTRICO", "campo-electrico-eventos")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INTERVAL_MS = int(os.getenv("ESTACION_INTERVAL_MS", "3000"))
ESTACION_IDS = os.getenv("ESTACION_IDS", "estacion-ns-01").split(",")

# Rangos y paso máximo calibrados sobre las estadísticas reales del Data
# Lake Gold de S04 (campo_electrico_particionado): min/max observados por
# variable sobre 92,847 mediciones reales.
RANGOS = {
    "CE": (-6.74, 2.85),
    "CM": (24020.0, 24498.4),
    "TempOut": (11.8, 27.2),
    "OutHum": (61.0, 92.0),
    "WindSpeed": (0.0, 17.7),
    "Bar": (945.3, 953.7),
    "SolarRad": (0.0, 770.0),
    "UVIndex": (0.0, 6.3),
}
PASO_MAXIMO = {
    "CE": 0.08,
    "CM": 1.5,
    "TempOut": 0.1,
    "OutHum": 0.4,
    "WindSpeed": 0.3,
    "Bar": 0.1,
    "SolarRad": 8.0,
    "UVIndex": 0.15,
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(json.dumps({
    "service": "uso-campo-electrico",
    "component": "producer",
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "estacionIds": ESTACION_IDS,
    "intervalMs": INTERVAL_MS,
    "status": "connected",
}))

# Misma idea que uso-atmos: una estación real no salta de golpe entre
# mediciones consecutivas de un minuto a otro; cada ronda avanza un poco
# desde el valor anterior (caminata aleatoria acotada a los rangos reales
# observados en el Data Lake de S04), no un número independiente del previo.
estado = {
    estacion_id: {
        variable: round(random.uniform(*rango), 2)
        for variable, rango in RANGOS.items()
    }
    for estacion_id in ESTACION_IDS
}


def siguiente_valor(valor_actual, variable):
    minimo, maximo = RANGOS[variable]
    paso = PASO_MAXIMO[variable]
    nuevo = valor_actual + random.uniform(-paso, paso)
    return round(min(max(nuevo, minimo), maximo), 2)


while True:
    for estacion_id in ESTACION_IDS:
        lectura = estado[estacion_id]
        for variable in RANGOS:
            lectura[variable] = siguiente_valor(lectura[variable], variable)

        data = {
            "tipoEvento": "estacion.lectura",
            "estacionId": estacion_id,
            "CE": lectura["CE"],
            "CM": lectura["CM"],
            "TempOut": lectura["TempOut"],
            "OutHum": lectura["OutHum"],
            "WindSpeed": lectura["WindSpeed"],
            "Bar": lectura["Bar"],
            "SolarRad": lectura["SolarRad"],
            "UVIndex": lectura["UVIndex"],
            "origen": "uso-campo-electrico",
            "timestamp": int(time.time() * 1000),
        }

        metadata = producer.send(TOPIC_CAMPO_ELECTRICO, key=estacion_id, value=data).get(timeout=10)

        log = {
            "service": "uso-campo-electrico",
            "component": "producer",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "eventType": data["tipoEvento"],
            "estacionId": estacion_id,
            "timestamp": data["timestamp"],
            "status": "published",
        }

        print(json.dumps(log))

    time.sleep(INTERVAL_MS / 1000)
