import json
import os
import time

from kafka import KafkaConsumer


TOPIC_RED = os.getenv("KAFKA_TOPIC_RED", "red-telemetria")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uso-red-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
UMBRAL_SATURACION_PCT = float(os.getenv("UMBRAL_SATURACION_PCT", "70"))

# 1 Mbps = 125 000 bytes/s. Se tolera un 5 % por encima de la velocidad
# nominal: el intervalo real de muestreo nunca es exactamente 1 s.
BYTES_POR_MBPS = 125_000
TOLERANCIA = 1.05

CAMPOS = {
    "tipoEvento": str,
    "sensorId": str,
    "interfaz": str,
    "tipoInterfaz": str,
    "velocidadEnlaceMbps": (int, float),
    "bytesRecibidosSeg": (int, float),
    "bytesEnviadosSeg": (int, float),
    "paquetesRecibidosSeg": (int, float),
    "paquetesEnviadosSeg": (int, float),
    "erroresEntrada": int,
    "erroresSalida": int,
    "descartesEntrada": int,
    "descartesSalida": int,
    "timestamp": int,
}
TASAS = ("bytesRecibidosSeg", "bytesEnviadosSeg", "paquetesRecibidosSeg", "paquetesEnviadosSeg")


def validar_esquema(raw):
    """Falla de forma: no es JSON, no es objeto, falta un campo o tiene otro tipo."""
    try:
        event = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as ex:
        return None, f"no es JSON: {ex}"
    if not isinstance(event, dict):
        return None, "no es un objeto JSON"
    for campo, tipo in CAMPOS.items():
        valor = event.get(campo)
        if valor is None:
            return event, f"falta el campo {campo}"
        # bool es subclase de int en Python: no debe pasar como número
        if isinstance(valor, bool) or not isinstance(valor, tipo):
            return event, f"tipo inválido en {campo}"
    return event, None


def validar_rango(event):
    """Falla física: el valor tiene la forma correcta pero es imposible."""
    negativas = [t for t in TASAS if event[t] < 0]
    if negativas:
        return f"tasa negativa en {','.join(negativas)} (contador reiniciado)"
    velocidad = event["velocidadEnlaceMbps"]
    if velocidad > 0:
        capacidad = velocidad * BYTES_POR_MBPS * TOLERANCIA
        pico = max(event["bytesRecibidosSeg"], event["bytesEnviadosSeg"])
        if pico > capacidad:
            return f"{pico:.0f} B/s supera la capacidad del enlace ({velocidad} Mbps)"
    return None


def uso_enlace_pct(event):
    velocidad = event["velocidadEnlaceMbps"]
    if velocidad <= 0:
        return 0.0
    pico = max(event["bytesRecibidosSeg"], event["bytesEnviadosSeg"])
    return round(pico / (velocidad * BYTES_POR_MBPS) * 100, 2)


consumer = KafkaConsumer(
    TOPIC_RED,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
)

print(json.dumps({
    "service": "uso-red",
    "component": "consumer",
    "topic": TOPIC_RED,
    "groupId": GROUP_ID,
    "umbralSaturacionPct": UMBRAL_SATURACION_PCT,
    "status": "listening",
}), flush=True)

for msg in consumer:
    processed_at = int(time.time() * 1000)
    event, error_esquema = validar_esquema(msg.value)
    uso = None
    motivo = None

    if error_esquema:
        status, motivo = "invalid", error_esquema
    else:
        motivo = validar_rango(event)
        uso = uso_enlace_pct(event)
        if motivo:
            status = "alerta"
        elif uso > UMBRAL_SATURACION_PCT:
            status, motivo = "saturacion", f"uso del enlace {uso}% > {UMBRAL_SATURACION_PCT}%"
        else:
            status = "consumed"

    event = event if isinstance(event, dict) else {}
    timestamp = event.get("timestamp") if isinstance(event.get("timestamp"), int) else None

    print(json.dumps({
        "service": "uso-red",
        "component": "consumer",
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "key": msg.key,
        "sensorId": event.get("sensorId"),
        "tipoInterfaz": event.get("tipoInterfaz"),
        "velocidadEnlaceMbps": event.get("velocidadEnlaceMbps"),
        "bytesRecibidosSeg": event.get("bytesRecibidosSeg"),
        "bytesEnviadosSeg": event.get("bytesEnviadosSeg"),
        "usoEnlacePct": uso,
        "latencyMs": processed_at - timestamp if timestamp else None,
        "motivo": motivo,
        "rawPayload": None if status != "invalid" else msg.value.decode("utf-8", "replace"),
        "status": status,
    }), flush=True)
