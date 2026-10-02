import json
import os
import re
import subprocess
import time
import uuid

import paho.mqtt.client as mqtt
import psutil


# El agente corre en Windows (fuera de Docker): Docker Desktop no ve las NIC
# físicas del host, solo su propia red virtual.
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "41883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/red/llsw3/telemetria")
INTERVAL_MS = int(os.getenv("AGENTE_INTERVAL_MS", "1000"))
INTERFACES = [i.strip() for i in os.getenv("AGENTE_INTERFACES", "").split(",") if i.strip()]

EXCLUIDAS = ("loopback", "teredo", "isatap")


def adaptadores_virtuales():
    # Windows sabe qué adaptador es hardware real (Get-NetAdapter -> Virtual);
    # el nombre no basta: "Ethernet 3" puede ser un adaptador de VirtualBox.
    try:
        salida = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-NetAdapter | Select-Object Name, Virtual | ConvertTo-Json"],
            capture_output=True, text=True, timeout=15,
        ).stdout
        datos = json.loads(salida)
        datos = datos if isinstance(datos, list) else [datos]
        return {d["Name"] for d in datos if d.get("Virtual")}
    except (OSError, ValueError, subprocess.SubprocessError):
        return set()


VIRTUALES = adaptadores_virtuales()


def sensor_id(interfaz):
    # "Wi-Fi 2" -> "llsw3-wi-fi-2"; la key de Kafka debe ser estable y sin espacios
    return "llsw3-" + re.sub(r"[^a-z0-9]+", "-", interfaz.lower()).strip("-")


def tipo_interfaz(interfaz):
    return "virtual" if interfaz in VIRTUALES or interfaz.lower().startswith("vethernet") else "fisica"


def interfaces_activas():
    stats = psutil.net_if_stats()
    activas = []
    for nombre, st in stats.items():
        if INTERFACES and nombre not in INTERFACES:
            continue
        if not st.isup or any(e in nombre.lower() for e in EXCLUIDAS):
            continue
        activas.append(nombre)
    return activas


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"agente-red-{uuid.uuid4().hex[:8]}")
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_start()

print(json.dumps({
    "service": "uso-red",
    "component": "agente",
    "mqttHost": MQTT_HOST,
    "mqttPort": MQTT_PORT,
    "mqttTopic": MQTT_TOPIC,
    "intervalMs": INTERVAL_MS,
    "interfaces": interfaces_activas(),
    "status": "connected",
}), flush=True)

# Última lectura de cada interfaz mientras estuvo activa. Si el adaptador se
# deshabilita, su entrada no se toca: al volver, el contador del sistema
# arrancó de cero y la diferencia sale negativa (reinicio real del contador).
anterior = {}

while True:
    # nowrap=False: contador crudo del sistema. Con el valor por defecto (True)
    # psutil "compensa" en memoria cualquier contador que baje y el reinicio
    # real del adaptador quedaría oculto.
    actual = psutil.net_io_counters(pernic=True, nowrap=False)
    t_actual = time.monotonic()
    stats = psutil.net_if_stats()

    for interfaz in interfaces_activas():
        if interfaz not in actual:
            continue
        b = actual[interfaz]
        if interfaz not in anterior:
            anterior[interfaz] = (b, t_actual)
            continue
        a, t_anterior = anterior[interfaz]
        anterior[interfaz] = (b, t_actual)
        dt = t_actual - t_anterior
        # Tasas por segundo SIN recortar: si el contador del sistema se reinicia
        # (adaptador deshabilitado/rehabilitado), la tasa sale negativa y es el
        # consumer quien la marca como alerta de rango físico.
        data = {
            "tipoEvento": "red.lectura",
            "sensorId": sensor_id(interfaz),
            "interfaz": interfaz,
            "tipoInterfaz": tipo_interfaz(interfaz),
            "velocidadEnlaceMbps": stats[interfaz].speed,
            "bytesRecibidosSeg": round((b.bytes_recv - a.bytes_recv) / dt, 1),
            "bytesEnviadosSeg": round((b.bytes_sent - a.bytes_sent) / dt, 1),
            "paquetesRecibidosSeg": round((b.packets_recv - a.packets_recv) / dt, 1),
            "paquetesEnviadosSeg": round((b.packets_sent - a.packets_sent) / dt, 1),
            "erroresEntrada": b.errin - a.errin,
            "erroresSalida": b.errout - a.errout,
            "descartesEntrada": b.dropin - a.dropin,
            "descartesSalida": b.dropout - a.dropout,
            "intervaloMs": round(dt * 1000),
            "origen": "agente-red",
            "timestamp": int(time.time() * 1000),
        }
        info = client.publish(MQTT_TOPIC, json.dumps(data))
        print(json.dumps({
            "service": "uso-red",
            "component": "agente",
            "sensorId": data["sensorId"],
            "tipoInterfaz": data["tipoInterfaz"],
            "velocidadEnlaceMbps": data["velocidadEnlaceMbps"],
            "bytesRecibidosSeg": data["bytesRecibidosSeg"],
            "bytesEnviadosSeg": data["bytesEnviadosSeg"],
            "status": "published" if info.rc == mqtt.MQTT_ERR_SUCCESS else f"error:{info.rc}",
        }), flush=True)

    time.sleep(INTERVAL_MS / 1000)
