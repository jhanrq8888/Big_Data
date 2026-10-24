"""Crea en Grafana (admin/admin, credenciales de prueba de obs/compose.yml) los tableros BI/ML de S11."""
import base64
import json
import urllib.request

G = "http://localhost:43000"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()


def api(metodo, ruta, cuerpo=None):
    req = urllib.request.Request(G + ruta, method=metodo, data=json.dumps(cuerpo).encode() if cuerpo is not None else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        return {"error": e.code, "body": e.read().decode()[:300]}


ds = [d for d in api("GET", "/api/datasources") if d["type"] == "prometheus"][0]
DS = {"type": "prometheus", "uid": ds["uid"]}


def panel(pid, titulo, tipo, consultas, x, y, w, h, unidad=None, decimales=None):
    targets = [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": l, "instant": tipo == "stat", "range": tipo != "stat"}
               for i, (e, l) in enumerate(consultas)]
    defaults = {}
    if unidad:
        defaults["unit"] = unidad
    if decimales is not None:
        defaults["decimals"] = decimales
    p = {"id": pid, "title": titulo, "type": tipo, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": targets, "fieldConfig": {"defaults": defaults, "overrides": []}, "options": {}}
    if tipo == "timeseries":
        defaults["custom"] = {"lineWidth": 2, "showPoints": "always", "pointSize": 4}
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}}
    return p


guia = [
    panel(1, "Eventos procesados (batch)", "stat", [("lambda26_bi_eventos_total", "")], 0, 0, 6, 5),
    panel(2, "Ratio de eventos válidos (batch)", "stat", [("lambda26_bi_eventos_validos_ratio", "")], 6, 0, 6, 5, "percentunit", 1),
    panel(3, "Latencia promedio (batch)", "stat", [("lambda26_bi_latencia_promedio_ms", "")], 12, 0, 6, 5, "ms"),
    panel(4, "Kafka brokers (infraestructura S9)", "stat", [("kafka_brokers", "")], 18, 0, 6, 5),
    panel(5, "CE real vs. predicción (en vivo)", "timeseries",
          [("lambda26_ml_ce_real", "CE real"), ("lambda26_ml_prediccion_siguiente", "Predicción siguiente")], 0, 5, 24, 11),
]
proy = [
    panel(1, "Lecturas procesadas (batch)", "stat", [("llsw3_bi_lecturas_total", "")], 0, 0, 4, 5),
    panel(2, "Lecturas válidas (batch)", "stat", [("llsw3_bi_lecturas_validas_ratio", "")], 4, 0, 4, 5, "percentunit", 1),
    panel(3, "Latencia promedio (batch)", "stat", [("llsw3_bi_latencia_promedio_ms", "")], 8, 0, 4, 5, "ms"),
    panel(4, "Tráfico recibido promedio (batch)", "stat", [("llsw3_bi_rx_promedio_bytes", "")], 12, 0, 4, 5, "Bps"),
    panel(5, "Pico de tráfico recibido (batch)", "stat", [("llsw3_bi_rx_pico_bytes", "")], 16, 0, 4, 5, "Bps"),
    panel(6, "Ingesta red-telemetria (infra)", "stat", [('sum(rate(kafka_topic_partition_current_offset{topic="red-telemetria"}[1m]))', "")], 20, 0, 4, 5, "reqps"),
    panel(7, "Tráfico recibido real vs. predicción t+1 (en vivo)", "timeseries",
          [("llsw3_ml_rx_real", "real ({{instance}})"), ("llsw3_ml_rx_prediccion_siguiente", "predicción t+1 ({{instance}})")], 0, 5, 16, 11, "Bps"),
    panel(8, "Error absoluto de la predicción anterior (en vivo)", "timeseries",
          [("llsw3_ml_error_abs_vivo", "error ({{instance}})")], 16, 5, 8, 11, "Bps"),
]

for uid, titulo, paneles in [("s11-bi-ml", "S11 - BI/ML distribuido", guia),
                             ("llsw3-s11-bi-ml", "S11 - Proyecto LLSW3: BI/ML tráfico de red", proy)]:
    d = {"dashboard": {"uid": uid, "title": titulo, "panels": paneles, "time": {"from": "now-5m", "to": "now"},
                       "refresh": "5s", "schemaVersion": 39}, "overwrite": True}
    print(titulo, "->", api("POST", "/api/dashboards/db", d).get("url"))
