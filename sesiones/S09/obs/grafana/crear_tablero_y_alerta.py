"""Crea en Grafana (admin/admin) el tablero del Proyecto Sello y la alerta 'Agente de red sin datos'."""
import base64
import json
import urllib.request

G = "http://localhost:43000"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()


def api(metodo, ruta, cuerpo=None):
    req = urllib.request.Request(G + ruta, method=metodo, data=json.dumps(cuerpo).encode() if cuerpo is not None else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json", "X-Disable-Provenance": "true"})
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        return {"error": e.code, "body": e.read().decode()[:300]}


ds = [d for d in api("GET", "/api/datasources") if d["type"] == "prometheus"][0]
DS = {"type": "prometheus", "uid": ds["uid"]}


def panel(pid, titulo, tipo, expr, x, y, w, h, leyenda=None, unidad=None, extra=None):
    p = {"id": pid, "title": titulo, "type": tipo, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"refId": "A", "datasource": DS, "expr": expr, "legendFormat": leyenda or "", "instant": tipo in ("stat", "table")}],
         "fieldConfig": {"defaults": {"unit": unidad} if unidad else {}, "overrides": []}, "options": {}}
    if extra:
        p.update(extra)
    return p


panels = [
    panel(1, "Kafka Brokers", "stat", "kafka_brokers", 0, 0, 4, 5),
    panel(2, "Kafka Exporter Up", "stat", 'up{job="kafka-exporter"}', 4, 0, 4, 5),
    panel(3, "Ingesta red-telemetria (lecturas/s)", "stat", 'sum(rate(kafka_topic_partition_current_offset{topic="red-telemetria"}[1m]))', 8, 0, 5, 5, unidad="reqps"),
    panel(4, "Lag uso-red-group (actual)", "stat", 'sum(kafka_consumergroup_lag{consumergroup="uso-red-group"})', 13, 0, 5, 5),
    panel(5, "Mensajes en red-telemetria", "stat", 'sum(kafka_topic_partition_current_offset{topic="red-telemetria"})', 18, 0, 6, 5),
    panel(6, "Ingesta red-telemetria por partición (lecturas/s)", "timeseries", 'sum by (partition) (rate(kafka_topic_partition_current_offset{topic="red-telemetria"}[1m]))', 0, 5, 12, 9, leyenda="partición {{partition}}", unidad="reqps"),
    panel(7, "Consumer lag uso-red-group", "timeseries", 'sum by (consumergroup) (kafka_consumergroup_lag{consumergroup="uso-red-group"})', 12, 5, 12, 9, leyenda="{{consumergroup}}"),
]

dash = {"dashboard": {"uid": "llsw3-red-obs", "title": "S9 - Proyecto LLSW3: pipeline red-telemetria", "panels": panels,
                      "time": {"from": "now-30m", "to": "now"}, "refresh": "10s", "schemaVersion": 39},
        "overwrite": True}
print("dashboard:", api("POST", "/api/dashboards/db", dash))

carpeta = api("POST", "/api/folders", {"uid": "llsw3", "title": "LLSW3"})
print("carpeta:", carpeta.get("uid") or carpeta)

regla = {
    "uid": "llsw3-agente-sin-datos", "title": "Agente de red sin datos", "ruleGroup": "red-telemetria", "folderUID": "llsw3",
    "condition": "C", "for": "1m", "noDataState": "NoData", "execErrState": "Error",
    "annotations": {"summary": "red-telemetria no recibe lecturas del agente del nodo de captura (ingesta = 0)",
                    "__dashboardUid__": "llsw3-red-obs", "__panelId__": "3"},
    "labels": {"equipo": "LLSW3"},
    "data": [
        {"refId": "A", "datasourceUid": ds["uid"], "relativeTimeRange": {"from": 300, "to": 0},
         "model": {"refId": "A", "expr": 'sum(rate(kafka_topic_partition_current_offset{topic="red-telemetria"}[1m]))', "instant": True}},
        {"refId": "B", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
         "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last"}},
        {"refId": "C", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
         "model": {"refId": "C", "type": "threshold", "expression": "B", "conditions": [{"evaluator": {"type": "lt", "params": [0.01]}}]}},
    ],
}
existe = api("GET", "/api/v1/provisioning/alert-rules/llsw3-agente-sin-datos")
print("regla:", api("PUT" if "uid" in existe else "POST", "/api/v1/provisioning/alert-rules" + ("/llsw3-agente-sin-datos" if "uid" in existe else ""), regla).get("uid"))
print("grupo:", api("PUT", "/api/v1/provisioning/folder/llsw3/rule-groups/red-telemetria", {"title": "red-telemetria", "interval": 30, "rules": [regla]}).get("title"))
