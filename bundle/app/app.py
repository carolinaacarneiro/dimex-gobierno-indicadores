"""App de revisión de fichas de indicadores (Databricks Apps + Streamlit).

Los revisores (Data Owners, Data Stewards, Gobierno de Datos) resuelven los hallazgos que detectó la solución:
duplicados, conflictos, fichas incompletas y dominios a confirmar. Cada decisión queda registrada con el usuario
y la fecha en la tabla <prefijo>_gold.decisiones_revision.

Documentación: https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-apps/
"""
import os
import uuid

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOGO = os.environ["CATALOGO"]
PREFIJO = os.environ["PREFIJO_ESQUEMAS"]
GOLD = f"`{CATALOGO}`.`{PREFIJO}_gold`"
SILVER = f"`{CATALOGO}`.`{PREFIJO}_silver`"

DECISIONES = {
    "DUPLICADO": {"UNIFICAR": "Unificar: conservar la ficha oficial y retirar la otra (su nombre queda como sinónimo)",
                  "MANTENER_AMBAS": "No son duplicados: mantener ambas fichas"},
    "CONFLICTO": {"DEFINIR_OFICIAL": "Definir la ficha oficial y retirar la otra",
                  "MANTENER_AMBAS": "Son indicadores distintos: mantener ambos (se recomienda renombrar)",
                  "ESCALAR_A_COMITE": "Escalar al Comité de Datos (queda en revisión)"},
    "INCOMPLETA": {"DEVOLVER_AL_AREA": "Devolver al área para completar la ficha",
                   "ACEPTAR": "Aceptar la ficha como está"},
    "DOMINIO_A_CONFIRMAR": {"ACEPTAR_DOMINIO_SUGERIDO": "Aceptar el dominio sugerido",
                            "MANTENER_DOMINIO_DECLARADO": "Mantener el dominio declarado por el área"},
}
ETIQUETAS = {"DUPLICADO": "Duplicado", "CONFLICTO": "Conflicto", "INCOMPLETA": "Ficha incompleta", "DOMINIO_A_CONFIRMAR": "Dominio a confirmar"}
CAMPOS_COMPARACION = [
    ("nombre", "Nombre"), ("area_responsable", "Área responsable"), ("dominio_declarado", "Dominio declarado"),
    ("dominio_sugerido", "Dominio sugerido (IA)"), ("tipo_indicador", "Tipo"), ("definicion_funcional", "Definición funcional"),
    ("meta_umbral", "Meta / umbral"), ("criterios_exclusion", "Criterios de exclusión"), ("variable_a", "Variable A"),
    ("variable_b", "Variable B"), ("formula", "Fórmula"), ("tabla_origen", "Tabla de origen"), ("unidad_medida", "Unidad"),
    ("data_owner", "Data Owner"), ("data_steward", "Data Steward"),
]

st.set_page_config(page_title="Revisión de indicadores", layout="wide")


@st.cache_resource
def conexion():
    cfg = Config()  # credenciales del service principal de la app
    return sql.connect(server_hostname=cfg.host,
                       http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
                       credentials_provider=lambda: cfg.authenticate)


def consultar(query, params=None):
    with conexion().cursor() as cur:
        cur.execute(query, params or {})
        return cur.fetchall_arrow().to_pandas()


def ejecutar(query, params=None):
    with conexion().cursor() as cur:
        cur.execute(query, params or {})


def usuario():
    # Databricks Apps envía la identidad del usuario que inició sesión en los encabezados de la solicitud
    h = st.context.headers
    return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username")


st.title("Revisión de fichas de indicadores")
st.caption(f"Usuario: {usuario() or 'sin identificar'}")
pestana_cola, pestana_aprobar, pestana_catalogo, pestana_historial = st.tabs(
    ["Cola de revisión", "Aprobar indicadores", "Catálogo publicado", "Historial de decisiones"])


def dominios_de(ids):
    """Dominio resuelto de cada ficha: el del catálogo publicado o, si aún no se publicó, el sugerido por la IA."""
    marcadores = ", ".join(f":f{i}" for i in range(len(ids)))
    params = {f"f{i}": v for i, v in enumerate(ids)}
    try:
        df = consultar(f"SELECT id_indicador, dominio FROM {GOLD}.catalogo_indicadores WHERE id_indicador IN ({marcadores})", params)
        encontrados = dict(zip(df["id_indicador"], df["dominio"]))
    except Exception:
        encontrados = {}
    faltan = [i for i in ids if not encontrados.get(i)]
    if faltan:
        df = consultar(f"SELECT id_indicador, dominio_sugerido FROM {SILVER}.fichas_vigentes WHERE id_indicador IN ({marcadores})", params)
        encontrados.update({k: v for k, v in zip(df["id_indicador"], df["dominio_sugerido"]) if k in faltan})
    return {encontrados.get(i) for i in ids}


def puede_decidir(dominios):
    """Control por dominio: si gold.aprobadores_por_dominio tiene filas para un dominio, solo esos correos deciden en él.
    El usuario debe estar autorizado en TODOS los dominios involucrados. Ante cualquier error, se niega (falla cerrado)."""
    yo = (usuario() or "").lower()
    for dominio in dominios:
        try:
            filas = consultar(f"SELECT lower(correo) AS correo FROM {GOLD}.aprobadores_por_dominio WHERE dominio = :d", {"d": dominio or ""})
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                continue  # control por dominio no configurado: aplica solo el permiso de uso de la app
            return False
        if not filas.empty and yo not in set(filas["correo"]):
            return False
    return True


def mostrar_error(e, contexto):
    """Muestra el error real (por ejemplo, PERMISSION_DENIED) en lugar de ocultarlo."""
    if type(e).__name__ in ("RerunException", "StopException", "ScriptControlException"):
        raise e  # st.rerun() usa excepciones de control: no deben ocultarse
    texto = str(e)
    if "TABLE_OR_VIEW_NOT_FOUND" in texto:
        st.info(f"{contexto}: la tabla todavía no existe. Ejecute primero el job correspondiente.")
    else:
        st.error(f"{contexto}: {texto[:500]}")

with pestana_cola:
  try:
      pendientes = consultar(f"""
          SELECT * FROM {GOLD}.hallazgos_revision WHERE estado = 'PENDIENTE'
          ORDER BY CASE prioridad WHEN 'Alta' THEN 1 WHEN 'Media' THEN 2 ELSE 3 END, tipo_hallazgo, id_ficha""")
      c1, c2, c3, c4 = st.columns(4)
      for col, tipo in zip([c1, c2, c3, c4], ETIQUETAS):
          col.metric(ETIQUETAS[tipo], int((pendientes["tipo_hallazgo"] == tipo).sum()) if len(pendientes) else 0)

      if pendientes.empty:
          st.success("No hay hallazgos pendientes de revisión.")
      else:
          tipos = st.multiselect("Tipo de hallazgo", list(ETIQUETAS), default=list(ETIQUETAS), format_func=ETIQUETAS.get)
          vista = pendientes[pendientes["tipo_hallazgo"].isin(tipos)]
          opciones = {r.id_hallazgo: f"[{r.prioridad}] {ETIQUETAS[r.tipo_hallazgo]} · {r.id_ficha} {r.nombre}"
                      + (f"  ↔  {r.id_ficha_relacionada} {r.nombre_relacionado}" if r.id_ficha_relacionada else "")
                      for r in vista.itertuples()}
          if opciones:
              elegido = st.selectbox("Hallazgo", list(opciones), format_func=opciones.get)
              h = vista[vista["id_hallazgo"] == elegido].iloc[0]

              st.subheader(ETIQUETAS[h["tipo_hallazgo"]])
              st.write(h["justificacion"])
              if isinstance(h["diferencias"], (list, tuple)) or hasattr(h["diferencias"], "__len__"):
                  for dif in list(h["diferencias"] or []):
                      st.markdown(f"- {dif}")
              if h["recomendacion"]:
                  st.info(f"Recomendación: {h['recomendacion']}")

              ids = [h["id_ficha"]] + ([h["id_ficha_relacionada"]] if h["id_ficha_relacionada"] else [])
              fichas = consultar(f"SELECT * FROM {SILVER}.fichas_vigentes WHERE id_indicador IN ({', '.join(f':f{i}' for i in range(len(ids)))})",
                                 {f"f{i}": v for i, v in enumerate(ids)}).set_index("id_indicador")
              tabla = pd.DataFrame({fid: [fichas.loc[fid, c] if fid in fichas.index else None for c, _ in CAMPOS_COMPARACION] for fid in ids},
                                   index=[e for _, e in CAMPOS_COMPARACION])
              st.dataframe(tabla, use_container_width=True)

              with st.form("decision", clear_on_submit=True):
                  opciones_dec = DECISIONES[h["tipo_hallazgo"]]
                  decision = st.radio("Decisión", list(opciones_dec), format_func=opciones_dec.get)
                  oficial = None
                  if h["tipo_hallazgo"] in ("DUPLICADO", "CONFLICTO") and len(ids) == 2:
                      sugerida = h["ficha_sugerida_oficial"] if h["ficha_sugerida_oficial"] in ids else ids[0]
                      oficial = st.radio("Ficha oficial (si unifica o define la oficial)", ids, index=ids.index(sugerida), horizontal=True)
                  comentario = st.text_area("Comentario (obligatorio si escala o devuelve)")
                  if st.form_submit_button("Registrar decisión", type="primary"):
                      doms_h = dominios_de(ids)
                      if not usuario():
                          st.error("No se pudo identificar al usuario: abra la app desde su URL en el workspace.")
                      elif not puede_decidir(doms_h):
                          st.error(f"Su usuario no está autorizado para decidir en los dominios {', '.join(sorted(d or '?' for d in doms_h))} (gold.aprobadores_por_dominio).")
                      elif decision in ("ESCALAR_A_COMITE", "DEVOLVER_AL_AREA") and not comentario.strip():
                          st.error("Agregue un comentario para el área o el comité.")
                      else:
                          ejecutar(f"""INSERT INTO {GOLD}.decisiones_revision
                                       (id_decision, id_hallazgo, decision, id_ficha_oficial, comentario, revisor, fecha_decision)
                                       VALUES (:id, :h, :d, :o, :c, :u, current_timestamp())""",
                                   {"id": str(uuid.uuid4()), "h": elegido, "d": decision,
                                    "o": oficial if decision in ("UNIFICAR", "DEFINIR_OFICIAL") else None,
                                    "c": comentario.strip() or None, "u": usuario()})
                          if decision != "ESCALAR_A_COMITE":
                              ejecutar(f"UPDATE {GOLD}.hallazgos_revision SET estado = 'RESUELTO' WHERE id_hallazgo = :h", {"h": elegido})
                          st.success("Decisión registrada. El catálogo se actualiza en la siguiente publicación.")
                          st.rerun()
  except Exception as e:  # la cola se crea en la primera detección (notebook 04)
    mostrar_error(e, "Cola de revisión")

with pestana_aprobar:
    st.write("Indicadores sin hallazgos pendientes que esperan la aprobación del Data Owner para quedar **certificados** "
             "en la siguiente publicación. Si la ficha cambia, la aprobación deja de valer.")
    try:
        por_aprobar = consultar(f"""SELECT id_indicador, nombre, dominio, area_responsable, data_owner, definicion_funcional, formula,
                                           meta_umbral, huella_ficha
                                    FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'PENDIENTE_APROBACION'
                                    ORDER BY dominio, id_indicador""")
        if por_aprobar.empty:
            st.success("No hay indicadores pendientes de aprobación.")
        else:
            dominio = st.selectbox("Dominio", sorted(por_aprobar["dominio"].dropna().unique()))
            vista = por_aprobar[por_aprobar["dominio"] == dominio]
            st.dataframe(vista.drop(columns=["huella_ficha"]), use_container_width=True, hide_index=True)
            elegidos = st.multiselect("Indicadores a aprobar", list(vista["id_indicador"]))
            if st.button("Aprobar seleccionados", type="primary", disabled=not elegidos):
                if not usuario():
                    st.error("No se pudo identificar al usuario: abra la app desde su URL en el workspace.")
                elif not puede_decidir({dominio}):
                    st.error(f"Su usuario no figura como aprobador del dominio {dominio} (gold.aprobadores_por_dominio).")
                else:
                    for fid in elegidos:
                        huella = vista.loc[vista["id_indicador"] == fid, "huella_ficha"].iloc[0]
                        ejecutar(f"""INSERT INTO {GOLD}.aprobaciones_indicadores (id_indicador, huella_ficha, aprobado_por, fecha_aprobacion)
                                     VALUES (:i, :h, :u, current_timestamp())""", {"i": fid, "h": huella, "u": usuario()})
                    st.success(f"{len(elegidos)} indicadores aprobados. Quedarán certificados en la siguiente publicación.")
    except Exception as e:
        mostrar_error(e, "Aprobación de indicadores")

with pestana_catalogo:
    try:
        catalogo = consultar(f"""SELECT id_indicador, nombre, dominio, estado_certificacion, data_owner, unidad_medida, sinonimos, vista_de_metricas
                                 FROM {GOLD}.catalogo_indicadores ORDER BY dominio, id_indicador""")
        estado = st.multiselect("Estado", sorted(catalogo["estado_certificacion"].unique()),
                                default=sorted(catalogo["estado_certificacion"].unique()))
        st.dataframe(catalogo[catalogo["estado_certificacion"].isin(estado)], use_container_width=True, hide_index=True)
    except Exception as e:  # el catálogo se crea en la primera publicación
        mostrar_error(e, "Catálogo publicado")

with pestana_historial:
    try:
        st.dataframe(consultar(f"""
            SELECT d.fecha_decision, d.revisor, h.tipo_hallazgo, h.id_ficha, h.id_ficha_relacionada, d.decision, d.id_ficha_oficial, d.comentario
            FROM {GOLD}.decisiones_revision d JOIN {GOLD}.hallazgos_revision h USING (id_hallazgo)
            ORDER BY d.fecha_decision DESC LIMIT 500"""), use_container_width=True, hide_index=True)
    except Exception as e:
        mostrar_error(e, "Historial de decisiones")
