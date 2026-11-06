# Genera una ficha PDF con la estructura de la plantilla "DOCUMENTACIÓN INDICADORES - PLANTILLA" v1.2
# (6 bloques). Se usa solo para crear datos sintéticos de la demo; en producción las fichas
# llegan de las áreas de negocio.
import random
from datetime import date, timedelta

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

AZUL = colors.HexColor("#0B2E4F")
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
MESES_LARGOS = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre",
                "Octubre", "Noviembre", "Diciembre"]

_base = ParagraphStyle("base", fontName="Helvetica", fontSize=9.5, leading=12.5)
_aviso = ParagraphStyle("aviso", parent=_base, fontSize=7.5, textColor=colors.grey, alignment=TA_CENTER)
_titulo = ParagraphStyle("titulo", parent=_base, fontName="Helvetica-Bold", textColor=colors.white,
                         alignment=TA_CENTER, fontSize=10.5, leading=13)
_bloque = ParagraphStyle("bloque", parent=_base, fontName="Helvetica-Bold")

ANCHO = 17 * cm


def _p(texto, estilo=_base):
    return Paragraph(texto.replace("\n", "<br/>"), estilo)


def _campo(etiqueta, valor):
    return _p(f"<b>{etiqueta}:</b> {valor}")


def _lista(items):
    return "<br/>".join(f"&#8226; {i}" for i in items)


def _tabla(filas, encabezado):
    """filas: lista de celdas (una celda = fila completa) o tuplas de 2 celdas."""
    datos = [[_p(encabezado, _bloque), ""]]
    estilo = [("SPAN", (0, 0), (1, 0))]
    for i, fila in enumerate(filas, start=1):
        if isinstance(fila, tuple):
            datos.append([fila[0], fila[1]])
        else:
            datos.append([fila, ""])
            estilo.append(("SPAN", (0, i), (1, i)))
    t = Table(datos, colWidths=[ANCHO / 2, ANCHO / 2])
    t.setStyle(TableStyle(estilo + [
        ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def _fecha_corta(d):
    return f"{d.day} {MESES[d.month - 1]} {d.year}"


def _fecha_larga(d):
    return f"{d.day:02d} de {MESES_LARGOS[d.month - 1]} del {d.year}"


def completar(ind, dominios, responsables, semilla=0):
    """Rellena con valores coherentes los campos que la entrada del catálogo no especifica."""
    r = random.Random(f"{semilla}-{ind['id']}")
    d = dict(ind)
    dom = dominios[d["dominio"]]
    nombre_min = d["nombre"]
    d.setdefault("objetivo", f"Contar con una medición única y confiable de {nombre_min} para la toma de decisiones del área de {d['area']}.")
    d.setdefault("exclusiones", "No aplica.")
    d.setdefault("audiencia", f"Dirección de {d['area']} y Comité {'de Riesgos' if d['tipo'] == 'Regulatorio' else 'Operativo'}.")
    d.setdefault("riesgos", dom["riesgos"])
    d.setdefault("dependencias", dom["dependencias"])
    mensual = d["tipo"] in ("Estratégico", "Regulatorio")
    d.setdefault("frec_calculo", "Mensual (cierre de mes)" if mensual else "Diario (Cierre nocturno)")
    d.setdefault("frec_publicacion", "Mensual en Dashboard Power Bi en Fabric." if mensual else "Diaria en Dashboard Power Bi en Fabric.")
    d.setdefault("horario", r.choice(["08:00", "09:00", "10:00"]))
    owner, steward = responsables.get(d["area"], ("Gerente del Área", "Analista del Área"))
    if d.get("sin_responsables"):
        owner, steward = "", ""
    d.setdefault("owner", owner)
    d.setdefault("steward", steward)
    d.setdefault("frec_revision", r.choice(["Semestral", "Anual", "Trimestral"]))
    creacion = date(2024, 6, 1) + timedelta(days=r.randint(0, 420))
    ultima = min(creacion + timedelta(days=r.randint(90, 600)), date(2026, 9, 30))
    d.setdefault("fecha_creacion", _fecha_corta(creacion))
    d.setdefault("ultima_actualizacion", _fecha_corta(ultima))
    hist = [f"{_fecha_larga(creacion)}. Alta del indicador."]
    paso = creacion
    for cambio in r.sample(["Revisión de Vigencia. Sin ningún cambio en el cálculo.",
                            "Se actualiza la meta del indicador.",
                            "Se agregan dimensiones de análisis.",
                            "Se documentan las reglas de calidad."], k=r.randint(1, 3)):
        paso = min(paso + timedelta(days=r.randint(60, 200)), ultima)
        hist.append(f"{_fecha_larga(paso)}. {cambio}")
    d.setdefault("historial", hist)
    d.setdefault("evidencias", ["Diccionario de Datos: Link", f"Manual de políticas de {d['area']}: Link"])
    if not d.get("sin_calidad"):
        campos = [c.strip() for c in d.get("campos", "").split(",") if c.strip()]
        llave = campos[0] if campos else "el identificador del registro"
        d.setdefault("calidad", {
            "completitud": f"Los campos {', '.join(campos[:2]) or 'clave'} no permiten nulos (100% de completitud).",
            "validez": "Los importes y conteos deben ser mayores o iguales a cero; las fechas no pueden ser futuras.",
            "unicidad": f"Un único registro por {llave} en cada corte.",
            "oportunidad": "Disponible a más tardar al día hábil siguiente del cierre." if not mensual
                           else "Disponible a más tardar el día hábil 5 posterior al cierre de mes.",
            "contingencia": "Si alguna regla de completitud falla, el Steward suspende la publicación y notifica al Owner.",
        })
    d.setdefault("glosario", [])
    d.setdefault("homologacion", "No aplica.")
    d.setdefault("propositos", [f"Por {n}: permite focalizar acciones del área de {d['area']}." for n, _ in d.get("dimensiones", [])[:3]])
    return d


def generar_pdf(d, ruta, version_plantilla="1.2", fecha_plantilla="10-06-2026"):
    """Escribe la ficha d (ya completada) en la ruta PDF indicada."""
    doc = SimpleDocTemplate(ruta, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
                            title=f"{d['id']} {d['nombre']}", author=d["area"])
    aviso = "Este documento es solamente un ejemplo ilustrativo, no forma parte de una documentación oficial."
    el = [_p(aviso, _aviso), Spacer(1, 0.4 * cm)]

    cab = Table([[_p("DOCUMENTACIÓN INDICADORES -<br/>PLANTILLA", _titulo),
                  _p(f"Versión: {version_plantilla}<br/>Fecha última versión: {fecha_plantilla}", _titulo)]],
                colWidths=[ANCHO / 2, ANCHO / 2])
    cab.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), AZUL), ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
                             ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("TOPPADDING", (0, 0), (-1, -1), 8),
                             ("BOTTOMPADDING", (0, 0), (-1, -1), 8)]))
    el.append(cab)

    el.append(_tabla([
        (_p(f"<b>ID del Indicador:</b><br/>{d['id']}"), _campo("Objetivo Estratégico", d["objetivo"])),
        _campo("Nombre del Indicador", d["nombre"] + "."),
        _campo("Definición Funcional", d["definicion"]),
        _campo("Umbrales / Meta", d["meta"]),
        (_campo("Pregunta de Negocio que Responde", "<br/>" + d["pregunta"]),
         _campo("Criterios de Exclusión de Negocio", d["exclusiones"])),
        (_campo("Audiencia", d["audiencia"]), _campo("Unidad de Medida", d["unidad"])),
    ], "BLOQUE 1: IDENTIDAD FUNCIONAL (AUTOEXPLICATIVO)"))
    el.append(Spacer(1, 0.5 * cm))

    tipos = "<br/>".join(f"{'[X]' if t == d['tipo'] else '[ ]'} {t}" for t in ["Estratégico", "Operativo", "Regulatorio", "Informativo"])
    el.append(_tabla([
        _p("<b>Tipo de Indicador:</b> <i>Clasificación del indicador según su criticidad funcional (Crítico / Operativo / Informativo).</i><br/>" + tipos),
        _p(f"<b>Riesgos Asociados:</b><br/>{d['riesgos']}"),
        _campo("Dependencias", d["dependencias"]),
    ], "BLOQUE 2: CLASIFICACIÓN Y RIESGO"))
    el.append(Spacer(1, 0.5 * cm))

    if d.get("sin_fuente"):
        origen = "<b>Origen de Datos (Ubicación Física):</b><br/>Por definir."
    else:
        s, bd, esq, tab = d["fuente"]
        origen = (f"<b>Origen de Datos (Ubicación Física):</b><br/>Sistema: {s}<br/>Base de Datos: {bd}<br/>"
                  f"Esquema: {esq}<br/>Tabla: {tab}<br/>Campos: {d.get('campos', '')}.")
    el.append(_tabla([
        _p("<b>Inputs Requeridos (Variables):</b><br/>" + _lista([f"Variable A: {d['var_a']}", f"Variable B: {d['var_b']}"])),
        _p("<b>Frecuencia:</b><br/>" + _lista([f"Cálculo: {d['frec_calculo']}", f"Publicación: {d['frec_publicacion']}"])
           + f"<br/><br/><b>Horario Operativo:</b> {d['horario']}"),
        (_campo("Fórmula / Lógica", d["formula"]), _p(origen)),
    ], "BLOQUE 3: MÚSCULO TÉCNICO Y ORIGEN"))
    el.append(Spacer(1, 0.5 * cm))

    el.append(_tabla([
        _campo("Área Responsable", d["area"]),
        _campo("Dominio de Datos", d["dominio"]),
        (_p(f"<b>Data Owner (Brinda el VoBo Funcional):</b><br/>{d['owner']}"),
         _p(f"<b>Data Steward (Responsable de implementación y Ejecutor):</b><br/>{d['steward']}")),
        (_campo("Frecuencia de Revisión", d["frec_revision"]), _campo("Fecha de Creación", d["fecha_creacion"])),
        _campo("Última Actualización", d["ultima_actualizacion"]),
        _p("<b>Evidencias:</b><br/>" + "<br/>".join(d["evidencias"])),
        _p("<b>Historial de cambios:</b><br/>" + "<br/>".join(d["historial"])),
    ], "BLOQUE 4: MODELO OPERATIVO"))
    el.append(Spacer(1, 0.5 * cm))

    c = d.get("calidad")
    filas5 = []
    if c:
        filas5 += [_campo("Criterio de Completitud", c["completitud"]), _campo("Criterio de Validez", c["validez"]),
                   _campo("Regla de Unicidad", c["unicidad"]), _campo("Regla de Oportunidad", c["oportunidad"]),
                   _campo("Protocolo de Contingencia", c["contingencia"])]
    else:
        filas5.append(_p("<b>Reglas de calidad:</b> Por definir."))
    glos = _lista([f"<b>{t}:</b> {x}" for t, x in d["glosario"]]) if d["glosario"] else "Sin términos adicionales."
    filas5 += [_p("<b>Glosario:</b><br/>" + glos), _campo("Homologación de Vocabulario", d["homologacion"])]
    el.append(_tabla(filas5, "BLOQUE 5: REGLAS DE CALIDAD Y GLOSARIO"))
    el.append(Spacer(1, 0.5 * cm))

    dims = d.get("dimensiones", [])
    if dims:
        filas6 = [_p("<b>Listado de Dimensiones Autorizadas:</b><br/>" + _lista([n for n, _ in dims])),
                  _p("<b>Propósito de Negocio del Desglose:</b><br/>" + _lista(d["propositos"]))]
        if not d.get("sin_fuente"):
            s, bd, esq, tab = d["fuente"]
            filas6.append(_p(f"<b>Mapeo Técnico de Atributos:</b><br/>Sistema: {s}<br/>Base de Datos: {bd}<br/>Esquema: {esq}<br/>"
                             f"Tabla: {tab}<br/>Campos de filtro / agrupación: {', '.join(cmp for _, cmp in dims)}"))
    else:
        filas6 = [_p("<b>Listado de Dimensiones Autorizadas:</b> Por definir.")]
    el.append(_tabla(filas6, "BLOQUE 6: DIMENSIONES DE ANÁLISIS"))

    def _pie(canvas, _doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.grey)
        canvas.drawCentredString(A4[0] / 2, A4[1] - 0.9 * cm, aviso)
        canvas.restoreState()

    doc.build(el, onLaterPages=_pie)
