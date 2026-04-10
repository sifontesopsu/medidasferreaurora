import base64
import io
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side


st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")

APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")
ESTADOS_CIERRE = [
    "listo_para_ejecutivo",
    "en_gestion_ejecutivo",
    "resuelto",
    "rechazado_ml",
    "rechazado_ejecutivo",
]
BANDEJAS_ADMINISTRATIVA = {
    "Pendientes por gestionar": ["validado_supervisor"],
    "Enviados a ejecutiva": ["listo_para_ejecutivo"],
    "En gestión ejecutiva": ["en_gestion_ejecutivo"],
    "Cerrados": ["resuelto", "rechazado_ml", "rechazado_ejecutivo"],
}


# =========================================================
# API
# =========================================================
def api_post(payload: Dict[str, Any], timeout: int = 180) -> Dict[str, Any]:
    if not APPS_SCRIPT_URL:
        raise RuntimeError("Falta APPS_SCRIPT_URL en st.secrets")

    response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Error desconocido en API"))

    return data


@st.cache_data(ttl=25, show_spinner=False)
def api_get_dashboard_counts() -> Dict[str, Any]:
    return api_post({"action": "get_dashboard_counts"}, timeout=120)


@st.cache_data(ttl=15, show_spinner=False)
def api_get_admin_queue(
    query: str = "",
    estados: Optional[List[str]] = None,
    operador: str = "",
    limit: int = 1000000,
) -> pd.DataFrame:
    data = api_post(
        {
            "action": "get_admin_queue",
            "query": query,
            "estados": estados or [],
            "operador": operador,
            "limit": limit,
        },
        timeout=120,
    )
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_admin_queue_grouped_by_sku(
    query: str = "",
    estados: Optional[List[str]] = None,
    operador: str = "",
) -> pd.DataFrame:
    data = api_post(
        {
            "action": "get_admin_queue_grouped_by_sku",
            "query": query,
            "estados": estados or [],
            "operador": operador,
        },
        timeout=120,
    )
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_tasks_by_operator(operador: str) -> pd.DataFrame:
    data = api_post({"action": "get_tasks_by_operator", "operador": operador}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_tasks_by_operator_grouped_by_sku(operador: str) -> pd.DataFrame:
    data = api_post({"action": "get_tasks_by_operator_grouped_by_sku", "operador": operador}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_pending_validation(limit: int = 200) -> pd.DataFrame:
    data = api_post({"action": "get_pending_validation", "limit": limit}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_pending_validation_grouped_by_sku(limit: int = 200) -> pd.DataFrame:
    data = api_post({"action": "get_pending_validation_grouped_by_sku", "limit": limit}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=15, show_spinner=False)
def api_get_administrative_queue(statuses: List[str], limit: int = 300) -> pd.DataFrame:
    data = api_post(
        {"action": "get_administrative_queue", "statuses": statuses, "limit": limit},
        timeout=120,
    )
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=20, show_spinner=False)
def api_get_case_detail(sku: str, mlc: str) -> Dict[str, Any]:
    return api_post({"action": "get_case_detail", "sku": sku, "mlc": mlc}, timeout=120)


@st.cache_data(ttl=20, show_spinner=False)
def api_get_case_detail_by_sku(sku: str) -> Dict[str, Any]:
    return api_post({"action": "get_case_detail_by_sku", "sku": sku}, timeout=120)


@st.cache_data(ttl=30, show_spinner=False)
def api_get_evidencias(sku: str, mlc: str) -> pd.DataFrame:
    data = api_post({"action": "get_evidencias", "sku": sku, "mlc": mlc}, timeout=120)
    return pd.DataFrame(data.get("data", []))


@st.cache_data(ttl=30, show_spinner=False)
def api_get_evidencias_by_sku(sku: str) -> pd.DataFrame:
    data = api_post({"action": "get_evidencias_by_sku", "sku": sku}, timeout=120)
    return pd.DataFrame(data.get("data", []))


def api_login_with_pin(usuario: str, pin: str) -> Dict[str, Any]:
    return api_post({"action": "login_with_pin", "usuario": usuario, "pin": pin}, timeout=60)


def api_assign_tasks(items: List[Dict[str, str]], operador: str, usuario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "assign_tasks",
            "items": items,
            "operador": operador,
            "prioridad": "",
            "usuario": usuario,
        }
    )


def api_assign_tasks_grouped_by_sku(items: List[Dict[str, str]], operador: str, usuario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "assign_tasks_grouped_by_sku",
            "items": items,
            "operador": operador,
            "usuario": usuario,
        }
    )


def api_save_measurement_with_photos(
    sku: str,
    mlc: str,
    operador: str,
    alto_real_cm: float,
    ancho_real_cm: float,
    profundidad_real_cm: float,
    peso_real_kg: float,
    observacion_operador: str,
    foto_alto,
    foto_ancho,
    foto_profundidad,
    foto_peso,
) -> Dict[str, Any]:
    def to_base64(uploaded_file) -> str:
        return base64.b64encode(uploaded_file.getvalue()).decode("utf-8")

    return api_post(
        {
            "action": "save_measurement_with_photos",
            "sku": sku,
            "mlc": mlc,
            "operador": operador,
            "alto_real_cm": alto_real_cm,
            "ancho_real_cm": ancho_real_cm,
            "profundidad_real_cm": profundidad_real_cm,
            "peso_real_kg": peso_real_kg,
            "observacion_operador": observacion_operador,
            "photos": [
                {
                    "tipo": "alto",
                    "file_base64": to_base64(foto_alto),
                    "mime_type": foto_alto.type or "image/jpeg",
                    "file_name": foto_alto.name,
                },
                {
                    "tipo": "ancho",
                    "file_base64": to_base64(foto_ancho),
                    "mime_type": foto_ancho.type or "image/jpeg",
                    "file_name": foto_ancho.name,
                },
                {
                    "tipo": "profundidad",
                    "file_base64": to_base64(foto_profundidad),
                    "mime_type": foto_profundidad.type or "image/jpeg",
                    "file_name": foto_profundidad.name,
                },
                {
                    "tipo": "peso",
                    "file_base64": to_base64(foto_peso),
                    "mime_type": foto_peso.type or "image/jpeg",
                    "file_name": foto_peso.name,
                },
            ],
        },
        timeout=240,
    )


def api_save_measurement_with_photos_by_sku(
    sku: str,
    operador: str,
    alto_real_cm: float,
    ancho_real_cm: float,
    profundidad_real_cm: float,
    peso_real_kg: float,
    observacion_operador: str,
    foto_alto,
    foto_ancho,
    foto_profundidad,
    foto_peso,
) -> Dict[str, Any]:
    def to_base64(uploaded_file) -> str:
        return base64.b64encode(uploaded_file.getvalue()).decode("utf-8")

    return api_post(
        {
            "action": "save_measurement_with_photos_by_sku",
            "sku": sku,
            "operador": operador,
            "alto_real_cm": alto_real_cm,
            "ancho_real_cm": ancho_real_cm,
            "profundidad_real_cm": profundidad_real_cm,
            "peso_real_kg": peso_real_kg,
            "observacion_operador": observacion_operador,
            "photos": [
                {"tipo": "alto", "file_base64": to_base64(foto_alto), "mime_type": foto_alto.type or "image/jpeg", "file_name": foto_alto.name},
                {"tipo": "ancho", "file_base64": to_base64(foto_ancho), "mime_type": foto_ancho.type or "image/jpeg", "file_name": foto_ancho.name},
                {"tipo": "profundidad", "file_base64": to_base64(foto_profundidad), "mime_type": foto_profundidad.type or "image/jpeg", "file_name": foto_profundidad.name},
                {"tipo": "peso", "file_base64": to_base64(foto_peso), "mime_type": foto_peso.type or "image/jpeg", "file_name": foto_peso.name},
            ],
        },
        timeout=240,
    )


def api_validate_measurement(sku: str, mlc: str, supervisor: str, aprobar: bool, comentario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "validate_measurement",
            "sku": sku,
            "mlc": mlc,
            "supervisor": supervisor,
            "aprobar": aprobar,
            "comentario": comentario,
        }
    )


def api_validate_measurement_by_sku(sku: str, supervisor: str, aprobar: bool, comentario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "validate_measurement_by_sku",
            "sku": sku,
            "supervisor": supervisor,
            "aprobar": aprobar,
            "comentario": comentario,
        }
    )


def api_update_status(
    sku: str,
    mlc: str,
    nuevo_estado: str,
    usuario: str,
    comentario: str = "",
    ticket_ejecutivo: str = "",
) -> Dict[str, Any]:
    return api_post(
        {
            "action": "update_status",
            "sku": sku,
            "mlc": mlc,
            "nuevo_estado": nuevo_estado,
            "usuario": usuario,
            "comentario": comentario,
            "ticket_ejecutivo": ticket_ejecutivo,
        }
    )


# =========================================================
# HELPERS
# =========================================================
def clear_caches() -> None:
    api_get_dashboard_counts.clear()
    api_get_admin_queue.clear()
    api_get_admin_queue_grouped_by_sku.clear()
    api_get_tasks_by_operator.clear()
    api_get_tasks_by_operator_grouped_by_sku.clear()
    api_get_pending_validation.clear()
    api_get_pending_validation_grouped_by_sku.clear()
    api_get_administrative_queue.clear()
    api_get_case_detail.clear()
    api_get_case_detail_by_sku.clear()
    api_get_evidencias.clear()
    api_get_evidencias_by_sku.clear()


def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) and not df.empty else pd.DataFrame()


def badge_estado(estado: str) -> str:
    color_map = {
        "pendiente_medicion": "#f59e0b",
        "requiere_nueva_evidencia": "#f97316",
        "medido_pendiente_validacion": "#3b82f6",
        "validado_supervisor": "#10b981",
        "listo_para_ejecutivo": "#6366f1",
        "en_gestion_ejecutivo": "#8b5cf6",
        "resuelto": "#16a34a",
        "rechazado_ml": "#dc2626",
        "rechazado_ejecutivo": "#b91c1c",
    }
    color = color_map.get(str(estado), "#6b7280")
    return (
        f"<span style='background:{color};color:white;padding:4px 8px;"
        f"border-radius:999px;font-size:12px'>{estado}</span>"
    )


def show_kpi_row_from_counts(counts: Dict[str, Any]) -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total", int(counts.get("total", 0)))
    with c2:
        st.metric("Pendiente medición", int(counts.get("pendiente_medicion", 0)))
    with c3:
        st.metric("Pendiente validación", int(counts.get("medido_pendiente_validacion", 0)))
    with c4:
        st.metric("Pendiente gestión administrativa", int(counts.get("validado_supervisor", 0)))


def build_ejecutiva_excel_bytes(df: pd.DataFrame, seller_id: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Hoja 1"

    ws.merge_cells("C1:F1")
    ws["C1"] = "PACKAGING"
    ws["C1"].font = Font(bold=True)
    ws["C1"].alignment = Alignment(horizontal="center", vertical="center")
    ws["C1"].fill = PatternFill(fill_type="solid", fgColor="F4DDC6")

    headers = [
        "seller_id",
        "item_id",
        "height_propuesto (cm)",
        "width_propuesto (cm)",
        "length_propuesto (cm)",
        "weight_propuesto (gr)",
    ]
    header_fills = ["C6E0B4", "C6E0B4", "F4DDC6", "F4DDC6", "F4DDC6", "F4DDC6"]
    thin = Side(style="thin", color="A0A0A0")

    for col_idx, (header, color) in enumerate(zip(headers, header_fills), start=1):
        cell = ws.cell(row=2, column=col_idx, value=header)
        cell.font = Font(bold=True)
        cell.fill = PatternFill(fill_type="solid", fgColor=color)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for row_idx, (_, row) in enumerate(df.iterrows(), start=3):
        ws.cell(row=row_idx, column=1, value=seller_id)
        ws.cell(row=row_idx, column=2, value=str(row.get("mlc", "")))
        ws.cell(row=row_idx, column=3, value=row.get("alto_real_cm", ""))
        ws.cell(row=row_idx, column=4, value=row.get("ancho_real_cm", ""))
        ws.cell(row=row_idx, column=5, value=row.get("profundidad_real_cm", ""))
        peso_kg = pd.to_numeric(pd.Series([row.get("peso_real_kg", "")]), errors="coerce").iloc[0]
        peso_gr = "" if pd.isna(peso_kg) else round(float(peso_kg) * 1000, 2)
        ws.cell(row=row_idx, column=6, value=peso_gr)
        for col_idx in range(1, 7):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    widths = {"A": 14, "B": 18, "C": 20, "D": 20, "E": 22, "F": 22}
    for col_letter, width in widths.items():
        ws.column_dimensions[col_letter].width = width

    ws.row_dimensions[1].height = 22
    ws.freeze_panes = "A3"

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


def build_comparativas_excel_bytes(df: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Comparativas"

    headers = [
        "sku",
        "mlc",
        "titulo",
        "alto_ml_cm",
        "ancho_ml_cm",
        "profundidad_ml_cm",
        "peso_ml_kg",
        "alto_real_cm",
        "ancho_real_cm",
        "profundidad_real_cm",
        "peso_real_kg",
        "estado_actual",
        "operador_asignado",
        "supervisor",
        "fecha_ultima_medicion",
        "fecha_validacion",
    ]

    header_fill = PatternFill(fill_type="solid", fgColor="D9EAF7")
    thin = Side(style="thin", color="A0A0A0")

    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    export_df = df.copy()
    for col in headers:
        if col not in export_df.columns:
            export_df[col] = ""

    export_df = export_df[headers]

    for row_idx, (_, row) in enumerate(export_df.iterrows(), start=2):
        for col_idx, value in enumerate(row.tolist(), start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    widths = {
        "A": 18, "B": 16, "C": 55, "D": 12, "E": 12, "F": 16, "G": 12,
        "H": 12, "I": 12, "J": 16, "K": 12, "L": 24, "M": 18, "N": 18,
        "O": 20, "P": 20
    }
    for col_letter, width in widths.items():
        ws.column_dimensions[col_letter].width = width

    ws.freeze_panes = "A2"

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


def normalize_case_payload(detail: Dict[str, Any], fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    fallback = fallback or {}
    if not isinstance(detail, dict):
        return fallback.copy()

    for key in ["case", "item", "data", "detail", "row"]:
        value = detail.get(key)
        if isinstance(value, dict) and value:
            merged = fallback.copy()
            merged.update(value)
            return merged

    merged = fallback.copy()
    flat = {k: v for k, v in detail.items() if not isinstance(v, (dict, list))}
    merged.update(flat)
    return merged


def render_case_summary(case: Dict[str, Any]) -> None:
    if not case:
        st.warning("No se pudo cargar el caso")
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {case.get('sku', '')}")
        st.markdown(f"**MLC:** {case.get('mlc', '')}")
        st.markdown(f"**Título:** {case.get('titulo', '')}")
        st.markdown(badge_estado(str(case.get('estado_actual', ''))), unsafe_allow_html=True)
    with c2:
        st.markdown(
            f"**ML:** {case.get('alto_ml_cm', '')} x {case.get('ancho_ml_cm', '')} x {case.get('profundidad_ml_cm', '')} cm | {case.get('peso_ml_kg', '')} kg"
        )
        st.markdown(
            f"**Real:** {case.get('alto_real_cm', '')} x {case.get('ancho_real_cm', '')} x {case.get('profundidad_real_cm', '')} cm | {case.get('peso_real_kg', '')} kg"
        )
        st.markdown(f"**Operador:** {case.get('operador_asignado', '')}")
        st.markdown(f"**Supervisor:** {case.get('supervisor', '')}")

    obs_operador = case.get("observacion_operador", "")
    obs_admin = case.get("observacion_admin", "")
    if obs_operador:
        st.info(f"Observación operador: {obs_operador}")
    if obs_admin:
        st.info(f"Observación admin/supervisor: {obs_admin}")


def build_drive_view_url(row) -> str:
    file_id = str(row.get("drive_file_id", "") or "").strip()
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"

    drive_link = str(row.get("drive_link", "") or "").strip()
    if "id=" in drive_link:
        return drive_link

    return drive_link


def render_drive_image(row):
    url = build_drive_view_url(row)
    if not url:
        st.warning("Sin link")
        return

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        st.image(resp.content, use_container_width=True)
    except Exception:
        st.warning("No se pudo mostrar imagen")


def render_evidencias(evidencias: pd.DataFrame) -> None:
    st.markdown("### Evidencia fotográfica")
    if evidencias.empty:
        st.warning("No hay fotos disponibles")
        return

    evidencias = evidencias.copy()
    evidencias["tipo_foto"] = evidencias["tipo_foto"].astype(str).str.lower()
    if "fecha_carga" in evidencias.columns:
        evidencias = evidencias.sort_values("fecha_carga")
    evidencias = evidencias.drop_duplicates(subset=["tipo_foto"], keep="last")

    orden = ["alto", "ancho", "profundidad", "peso"]
    mapa = {str(r["tipo_foto"]).lower(): r for _, r in evidencias.iterrows()}
    cols = st.columns(4)

    for i, tipo in enumerate(orden):
        with cols[i]:
            st.markdown(f"**{tipo.upper()}**")
            row = mapa.get(tipo)
            if row is None:
                st.warning("Falta foto")
                continue
            render_drive_image(row)


def require_login() -> Dict[str, Any]:
    if "auth_user" not in st.session_state:
        st.title("Control Medidas ML")
        st.subheader("Ingreso con PIN")
        with st.form("login_form"):
            usuario = st.text_input("Usuario")
            pin = st.text_input("PIN", type="password")
            submitted = st.form_submit_button("Ingresar", use_container_width=True)
        if submitted:
            try:
                auth = api_login_with_pin(usuario, pin)
                st.session_state["auth_user"] = auth.get("user", {})
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo iniciar sesión: {e}")
        st.stop()
    return st.session_state["auth_user"]


def toggle_evidencias(case_key: str) -> None:
    current = st.session_state.get(case_key, False)
    st.session_state[case_key] = not current


# =========================================================
# AUTH / SIDEBAR
# =========================================================
user = require_login()
rol = str(user.get("rol", "")).strip().lower()
usuario_actual = str(user.get("usuario_id", user.get("nombre", "")))
operador_codigo = str(user.get("operador_codigo", "") or usuario_actual)

with st.sidebar:
    st.title("Control Medidas ML")
    st.caption(f"Usuario: {user.get('nombre', usuario_actual)}")
    st.caption(f"Rol: {rol}")
    if st.button("Recargar", use_container_width=True):
        clear_caches()
        st.rerun()
    if st.button("Cerrar sesión", use_container_width=True):
        st.session_state.clear()
        st.rerun()

if rol == "admin":
    opciones_modulo = ["Administrador", "Operador", "Supervisor", "Administrativa"]
elif rol == "operador":
    opciones_modulo = ["Operador"]
elif rol == "supervisor":
    opciones_modulo = ["Supervisor"]
elif rol == "administrativa":
    opciones_modulo = ["Administrativa"]
else:
    opciones_modulo = ["Administrador", "Operador", "Supervisor", "Administrativa"]

modo = st.sidebar.radio("Módulo", opciones_modulo)


# =========================================================
# ADMINISTRADOR
# =========================================================
if modo == "Administrador":
    st.title("Panel Administrador")

    try:
        counts = api_get_dashboard_counts()
    except Exception as e:
        st.error(f"No se pudieron cargar los indicadores: {e}")
        st.stop()

    show_kpi_row_from_counts(counts)

    st.subheader("Filtros")
    with st.form("admin_filters_form"):
        f1, f2, f3 = st.columns([2, 2, 2])
        with f1:
            texto = st.text_input("Buscar SKU / MLC / título")
        with f2:
            estados_disponibles = counts.get("estados_disponibles", [])
            estados_sel = st.multiselect("Estado", estados_disponibles, default=estados_disponibles)
        with f3:
            operador_filter = st.text_input("Operador asignado")
        filtros_submit = st.form_submit_button("Aplicar filtros", use_container_width=True)

    admin_filter_state = st.session_state.setdefault(
        "admin_filters_state",
        {"texto": "", "estados_sel": counts.get("estados_disponibles", []), "operador_filter": ""},
    )
    if filtros_submit:
        admin_filter_state["texto"] = texto.strip()
        admin_filter_state["estados_sel"] = estados_sel
        admin_filter_state["operador_filter"] = operador_filter.strip()

    try:
        df_filtrado_pub = api_get_admin_queue(
            query=admin_filter_state["texto"],
            estados=admin_filter_state["estados_sel"],
            operador=admin_filter_state["operador_filter"],
        )
        df_filtrado_sku = api_get_admin_queue_grouped_by_sku(
            query=admin_filter_state["texto"],
            estados=admin_filter_state["estados_sel"],
            operador=admin_filter_state["operador_filter"],
        )
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja administrativa: {e}")
        st.stop()

    df_filtrado_pub = safe_df(df_filtrado_pub)
    df_filtrado_sku = safe_df(df_filtrado_sku)

    if df_filtrado_sku.empty:
        st.info("No hay SKUs para los filtros seleccionados")
        st.stop()

    st.caption(f"Resultados encontrados: {len(df_filtrado_sku)} SKUs | {len(df_filtrado_pub)} publicaciones")

    st.subheader("Reporte comparativo")
    comparativas_bytes = build_comparativas_excel_bytes(df_filtrado_pub if not df_filtrado_pub.empty else df_filtrado_sku)
    st.download_button(
        "Descargar Excel comparativas",
        data=comparativas_bytes,
        file_name="reporte_comparativas_medidas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
        key="download_comparativas_admin",
    )

    st.subheader("Asignación de tareas por SKU")
    with st.form("admin_assign_form"):
        operador_destino = st.text_input("Asignar a operador", value="")
        cols_view = [c for c in ["sku", "titulo", "publicaciones_count", "estado_actual", "operador_asignado"] if c in df_filtrado_sku.columns]
        edited = st.data_editor(
            df_filtrado_sku[cols_view].assign(seleccionar=False),
            use_container_width=True,
            hide_index=True,
            column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
            disabled=cols_view,
            key="admin_editor_asignacion_sku",
        )
        asignar_btn = st.form_submit_button("Asignar SKUs seleccionados", use_container_width=True)

    if asignar_btn:
        if not operador_destino.strip():
            st.warning("Debes indicar el nombre del operador")
        else:
            seleccionados = edited[edited["seleccionar"] == True]  # noqa: E712
            if seleccionados.empty:
                st.warning("No seleccionaste SKUs")
            else:
                items = seleccionados[["sku"]].to_dict(orient="records")
                try:
                    result = api_assign_tasks_grouped_by_sku(items, operador_destino.strip(), usuario_actual)
                    clear_caches()
                    st.success(f"Publicaciones afectadas por asignación: {result.get('assigned', 0)}")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo asignar: {e}")


# =========================================================
# OPERADOR
# =========================================================
elif modo == "Operador":
    st.title("Módulo Operador PDA")

    nombre_operador = st.text_input(
        "Nombre operador",
        value=st.session_state.get("nombre_operador", operador_codigo),
        key="nombre_operador",
    )

    if not nombre_operador.strip():
        st.warning("Debes ingresar el nombre del operador para procesar la tarea")
        st.stop()

    try:
        tareas = api_get_tasks_by_operator_grouped_by_sku(nombre_operador.strip())
    except Exception as e:
        st.error(f"No se pudo cargar tareas: {e}")
        st.stop()

    tareas = safe_df(tareas)
    st.metric("Mis SKUs pendientes", len(tareas))

    if tareas.empty:
        st.info("No tienes tareas pendientes")
        st.stop()

    tareas["label"] = tareas.apply(lambda r: f"{r['sku']} | {r['titulo']} | {r.get('publicaciones_count', 0)} publicaciones", axis=1)
    selected_label = st.selectbox("Selecciona SKU", tareas["label"].tolist())
    fila = tareas[tareas["label"] == selected_label].iloc[0]

    st.markdown("### Información actual")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {fila['sku']}")
        st.markdown(f"**Título:** {fila['titulo']}")
        st.markdown(f"**Publicaciones asociadas:** {fila.get('publicaciones_count', '')}")
        st.markdown(f"**Operador:** {nombre_operador.strip()}")
    with c2:
        st.markdown(f"**Peso ML:** {fila.get('peso_ml_kg', '')}")
        st.markdown(
            f"**Dimensiones ML:** {fila.get('alto_ml_cm', '')} x {fila.get('ancho_ml_cm', '')} x {fila.get('profundidad_ml_cm', '')}"
        )
        st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)

    with st.form("form_medicion_fast"):
        st.markdown("### Ingresar medidas reales")
        col1, col2 = st.columns(2)
        with col1:
            alto = st.number_input("Alto real (cm)", min_value=0.0, step=0.1, format="%.2f")
            ancho = st.number_input("Ancho real (cm)", min_value=0.0, step=0.1, format="%.2f")
        with col2:
            profundidad = st.number_input("Profundidad real (cm)", min_value=0.0, step=0.1, format="%.2f")
            peso = st.number_input("Peso real (kg)", min_value=0.0, step=0.001, format="%.3f")

        observacion = st.text_area("Observación operador")
        st.markdown("### Fotos de respaldo")
        foto_alto = st.file_uploader("Foto alto", type=["jpg", "jpeg", "png"], key="foto_alto_fast")
        foto_ancho = st.file_uploader("Foto ancho", type=["jpg", "jpeg", "png"], key="foto_ancho_fast")
        foto_profundidad = st.file_uploader("Foto profundidad", type=["jpg", "jpeg", "png"], key="foto_profundidad_fast")
        foto_peso = st.file_uploader("Foto peso", type=["jpg", "jpeg", "png"], key="foto_peso_fast")
        submitted = st.form_submit_button("Guardar medición del SKU y subir fotos", use_container_width=True)

    if submitted:
        faltantes = []
        if foto_alto is None:
            faltantes.append("alto")
        if foto_ancho is None:
            faltantes.append("ancho")
        if foto_profundidad is None:
            faltantes.append("profundidad")
        if foto_peso is None:
            faltantes.append("peso")

        if faltantes:
            st.error(f"Faltan fotos obligatorias: {', '.join(faltantes)}")
            st.stop()

        try:
            result = api_save_measurement_with_photos_by_sku(
                sku=str(fila["sku"]),
                operador=nombre_operador.strip(),
                alto_real_cm=float(alto),
                ancho_real_cm=float(ancho),
                profundidad_real_cm=float(profundidad),
                peso_real_kg=float(peso),
                observacion_operador=observacion,
                foto_alto=foto_alto,
                foto_ancho=foto_ancho,
                foto_profundidad=foto_profundidad,
                foto_peso=foto_peso,
            )
            clear_caches()
            st.success(
                f"Medición SKU guardada. Publicaciones afectadas: {result.get('publicaciones_afectadas', 0)} | ID: {result.get('medicion_id', '')}"
            )
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo guardar la medición/fotos: {e}")


# =========================================================
# SUPERVISOR
# =========================================================
elif modo == "Supervisor":
    st.title("Módulo Supervisor")

    try:
        pendientes = api_get_pending_validation_grouped_by_sku(limit=300)
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja: {e}")
        st.stop()

    pendientes = safe_df(pendientes)
    st.metric("SKUs pendientes validación", len(pendientes))

    if pendientes.empty:
        st.info("No hay mediciones pendientes de validación")
        st.stop()

    pendientes["label"] = pendientes.apply(lambda r: f"{r['sku']} | {r['titulo']} | {r.get('publicaciones_count', 0)} publicaciones", axis=1)
    selected_label = st.selectbox("SKU a revisar", pendientes["label"].tolist())
    fila = pendientes[pendientes["label"] == selected_label].iloc[0]

    fallback_case = fila.to_dict()
    detail = {}
    try:
        detail = api_get_case_detail_by_sku(str(fila["sku"]))
    except Exception:
        detail = {}

    case = normalize_case_payload(detail, fallback_case)
    render_case_summary(case)

    comp = pd.DataFrame(
        [
            ["Alto", case.get("alto_ml_cm", ""), case.get("alto_real_cm", "")],
            ["Ancho", case.get("ancho_ml_cm", ""), case.get("ancho_real_cm", "")],
            ["Profundidad", case.get("profundidad_ml_cm", ""), case.get("profundidad_real_cm", "")],
            ["Peso", case.get("peso_ml_kg", ""), case.get("peso_real_kg", "")],
        ],
        columns=["Campo", "ML", "Real"],
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)

    evid_key = f"show_evid_supervisor_{fila['sku']}"
    st.button(
        "Ver evidencias" if not st.session_state.get(evid_key, False) else "Ocultar evidencias",
        use_container_width=False,
        on_click=toggle_evidencias,
        args=(evid_key,),
        key=f"btn_{evid_key}",
    )

    if st.session_state.get(evid_key, False):
        try:
            evidencias = api_get_evidencias_by_sku(str(fila["sku"]))
        except Exception:
            evidencias = pd.DataFrame(detail.get("evidencias", []))
        render_evidencias(evidencias)

    with st.form(f"supervisor_action_form_{fila['sku']}"):
        comentario = st.text_area("Comentario supervisor", key=f"comentario_supervisor_{fila['sku']}")
        c1, c2 = st.columns(2)
        aprobar = c1.form_submit_button("Aprobar SKU", use_container_width=True)
        devolver = c2.form_submit_button("Solicitar nueva evidencia", use_container_width=True)

    if aprobar:
        try:
            result = api_validate_measurement_by_sku(str(fila["sku"]), usuario_actual, True, comentario)
            clear_caches()
            st.success(f"SKU aprobado. Publicaciones afectadas: {result.get('affected', 0)}")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo aprobar: {e}")

    if devolver:
        try:
            result = api_validate_measurement_by_sku(
                str(fila["sku"]),
                usuario_actual,
                False,
                comentario or "Se solicita nueva evidencia",
            )
            clear_caches()
            st.warning(f"SKU devuelto a medición. Publicaciones afectadas: {result.get('affected', 0)}")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo devolver: {e}")


# =========================================================
# ADMINISTRATIVA
# =========================================================
elif modo == "Administrativa":
    st.title("Panel Administrativa")
    bandeja = st.radio("Bandeja", list(BANDEJAS_ADMINISTRATIVA.keys()), horizontal=True)
    estados_bandeja = BANDEJAS_ADMINISTRATIVA[bandeja]

    try:
        cola = api_get_administrative_queue(estados_bandeja, limit=400)
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja: {e}")
        st.stop()

    cola = safe_df(cola)
    st.metric("Casos en bandeja", len(cola))

    if cola.empty:
        st.info("No hay casos en esta bandeja")
        st.stop()

    with st.form("administrativa_filter_form"):
        texto = st.text_input("Buscar SKU / MLC / título")
        filtro_submit = st.form_submit_button("Aplicar búsqueda", use_container_width=False)

    admina_state = st.session_state.setdefault("administrativa_texto", "")
    if filtro_submit:
        st.session_state["administrativa_texto"] = texto.strip()
        admina_state = texto.strip()
    else:
        admina_state = st.session_state.get("administrativa_texto", "")

    if admina_state:
        mask = (
            cola["sku"].astype(str).str.contains(admina_state, case=False, na=False)
            | cola["mlc"].astype(str).str.contains(admina_state, case=False, na=False)
            | cola["titulo"].astype(str).str.contains(admina_state, case=False, na=False)
        )
        cola = cola[mask]

    st.subheader("Bandeja de trabajo")
    cols = [
        c for c in [
            "sku", "mlc", "titulo", "estado_actual", "operador_asignado", "supervisor",
            "fecha_validacion", "ticket_ejecutivo"
        ] if c in cola.columns
    ]
    st.dataframe(cola[cols], use_container_width=True, hide_index=True)

    if bandeja == "Pendientes por gestionar":
        st.markdown("### Exportar Excel para ejecutiva")
        seller_id_default = st.secrets.get("SELLER_ID", "")
        with st.form("export_ejecutiva_form"):
            seller_id = st.text_input("seller_id", value=str(seller_id_default), key="seller_id_export_fast")
            export_cols = [c for c in ["sku", "mlc", "titulo", "alto_real_cm", "ancho_real_cm", "profundidad_real_cm", "peso_real_kg"] if c in cola.columns]
            export_editor = st.data_editor(
                cola[export_cols].assign(seleccionar=False),
                use_container_width=True,
                hide_index=True,
                column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
                disabled=export_cols,
                key="admin_export_editor_fast",
            )
            preparar_excel = st.form_submit_button("Preparar Excel ejecutiva", use_container_width=True)

        if preparar_excel:
            seleccionados_export = export_editor[export_editor["seleccionar"] == True]  # noqa: E712
            if not seller_id.strip():
                st.error("Debes ingresar seller_id para generar el Excel.")
            elif seleccionados_export.empty:
                st.error("Debes seleccionar al menos un producto.")
            else:
                excel_bytes = build_ejecutiva_excel_bytes(seleccionados_export, seller_id.strip())
                st.download_button(
                    "Descargar Excel ejecutiva",
                    data=excel_bytes,
                    file_name="packaging_para_ejecutiva.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key="download_excel_ejecutiva_fast",
                )

    cola["label"] = cola.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)
    selected_label = st.selectbox("Caso", cola["label"].tolist())
    fila = cola[cola["label"] == selected_label].iloc[0]

    fallback_case = fila.to_dict()
    detail = {}
    try:
        detail = api_get_case_detail(str(fila["sku"]), str(fila["mlc"]))
    except Exception:
        detail = {}

    case = normalize_case_payload(detail, fallback_case)
    render_case_summary(case)

    evid_key = f"show_evid_admina_{fila['sku']}_{fila['mlc']}"
    st.button(
        "Ver evidencias" if not st.session_state.get(evid_key, False) else "Ocultar evidencias",
        use_container_width=False,
        on_click=toggle_evidencias,
        args=(evid_key,),
        key=f"btn_{evid_key}",
    )

    if st.session_state.get(evid_key, False):
        try:
            evidencias = api_get_evidencias(str(fila["sku"]), str(fila["mlc"]))
        except Exception:
            evidencias = pd.DataFrame(detail.get("evidencias", []))
        render_evidencias(evidencias)

    st.markdown("### Acción administrativa")
    estado_actual = str(case.get("estado_actual", ""))
    opciones = ESTADOS_CIERRE.copy()
    if estado_actual == "validado_supervisor":
        opciones = ["listo_para_ejecutivo", "en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual == "listo_para_ejecutivo":
        opciones = ["en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual == "en_gestion_ejecutivo":
        opciones = ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual in ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]:
        opciones = [estado_actual]

    with st.form(f"administrativa_action_form_{fila['sku']}_{fila['mlc']}"):
        nuevo_estado = st.selectbox("Nuevo estado", opciones)
        ticket_default = str(detail.get("case", {}).get("ticket_ejecutivo", "")) if isinstance(detail.get("case"), dict) else str(case.get("ticket_ejecutivo", ""))
        ticket = st.text_input("Ticket ejecutivo", value=ticket_default)
        comentario = st.text_area("Comentario", height=120)
        guardar_gestion = st.form_submit_button("Guardar gestión", use_container_width=True)

    if guardar_gestion:
        requiere_ticket = nuevo_estado in ["listo_para_ejecutivo", "en_gestion_ejecutivo"]
        if not comentario.strip():
            st.error("El comentario es obligatorio")
            st.stop()
        if requiere_ticket and not ticket.strip():
            st.error("Debes ingresar ticket ejecutivo para este estado")
            st.stop()
        try:
            api_update_status(
                sku=str(fila["sku"]),
                mlc=str(fila["mlc"]),
                nuevo_estado=nuevo_estado,
                usuario=usuario_actual,
                comentario=comentario.strip(),
                ticket_ejecutivo=ticket.strip(),
            )
            clear_caches()
            st.success(f"Caso actualizado a {nuevo_estado}")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo actualizar el caso: {e}")
