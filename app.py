import base64
import gc
import hashlib
import hmac
import io
import json
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from PIL import Image, ImageOps


st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")

APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")
SESSION_TOKEN_SECRET = str(st.secrets.get("SESSION_TOKEN_SECRET", "") or f"{APPS_SCRIPT_URL}|control-medidas-ml")
SESSION_TOKEN_DAYS = int(st.secrets.get("SESSION_TOKEN_DAYS", 14))
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
                build_photo_payload(foto_alto, "alto"),
                build_photo_payload(foto_ancho, "ancho"),
                build_photo_payload(foto_profundidad, "profundidad"),
                build_photo_payload(foto_peso, "peso"),
            ],
        },
        timeout=180,
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
                build_photo_payload(foto_alto, "alto"),
                build_photo_payload(foto_ancho, "ancho"),
                build_photo_payload(foto_profundidad, "profundidad"),
                build_photo_payload(foto_peso, "peso"),
            ],
        },
        timeout=180,
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


def normalize_identifier(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    return text


def api_bulk_update_status(items: List[Dict[str, str]], nuevo_estado: str, usuario: str) -> Dict[str, Any]:
    normalized_items: List[Dict[str, str]] = []
    for item in items:
        sku = normalize_identifier(item.get("sku", ""))
        mlc = normalize_identifier(item.get("mlc", ""))
        if sku and mlc:
            normalized_items.append({"sku": sku, "mlc": mlc})
    if not normalized_items:
        raise RuntimeError("No hay items válidos para actualizar")
    return api_post(
        {
            "action": "bulk_update_status",
            "items": normalized_items,
            "nuevo_estado": nuevo_estado,
            "usuario": usuario,
        },
        timeout=180,
    )


# =========================================================
# HELPERS
# =========================================================
def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def build_persistent_session_token(user: Dict[str, Any]) -> str:
    payload = {
        "user": user,
        "exp": int(time.time()) + (SESSION_TOKEN_DAYS * 86400),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(SESSION_TOKEN_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"


def restore_user_from_session_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        payload_b64, signature = token.split(".", 1)
        expected_signature = hmac.new(SESSION_TOKEN_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            return None
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        user = payload.get("user")
        if not isinstance(user, dict) or not user:
            return None
        return user
    except Exception:
        return None


def persist_session_token(user: Dict[str, Any]) -> None:
    st.query_params["session_token"] = build_persistent_session_token(user)


def clear_persistent_session_token() -> None:
    try:
        del st.query_params["session_token"]
    except Exception:
        st.query_params["session_token"] = ""


def compress_image_upload(
    uploaded_file,
    max_size: int = 960,
    quality: int = 55,
    target_size_kb: int = 450,
    min_quality: int = 42,
    min_size: int = 720,
) -> Dict[str, Any]:
    raw_bytes = uploaded_file.getvalue()
    source_size_kb = round(len(raw_bytes) / 1024, 1)

    with Image.open(io.BytesIO(raw_bytes)) as image:
        image = ImageOps.exif_transpose(image).convert("RGB")
        current_max = max_size
        current_quality = quality
        optimized_bytes = b""

        while True:
            working = image.copy()
            working.thumbnail((current_max, current_max))

            buffer = io.BytesIO()
            working.save(buffer, format="JPEG", quality=current_quality, optimize=True, progressive=True)
            candidate = buffer.getvalue()
            optimized_bytes = candidate
            candidate_size_kb = len(candidate) / 1024

            if candidate_size_kb <= target_size_kb:
                break
            if current_quality > min_quality:
                current_quality = max(min_quality, current_quality - 5)
                continue
            if current_max > min_size:
                current_max = max(min_size, current_max - 120)
                continue
            break

    del raw_bytes
    gc.collect()

    return {
        "file_base64": base64.b64encode(optimized_bytes).decode("utf-8"),
        "mime_type": "image/jpeg",
        "file_name": f"{uploaded_file.name.rsplit('.', 1)[0]}.jpg",
        "size_kb": round(len(optimized_bytes) / 1024, 1),
        "source_size_kb": source_size_kb,
    }


def build_photo_payload(uploaded_file, tipo: str) -> Dict[str, Any]:
    compressed = compress_image_upload(uploaded_file)
    return {
        "tipo": tipo,
        "file_base64": compressed["file_base64"],
        "mime_type": compressed["mime_type"],
        "file_name": compressed["file_name"],
        "size_kb": compressed["size_kb"],
        "source_size_kb": compressed["source_size_kb"],
    }


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


def get_allowed_admin_status_transitions(estado_actual: str) -> List[str]:
    estado_actual = str(estado_actual or "").strip()
    if estado_actual == "validado_supervisor":
        return ["listo_para_ejecutivo", "en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    if estado_actual == "listo_para_ejecutivo":
        return ["en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    if estado_actual == "en_gestion_ejecutivo":
        return ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    if estado_actual in ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]:
        return [estado_actual]
    return ESTADOS_CIERRE.copy()


def validate_admin_status_change(
    estado_actual: str,
    nuevo_estado: str,
    comentario: str,
    ticket_ejecutivo: str = "",
) -> Optional[str]:
    estado_actual = str(estado_actual or "").strip()
    nuevo_estado = str(nuevo_estado or "").strip()
    comentario = str(comentario or "").strip()
    ticket_ejecutivo = str(ticket_ejecutivo or "").strip()

    if not comentario:
        return "El comentario es obligatorio"

    if estado_actual in ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]:
        return "El caso ya está cerrado"

    permitidos = get_allowed_admin_status_transitions(estado_actual)
    if nuevo_estado not in permitidos:
        return f"La transición desde {estado_actual} hacia {nuevo_estado} no está permitida"

    requiere_ticket = nuevo_estado in ["listo_para_ejecutivo", "en_gestion_ejecutivo"]
    if requiere_ticket and not ticket_ejecutivo:
        return "Debes ingresar ticket ejecutivo para este estado"

    return None


def validate_bulk_admin_status_change(
    casos: List[Dict[str, Any]],
    nuevo_estado: str,
    comentario: str,
    ticket_ejecutivo: str = "",
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    validos: List[Dict[str, Any]] = []
    bloqueados: List[Dict[str, Any]] = []

    for caso in casos:
        error = validate_admin_status_change(
            estado_actual=str(caso.get("estado_actual", "")),
            nuevo_estado=nuevo_estado,
            comentario=comentario,
            ticket_ejecutivo=ticket_ejecutivo,
        )
        if error:
            bloqueados.append({"caso": caso, "motivo": error})
        else:
            validos.append(caso)

    return validos, bloqueados


def refresh_supervisor_queue(limit: int = 300, force: bool = False) -> pd.DataFrame:
    cache_key = "supervisor_queue_df"
    version_key = "supervisor_queue_version"
    current_version = st.session_state.get(version_key, 0)
    cached_df = st.session_state.get(cache_key)
    cached_version = st.session_state.get(f"{cache_key}_version")

    if (not force) and isinstance(cached_df, pd.DataFrame) and cached_version == current_version:
        return cached_df.copy()

    df = safe_df(api_get_pending_validation_grouped_by_sku(limit=limit))
    st.session_state[cache_key] = df.copy()
    st.session_state[f"{cache_key}_version"] = current_version
    return df


def bump_supervisor_queue_version() -> None:
    st.session_state["supervisor_queue_version"] = st.session_state.get("supervisor_queue_version", 0) + 1


def remove_supervisor_sku_from_queue(sku: str) -> None:
    cache_key = "supervisor_queue_df"
    cached_df = st.session_state.get(cache_key)
    if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
        remaining = cached_df[cached_df["sku"].astype(str) != str(sku)].reset_index(drop=True)
        st.session_state[cache_key] = remaining


def get_admin_filter_signature(query: str, estados: Optional[List[str]], operador: str) -> tuple:
    estados_norm = tuple(sorted(str(x) for x in (estados or [])))
    return (str(query or "").strip(), estados_norm, str(operador or "").strip())


def refresh_admin_queues(
    query: str = "",
    estados: Optional[List[str]] = None,
    operador: str = "",
    force: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    signature = get_admin_filter_signature(query, estados, operador)
    version = st.session_state.get("admin_queue_version", 0)
    cached_signature = st.session_state.get("admin_queue_signature")
    cached_version = st.session_state.get("admin_queue_cached_version")
    cached_pub = st.session_state.get("admin_queue_pub_df")
    cached_sku = st.session_state.get("admin_queue_sku_df")

    if (
        not force
        and cached_signature == signature
        and cached_version == version
        and isinstance(cached_pub, pd.DataFrame)
        and isinstance(cached_sku, pd.DataFrame)
    ):
        return cached_pub.copy(), cached_sku.copy()

    df_pub = safe_df(api_get_admin_queue(query=query, estados=estados, operador=operador))
    df_sku = safe_df(api_get_admin_queue_grouped_by_sku(query=query, estados=estados, operador=operador))

    st.session_state["admin_queue_signature"] = signature
    st.session_state["admin_queue_cached_version"] = version
    st.session_state["admin_queue_pub_df"] = df_pub.copy()
    st.session_state["admin_queue_sku_df"] = df_sku.copy()
    return df_pub, df_sku


def update_admin_queue_after_assignment(selected_skus: List[str], operador_destino: str) -> None:
    selected_skus_str = {str(x) for x in selected_skus}
    if not selected_skus_str:
        return

    operador_filter = str(st.session_state.get("admin_filters_state", {}).get("operador_filter", "") or "").strip()
    keep_rows = (not operador_filter) or (operador_filter.casefold() == str(operador_destino).strip().casefold())

    pub_df = st.session_state.get("admin_queue_pub_df")
    if isinstance(pub_df, pd.DataFrame) and not pub_df.empty and "sku" in pub_df.columns:
        mask_pub = pub_df["sku"].astype(str).isin(selected_skus_str)
        pub_new = pub_df.copy()
        if keep_rows:
            if "operador_asignado" in pub_new.columns:
                pub_new.loc[mask_pub, "operador_asignado"] = str(operador_destino).strip()
        else:
            pub_new = pub_new.loc[~mask_pub].reset_index(drop=True)
        st.session_state["admin_queue_pub_df"] = pub_new

    sku_df = st.session_state.get("admin_queue_sku_df")
    if isinstance(sku_df, pd.DataFrame) and not sku_df.empty and "sku" in sku_df.columns:
        mask_sku = sku_df["sku"].astype(str).isin(selected_skus_str)
        sku_new = sku_df.copy()
        if keep_rows:
            if "operador_asignado" in sku_new.columns:
                sku_new.loc[mask_sku, "operador_asignado"] = str(operador_destino).strip()
        else:
            sku_new = sku_new.loc[~mask_sku].reset_index(drop=True)
        st.session_state["admin_queue_sku_df"] = sku_new


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
    auth_user = st.session_state.get("auth_user")
    if isinstance(auth_user, dict) and auth_user:
        return auth_user

    token = str(st.query_params.get("session_token", "") or "").strip()
    if token:
        restored_user = restore_user_from_session_token(token)
        if restored_user:
            st.session_state["auth_user"] = restored_user
            return restored_user
        clear_persistent_session_token()

    st.title("Control Medidas ML")
    st.subheader("Ingreso con PIN")
    st.caption("En celular, la sesión intentará mantenerse aunque el navegador recargue al sacar o adjuntar fotos.")
    with st.form("login_form"):
        usuario = st.text_input("Usuario")
        pin = st.text_input("PIN", type="password")
        submitted = st.form_submit_button("Ingresar", use_container_width=True)
    if submitted:
        try:
            auth = api_login_with_pin(usuario, pin)
            user = auth.get("user", {})
            st.session_state["auth_user"] = user
            persist_session_token(user)
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo iniciar sesión: {e}")
    st.stop()


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
        clear_persistent_session_token()
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
        df_filtrado_pub, df_filtrado_sku = refresh_admin_queues(
            query=admin_filter_state["texto"],
            estados=admin_filter_state["estados_sel"],
            operador=admin_filter_state["operador_filter"],
        )
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja administrativa: {e}")
        st.stop()

    df_filtrado_pub = safe_df(df_filtrado_pub)
    df_filtrado_sku = safe_df(df_filtrado_sku)

    if "ventas" not in df_filtrado_sku.columns:
        df_filtrado_sku["ventas"] = df_filtrado_sku.get("ventas_total", "")

    if df_filtrado_sku.empty:
        st.info("No hay SKUs para los filtros seleccionados")
        st.stop()

    st.caption(f"Resultados encontrados: {len(df_filtrado_sku)} SKUs | {len(df_filtrado_pub)} publicaciones")

    st.subheader("Asignación de tareas por SKU")
    with st.form("admin_assign_form"):
        operador_destino = st.text_input("Asignar a operador", value="")
        cols_view = [c for c in ["sku", "titulo", "ventas", "estado_actual", "operador_asignado", "publicaciones_count"] if c in df_filtrado_sku.columns]
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
                    api_get_dashboard_counts.clear()
                    api_get_tasks_by_operator.clear()
                    api_get_tasks_by_operator_grouped_by_sku.clear()
                    update_admin_queue_after_assignment(
                        [str(x) for x in seleccionados["sku"].astype(str).tolist()],
                        operador_destino.strip(),
                    )
                    st.success(f"Publicaciones afectadas por asignación: {result.get('assigned', 0)}")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo asignar: {e}")

    st.subheader("Precios resueltos")
    df_resueltos = safe_df(
        api_get_admin_queue(
            query="",
            estados=["resuelto"],
            operador="",
            limit=1000000,
        )
    )

    if df_resueltos.empty:
        st.info("No hay casos resueltos pendientes de marcar como precio_actualizado")
    else:
        cols_precio = [c for c in ["sku", "mlc", "titulo", "ventas", "estado_actual", "fecha_resolucion"] if c in df_resueltos.columns]
        with st.form("admin_precio_actualizado_form"):
            precio_editor = st.data_editor(
                df_resueltos[cols_precio].assign(seleccionar=False),
                use_container_width=True,
                hide_index=True,
                column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
                disabled=cols_precio,
                key="admin_editor_precio_actualizado",
            )
            marcar_precio_btn = st.form_submit_button("Marcar seleccionados como precio_actualizado", use_container_width=True)

        if marcar_precio_btn:
            seleccionados_precio = precio_editor[precio_editor["seleccionar"] == True]  # noqa: E712
            if seleccionados_precio.empty:
                st.warning("No seleccionaste casos resueltos")
            else:
                items_precio = [
                    {
                        "sku": normalize_identifier(row_precio.get("sku", "")),
                        "mlc": normalize_identifier(row_precio.get("mlc", "")),
                    }
                    for _, row_precio in seleccionados_precio.iterrows()
                ]
                try:
                    result_precio = api_bulk_update_status(items_precio, "precio_actualizado", usuario_actual)
                    api_get_dashboard_counts.clear()
                    api_get_admin_queue.clear()
                    api_get_admin_queue_grouped_by_sku.clear()
                    api_get_administrative_queue.clear()
                    actualizados = int(result_precio.get("updated", 0))
                    errores = int(result_precio.get("errors", 0))
                    st.success(f"Precios actualizados: {actualizados}")
                    if errores:
                        st.warning(f"Registros con error: {errores}")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo marcar precio_actualizado: {e}")

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

    if "operador_form_nonce" not in st.session_state:
        st.session_state["operador_form_nonce"] = 0
    form_nonce = st.session_state["operador_form_nonce"]

    st.markdown("### Información actual")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {fila['sku']}")
        st.markdown(f"**Título:** {fila['titulo']}")
        st.markdown(f"**Publicaciones asociadas:** {fila.get('publicaciones_count', '')}")
        st.markdown(f"**Operador:** {nombre_operador.strip()}")
    with c2:
        st.markdown(f"**Peso ML:** {fila.get('peso_ml_kg', '')} kg")
        st.markdown(
            f"**Dimensiones ML:** {fila.get('alto_ml_cm', '')} x {fila.get('ancho_ml_cm', '')} x {fila.get('profundidad_ml_cm', '')} cm"
        )
        st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)

    with st.form("form_medicion_fast"):
        st.markdown("### Ingresar medidas reales")
        col1, col2 = st.columns(2)
        with col1:
            alto = st.number_input(
                "Alto real (cm)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key=f"alto_real_fast_{form_nonce}",
            )
            ancho = st.number_input(
                "Ancho real (cm)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key=f"ancho_real_fast_{form_nonce}",
            )
        with col2:
            profundidad = st.number_input(
                "Profundidad real (cm)",
                min_value=0.0,
                step=0.1,
                format="%.2f",
                key=f"profundidad_real_fast_{form_nonce}",
            )
            peso = st.number_input(
                "Peso real (kg)",
                min_value=0.0,
                step=0.001,
                format="%.3f",
                key=f"peso_real_fast_{form_nonce}",
            )

        observacion = st.text_area("Observación operador", key=f"observacion_operador_fast_{form_nonce}")
        st.markdown("### Fotos de respaldo")
        foto_alto = st.file_uploader("Foto alto", type=["jpg", "jpeg", "png"], key=f"foto_alto_fast_{form_nonce}")
        foto_ancho = st.file_uploader("Foto ancho", type=["jpg", "jpeg", "png"], key=f"foto_ancho_fast_{form_nonce}")
        foto_profundidad = st.file_uploader("Foto profundidad", type=["jpg", "jpeg", "png"], key=f"foto_profundidad_fast_{form_nonce}")
        foto_peso = st.file_uploader("Foto peso", type=["jpg", "jpeg", "png"], key=f"foto_peso_fast_{form_nonce}")
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
            st.session_state["operador_form_nonce"] = st.session_state.get("operador_form_nonce", 0) + 1
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
        pendientes = refresh_supervisor_queue(limit=300)
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja: {e}")
        st.stop()

    pendientes = safe_df(pendientes)
    st.metric("SKUs pendientes validación", len(pendientes))

    if pendientes.empty:
        st.info("No hay mediciones pendientes de validación")
        st.stop()

    pendientes["label"] = pendientes.apply(lambda r: f"{r['sku']} | {r['titulo']} | {r.get('publicaciones_count', 0)} publicaciones", axis=1)
    labels = pendientes["label"].tolist()

    selected_label_key = "supervisor_selected_label"
    if st.session_state.get(selected_label_key) not in labels:
        st.session_state[selected_label_key] = labels[0]

    selected_label = st.selectbox("SKU a revisar", labels, key=selected_label_key)
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
            ["Alto (cm)", case.get("alto_ml_cm", ""), case.get("alto_real_cm", "")],
            ["Ancho (cm)", case.get("ancho_ml_cm", ""), case.get("ancho_real_cm", "")],
            ["Profundidad (cm)", case.get("profundidad_ml_cm", ""), case.get("profundidad_real_cm", "")],
            ["Peso (kg)", case.get("peso_ml_kg", ""), case.get("peso_real_kg", "")],
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
            remove_supervisor_sku_from_queue(str(fila["sku"]))
            pendientes_restantes = st.session_state.get("supervisor_queue_df", pd.DataFrame())
            if isinstance(pendientes_restantes, pd.DataFrame) and not pendientes_restantes.empty:
                pendientes_restantes = pendientes_restantes.copy()
                pendientes_restantes["label"] = pendientes_restantes.apply(
                    lambda r: f"{r['sku']} | {r['titulo']} | {r.get('publicaciones_count', 0)} publicaciones",
                    axis=1,
                )
                st.session_state[selected_label_key] = pendientes_restantes.iloc[0]["label"]
            else:
                st.session_state.pop(selected_label_key, None)
            bump_supervisor_queue_version()
            api_get_dashboard_counts.clear()
            api_get_case_detail.clear()
            api_get_case_detail_by_sku.clear()
            api_get_pending_validation.clear()
            api_get_pending_validation_grouped_by_sku.clear()
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
            remove_supervisor_sku_from_queue(str(fila["sku"]))
            pendientes_restantes = st.session_state.get("supervisor_queue_df", pd.DataFrame())
            if isinstance(pendientes_restantes, pd.DataFrame) and not pendientes_restantes.empty:
                pendientes_restantes = pendientes_restantes.copy()
                pendientes_restantes["label"] = pendientes_restantes.apply(
                    lambda r: f"{r['sku']} | {r['titulo']} | {r.get('publicaciones_count', 0)} publicaciones",
                    axis=1,
                )
                st.session_state[selected_label_key] = pendientes_restantes.iloc[0]["label"]
            else:
                st.session_state.pop(selected_label_key, None)
            bump_supervisor_queue_version()
            api_get_dashboard_counts.clear()
            api_get_case_detail.clear()
            api_get_case_detail_by_sku.clear()
            api_get_pending_validation.clear()
            api_get_pending_validation_grouped_by_sku.clear()
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

    st.markdown("### Cambio masivo de estado")
    bulk_cols = [c for c in cols if c in cola.columns]
    bulk_editor = st.data_editor(
        cola[bulk_cols].assign(seleccionar=False),
        use_container_width=True,
        hide_index=True,
        column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
        disabled=bulk_cols,
        key=f"administrativa_bulk_editor_{bandeja}",
    )
    seleccionados_bulk = bulk_editor[bulk_editor["seleccionar"] == True].copy()  # noqa: E712

    estado_bulk = st.selectbox(
        "Nuevo estado masivo",
        ["listo_para_ejecutivo", "en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"],
        key=f"administrativa_bulk_estado_{bandeja}",
    )
    requiere_ticket_bulk = estado_bulk in ["listo_para_ejecutivo", "en_gestion_ejecutivo"]
    ticket_bulk = st.text_input(
        "Ticket ejecutivo masivo",
        key=f"administrativa_bulk_ticket_{bandeja}",
        help="Obligatorio cuando el estado masivo es listo_para_ejecutivo o en_gestion_ejecutivo.",
    )
    comentario_bulk = st.text_area(
        "Comentario masivo",
        height=100,
        key=f"administrativa_bulk_comentario_{bandeja}",
    )

    bulk_state_key = f"administrativa_bulk_validation_{bandeja}"
    c_bulk1, c_bulk2 = st.columns(2)
    with c_bulk1:
        validar_bulk = st.button("Validar selección masiva", use_container_width=True, key=f"btn_validar_bulk_{bandeja}")
    with c_bulk2:
        confirmar_bulk = st.button("Confirmar cambio masivo", use_container_width=True, key=f"btn_confirmar_bulk_{bandeja}")

    if validar_bulk:
        if seleccionados_bulk.empty:
            st.error("Debes seleccionar al menos un caso para el cambio masivo")
            st.session_state.pop(bulk_state_key, None)
        else:
            casos_bulk = seleccionados_bulk.drop(columns=["seleccionar"], errors="ignore").to_dict("records")
            validos_bulk, bloqueados_bulk = validate_bulk_admin_status_change(
                casos=casos_bulk,
                nuevo_estado=estado_bulk,
                comentario=comentario_bulk,
                ticket_ejecutivo=ticket_bulk,
            )
            st.session_state[bulk_state_key] = {
                "nuevo_estado": estado_bulk,
                "ticket": ticket_bulk.strip(),
                "comentario": comentario_bulk.strip(),
                "validos": validos_bulk,
                "bloqueados": bloqueados_bulk,
            }

    bulk_state = st.session_state.get(bulk_state_key)
    if bulk_state:
        st.info(
            f"Validación lista. Válidos: {len(bulk_state.get('validos', []))} | "
            f"Bloqueados: {len(bulk_state.get('bloqueados', []))}"
        )
        if bulk_state.get("bloqueados"):
            bloqueados_df = pd.DataFrame([
                {
                    "sku": str(item.get("caso", {}).get("sku", "")),
                    "mlc": str(item.get("caso", {}).get("mlc", "")),
                    "estado_actual": str(item.get("caso", {}).get("estado_actual", "")),
                    "motivo": str(item.get("motivo", "")),
                }
                for item in bulk_state.get("bloqueados", [])
            ])
            if not bloqueados_df.empty:
                with st.expander("Ver casos bloqueados"):
                    st.dataframe(bloqueados_df, use_container_width=True, hide_index=True)

    if confirmar_bulk:
        bulk_state = st.session_state.get(bulk_state_key)
        if not bulk_state:
            st.error("Primero debes validar la selección masiva")
        elif not bulk_state.get("validos"):
            st.error("No hay casos válidos para actualizar")
        else:
            actualizados = []
            errores_bulk = []
            for caso_bulk in bulk_state.get("validos", []):
                try:
                    api_update_status(
                        sku=str(caso_bulk.get("sku", "")),
                        mlc=str(caso_bulk.get("mlc", "")),
                        nuevo_estado=str(bulk_state.get("nuevo_estado", "")),
                        usuario=usuario_actual,
                        comentario=str(bulk_state.get("comentario", "")),
                        ticket_ejecutivo=str(bulk_state.get("ticket", "")),
                    )
                    actualizados.append(
                        {
                            "sku": str(caso_bulk.get("sku", "")),
                            "mlc": str(caso_bulk.get("mlc", "")),
                            "resultado": "OK",
                        }
                    )
                except Exception as e:
                    errores_bulk.append(
                        {
                            "sku": str(caso_bulk.get("sku", "")),
                            "mlc": str(caso_bulk.get("mlc", "")),
                            "resultado": f"ERROR: {e}",
                        }
                    )

            clear_caches()
            st.session_state.pop(bulk_state_key, None)
            st.success(f"Actualizados correctamente: {len(actualizados)}")
            if errores_bulk:
                st.error(f"Con error: {len(errores_bulk)}")
                st.dataframe(pd.DataFrame(errores_bulk), use_container_width=True, hide_index=True)
            st.rerun()

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
    opciones = get_allowed_admin_status_transitions(estado_actual)

    with st.form(f"administrativa_action_form_{fila['sku']}_{fila['mlc']}"):
        nuevo_estado = st.selectbox("Nuevo estado", opciones)
        ticket_default = str(detail.get("case", {}).get("ticket_ejecutivo", "")) if isinstance(detail.get("case"), dict) else str(case.get("ticket_ejecutivo", ""))
        ticket = st.text_input("Ticket ejecutivo", value=ticket_default)
        comentario = st.text_area("Comentario", height=120)
        guardar_gestion = st.form_submit_button("Guardar gestión", use_container_width=True)

    if guardar_gestion:
        error_validacion = validate_admin_status_change(
            estado_actual=estado_actual,
            nuevo_estado=nuevo_estado,
            comentario=comentario,
            ticket_ejecutivo=ticket,
        )
        if error_validacion:
            st.error(error_validacion)
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
