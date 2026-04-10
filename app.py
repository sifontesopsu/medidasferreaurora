import base64
import io
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side


st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")

APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")
PRIORIDADES = ["alta", "media", "baja"]
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


@st.cache_data(ttl=20, show_spinner=False)
def api_get_all_products() -> pd.DataFrame:
    data = api_post({"action": "get_all_products"}, timeout=180)
    return pd.DataFrame(data.get("data", []))


@st.cache_data(ttl=10, show_spinner=False)
def api_get_tasks_by_operator(operador: str) -> pd.DataFrame:
    data = api_post({"action": "get_tasks_by_operator", "operador": operador}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=10, show_spinner=False)
def api_get_pending_validation(limit: int = 200) -> pd.DataFrame:
    data = api_post({"action": "get_pending_validation", "limit": limit}, timeout=120)
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=10, show_spinner=False)
def api_get_administrative_queue(statuses: List[str], limit: int = 300) -> pd.DataFrame:
    data = api_post(
        {"action": "get_administrative_queue", "statuses": statuses, "limit": limit},
        timeout=120,
    )
    return pd.DataFrame(data.get("items", []))


@st.cache_data(ttl=10, show_spinner=False)
def api_get_case_detail(sku: str, mlc: str) -> Dict[str, Any]:
    return api_post({"action": "get_case_detail", "sku": sku, "mlc": mlc}, timeout=180)


def api_login_with_pin(usuario: str, pin: str) -> Dict[str, Any]:
    return api_post({"action": "login_with_pin", "usuario": usuario, "pin": pin}, timeout=60)


def api_assign_tasks(items: List[Dict[str, str]], operador: str, prioridad: str, usuario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "assign_tasks",
            "items": items,
            "operador": operador,
            "prioridad": prioridad,
            "usuario": usuario,
        }
    )


def api_save_measurement(
    sku: str,
    mlc: str,
    operador: str,
    alto_real_cm: float,
    ancho_real_cm: float,
    profundidad_real_cm: float,
    peso_real_kg: float,
    observacion_operador: str,
) -> Dict[str, Any]:
    return api_post(
        {
            "action": "save_measurement",
            "sku": sku,
            "mlc": mlc,
            "operador": operador,
            "alto_real_cm": alto_real_cm,
            "ancho_real_cm": ancho_real_cm,
            "profundidad_real_cm": profundidad_real_cm,
            "peso_real_kg": peso_real_kg,
            "observacion_operador": observacion_operador,
        }
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


def api_upload_photo(
    sku: str,
    mlc: str,
    tipo: str,
    uploaded_file,
    cargado_por: str,
    medicion_id: str = "",
) -> Dict[str, Any]:
    file_bytes = uploaded_file.getvalue()
    file_base64 = base64.b64encode(file_bytes).decode("utf-8")

    return api_post(
        {
            "action": "upload_photo",
            "sku": sku,
            "mlc": mlc,
            "tipo": tipo,
            "file_base64": file_base64,
            "mime_type": uploaded_file.type or "image/jpeg",
            "cargado_por": cargado_por,
            "medicion_id": medicion_id,
        },
        timeout=240,
    )


def api_replace_base_import_ml_raw(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return api_post({"action": "replace_base_import_ml_raw", "rows": rows}, timeout=240)


def api_process_import_ml_to_base(archivo_nombre: str, usuario: str) -> Dict[str, Any]:
    return api_post(
        {
            "action": "process_import_ml_to_base",
            "archivo_nombre": archivo_nombre,
            "usuario": usuario,
        },
        timeout=240,
    )


# =========================================================
# HELPERS
# =========================================================
def clear_caches() -> None:
    api_get_all_products.clear()
    api_get_tasks_by_operator.clear()
    api_get_pending_validation.clear()
    api_get_administrative_queue.clear()
    api_get_case_detail.clear()


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


def show_kpi_row(df: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total", len(df))
    with c2:
        st.metric(
            "Pendiente medición",
            int((df.get("estado_actual", pd.Series(dtype=str)) == "pendiente_medicion").sum()) if not df.empty else 0,
        )
    with c3:
        st.metric(
            "Pendiente validación",
            int((df.get("estado_actual", pd.Series(dtype=str)) == "medido_pendiente_validacion").sum())
            if not df.empty
            else 0,
        )
    with c4:
        st.metric(
            "Aprobados supervisor",
            int((df.get("estado_actual", pd.Series(dtype=str)) == "validado_supervisor").sum())
            if not df.empty
            else 0,
        )


def normalize_excel_value(value: Any) -> Any:
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value


def load_excel_as_rows(uploaded_file) -> List[Dict[str, Any]]:
    df = pd.read_excel(uploaded_file)

    if "SKU" in df.columns:
        df["SKU"] = df["SKU"].apply(lambda x: "" if pd.isna(x) else str(x).replace(".0", "").strip())

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        obj = {col: normalize_excel_value(row[col]) for col in df.columns}
        rows.append(obj)
    return rows


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

    widths = {"A": 14, "B": 14, "C": 20, "D": 20, "E": 22, "F": 22}
    for col_letter, width in widths.items():
        ws.column_dimensions[col_letter].width = width

    ws.row_dimensions[1].height = 22
    ws.freeze_panes = "A3"

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


def render_case_summary(detail: Dict[str, Any]) -> None:
    case = detail.get("case", {})
    if not case:
        st.warning("No se pudo cargar el caso")
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {case.get('sku', '')}")
        st.markdown(f"**MLC:** {case.get('mlc', '')}")
        st.markdown(f"**Título:** {case.get('titulo', '')}")
        st.markdown(f"**Categoría:** {case.get('categoria', '')}")
        st.markdown(badge_estado(str(case.get("estado_actual", ""))), unsafe_allow_html=True)
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


def render_evidencias(evidencias: pd.DataFrame) -> None:
    st.markdown("### Evidencia fotográfica")
    if evidencias.empty:
        st.warning("No hay fotos disponibles")
        return

    evidencias = evidencias.copy()
    evidencias["tipo_foto"] = evidencias["tipo_foto"].astype(str).str.lower()
    evidencias = evidencias.sort_values("fecha_carga").drop_duplicates(subset=["tipo_foto"], keep="last")

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
            link = str(row.get("drive_link", ""))
            if link:
                st.image(link, use_container_width=True)
            else:
                st.warning("Sin link de imagen")


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
        df = api_get_all_products()
    except Exception as e:
        st.error(f"No se pudo leer la API: {e}")
        st.stop()

    df = safe_df(df)
    show_kpi_row(df)

    if df.empty:
        st.warning("No hay productos en base_productos_ml")
        st.stop()

    st.subheader("Filtros")
    f1, f2, f3, f4 = st.columns(4)

    with f1:
        texto = st.text_input("Buscar SKU / MLC / título")
    with f2:
        estados = sorted([x for x in df["estado_actual"].dropna().astype(str).unique().tolist() if x])
        estados_sel = st.multiselect("Estado", estados, default=estados)
    with f3:
        prioridad_vals = sorted([x for x in df.get("prioridad", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        prioridad_sel = st.multiselect("Prioridad", prioridad_vals, default=prioridad_vals)
    with f4:
        operador_vals = sorted([x for x in df.get("operador_asignado", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        operador_filter = st.multiselect("Operador asignado", operador_vals, default=operador_vals)

    df_filtrado = df.copy()
    if texto:
        mask = (
            df_filtrado["sku"].astype(str).str.contains(texto, case=False, na=False)
            | df_filtrado["mlc"].astype(str).str.contains(texto, case=False, na=False)
            | df_filtrado["titulo"].astype(str).str.contains(texto, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]
    if estados_sel:
        df_filtrado = df_filtrado[df_filtrado["estado_actual"].astype(str).isin(estados_sel)]
    if prioridad_sel and "prioridad" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["prioridad"].astype(str).isin(prioridad_sel)]
    if operador_filter and "operador_asignado" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["operador_asignado"].astype(str).isin(operador_filter)]

    st.subheader("Asignación de tareas")
    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        operador_destino = st.text_input("Asignar a operador", value="operador_1")
    with col_b:
        prioridad_destino = st.selectbox("Prioridad nueva", PRIORIDADES, index=0)
    with col_c:
        st.write("")
        st.write("")
        asignar_btn = st.button("Asignar seleccionados", use_container_width=True)

    cols_view = [
        c for c in [
            "sku", "mlc", "titulo", "categoria", "ventas", "visitas",
            "estado_actual", "prioridad", "operador_asignado"
        ] if c in df_filtrado.columns
    ]

    edited = st.data_editor(
        df_filtrado[cols_view].assign(seleccionar=False),
        use_container_width=True,
        hide_index=True,
        column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
        disabled=cols_view,
    )

    if asignar_btn:
        seleccionados = edited[edited["seleccionar"] == True]  # noqa: E712
        if seleccionados.empty:
            st.warning("No seleccionaste productos")
        else:
            items = seleccionados[["sku", "mlc"]].to_dict(orient="records")
            try:
                result = api_assign_tasks(items, operador_destino, prioridad_destino, usuario_actual)
                clear_caches()
                st.success(f"Tareas asignadas: {result.get('assigned', 0)}")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo asignar: {e}")

    with st.expander("Herramientas avanzadas", expanded=False):
        st.caption("Aquí queda la importación ML, fuera del frente operativo diario.")
        uploaded_file = st.file_uploader("Sube Excel real de publicaciones ML", type=["xlsx"], key="admin_import_ml")
        if uploaded_file is not None and st.button("Procesar importación", use_container_width=True, key="btn_import_ml_admin"):
            try:
                rows = load_excel_as_rows(uploaded_file)
                r1 = api_replace_base_import_ml_raw(rows)
                r2 = api_process_import_ml_to_base(uploaded_file.name, usuario_actual)
                clear_caches()
                st.success(
                    f"Importación OK | raw: {r1.get('inserted', 0)} | actualizados: {r2.get('updated', 0)} | "
                    f"nuevos: {r2.get('inserted', 0)} | sin cambios: {r2.get('unchanged', 0)} | omitidos: {r2.get('skipped', 0)}"
                )
            except Exception as e:
                st.error(f"Error importando: {e}")


# =========================================================
# OPERADOR
# =========================================================
elif modo == "Operador":
    st.title("Módulo Operador PDA")

    try:
        tareas = api_get_tasks_by_operator(operador_codigo)
    except Exception as e:
        st.error(f"No se pudo cargar tareas: {e}")
        st.stop()

    tareas = safe_df(tareas)
    st.metric("Mis tareas", len(tareas))

    if tareas.empty:
        st.info("No tienes tareas pendientes")
        st.stop()

    tareas["label"] = tareas.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)
    selected_label = st.selectbox("Selecciona producto", tareas["label"].tolist())
    fila = tareas[tareas["label"] == selected_label].iloc[0]

    st.markdown("### Información actual")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {fila['sku']}")
        st.markdown(f"**MLC:** {fila['mlc']}")
        st.markdown(f"**Título:** {fila['titulo']}")
        st.markdown(f"**Categoría:** {fila.get('categoria', '')}")
    with c2:
        st.markdown(f"**Peso ML:** {fila.get('peso_ml_kg', '')}")
        st.markdown(
            f"**Dimensiones ML:** {fila.get('alto_ml_cm', '')} x {fila.get('ancho_ml_cm', '')} x {fila.get('profundidad_ml_cm', '')}"
        )
        st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)

    with st.form("form_medicion"):
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
        foto_alto = st.file_uploader("Foto alto", type=["jpg", "jpeg", "png"], key="foto_alto")
        foto_ancho = st.file_uploader("Foto ancho", type=["jpg", "jpeg", "png"], key="foto_ancho")
        foto_profundidad = st.file_uploader("Foto profundidad", type=["jpg", "jpeg", "png"], key="foto_profundidad")
        foto_peso = st.file_uploader("Foto peso", type=["jpg", "jpeg", "png"], key="foto_peso")
        submitted = st.form_submit_button("Guardar medición y subir fotos", use_container_width=True)

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
            result_med = api_save_measurement(
                sku=str(fila["sku"]),
                mlc=str(fila["mlc"]),
                operador=operador_codigo,
                alto_real_cm=float(alto),
                ancho_real_cm=float(ancho),
                profundidad_real_cm=float(profundidad),
                peso_real_kg=float(peso),
                observacion_operador=observacion,
            )
            medicion_id = result_med.get("medicion_id", "")
            api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "alto", foto_alto, operador_codigo, medicion_id)
            api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "ancho", foto_ancho, operador_codigo, medicion_id)
            api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "profundidad", foto_profundidad, operador_codigo, medicion_id)
            api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "peso", foto_peso, operador_codigo, medicion_id)
            clear_caches()
            st.success(f"Medición guardada y fotos subidas. ID: {medicion_id}")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo guardar la medición/fotos: {e}")


# =========================================================
# SUPERVISOR
# =========================================================
elif modo == "Supervisor":
    st.title("Módulo Supervisor")

    try:
        pendientes = api_get_pending_validation(limit=300)
    except Exception as e:
        st.error(f"No se pudo cargar la bandeja: {e}")
        st.stop()

    pendientes = safe_df(pendientes)
    st.metric("Pendientes validación", len(pendientes))

    if pendientes.empty:
        st.info("No hay mediciones pendientes de validación")
        st.stop()

    pendientes["label"] = pendientes.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)
    selected_label = st.selectbox("Caso a revisar", pendientes["label"].tolist())
    fila = pendientes[pendientes["label"] == selected_label].iloc[0]

    try:
        detail = api_get_case_detail(str(fila["sku"]), str(fila["mlc"]))
    except Exception as e:
        st.error(f"No se pudo cargar detalle: {e}")
        st.stop()

    render_case_summary(detail)

    comp = pd.DataFrame(
        [
            ["Alto", detail.get("case", {}).get("alto_ml_cm", ""), detail.get("case", {}).get("alto_real_cm", "")],
            ["Ancho", detail.get("case", {}).get("ancho_ml_cm", ""), detail.get("case", {}).get("ancho_real_cm", "")],
            ["Profundidad", detail.get("case", {}).get("profundidad_ml_cm", ""), detail.get("case", {}).get("profundidad_real_cm", "")],
            ["Peso", detail.get("case", {}).get("peso_ml_kg", ""), detail.get("case", {}).get("peso_real_kg", "")],
        ],
        columns=["Campo", "ML", "Real"],
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)
    render_evidencias(pd.DataFrame(detail.get("evidencias", [])))

    comentario = st.text_area("Comentario supervisor", key=f"comentario_supervisor_{fila['sku']}_{fila['mlc']}")
    c1, c2 = st.columns(2)

    with c1:
        if st.button("Aprobar", use_container_width=True, key=f"btn_aprobar_{fila['sku']}_{fila['mlc']}"):
            try:
                api_validate_measurement(str(fila["sku"]), str(fila["mlc"]), usuario_actual, True, comentario)
                clear_caches()
                st.success("Caso aprobado")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo aprobar: {e}")

    with c2:
        if st.button("Solicitar nueva evidencia", use_container_width=True, key=f"btn_devolver_{fila['sku']}_{fila['mlc']}"):
            try:
                api_validate_measurement(
                    str(fila["sku"]),
                    str(fila["mlc"]),
                    usuario_actual,
                    False,
                    comentario or "Se solicita nueva evidencia",
                )
                clear_caches()
                st.warning("Caso devuelto a medición")
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

    texto = st.text_input("Buscar SKU / MLC / título")
    if texto:
        mask = (
            cola["sku"].astype(str).str.contains(texto, case=False, na=False)
            | cola["mlc"].astype(str).str.contains(texto, case=False, na=False)
            | cola["titulo"].astype(str).str.contains(texto, case=False, na=False)
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
        seller_id = st.text_input("seller_id", value=str(seller_id_default), key="seller_id_export")
        export_cols = [c for c in ["sku", "mlc", "titulo", "alto_real_cm", "ancho_real_cm", "profundidad_real_cm", "peso_real_kg"] if c in cola.columns]
        export_editor = st.data_editor(
            cola[export_cols].assign(seleccionar=False),
            use_container_width=True,
            hide_index=True,
            column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
            disabled=export_cols,
            key="admin_export_editor",
        )
        seleccionados_export = export_editor[export_editor["seleccionar"] == True]  # noqa: E712
        if not seleccionados_export.empty and seller_id.strip():
            excel_bytes = build_ejecutiva_excel_bytes(seleccionados_export, seller_id.strip())
            st.download_button(
                "Descargar Excel ejecutiva",
                data=excel_bytes,
                file_name="packaging_para_ejecutiva.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        elif not seller_id.strip():
            st.warning("Debes ingresar seller_id para generar el Excel.")

    cola["label"] = cola.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)
    selected_label = st.selectbox("Caso", cola["label"].tolist())
    fila = cola[cola["label"] == selected_label].iloc[0]

    try:
        detail = api_get_case_detail(str(fila["sku"]), str(fila["mlc"]))
    except Exception as e:
        st.error(f"No se pudo cargar detalle: {e}")
        st.stop()

    render_case_summary(detail)
    render_evidencias(pd.DataFrame(detail.get("evidencias", [])))

    st.markdown("### Acción administrativa")
    estado_actual = str(detail.get("case", {}).get("estado_actual", ""))
    opciones = ESTADOS_CIERRE.copy()
    if estado_actual == "validado_supervisor":
        opciones = ["listo_para_ejecutivo", "en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual == "listo_para_ejecutivo":
        opciones = ["en_gestion_ejecutivo", "resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual == "en_gestion_ejecutivo":
        opciones = ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]
    elif estado_actual in ["resuelto", "rechazado_ml", "rechazado_ejecutivo"]:
        opciones = [estado_actual]

    nuevo_estado = st.selectbox("Nuevo estado", opciones)
    ticket_default = str(detail.get("case", {}).get("ticket_ejecutivo", ""))
    ticket = st.text_input("Ticket ejecutivo", value=ticket_default)
    comentario = st.text_area("Comentario", height=120)

    requiere_ticket = nuevo_estado in ["listo_para_ejecutivo", "en_gestion_ejecutivo"]

    if st.button("Guardar gestión", use_container_width=True):
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
