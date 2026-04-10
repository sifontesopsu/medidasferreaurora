import base64
from io import BytesIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")

APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")
PRIORIDADES = ["alta", "media", "baja"]
ESTADOS_ADMIN = [
    "listo_para_actualizar_medidas",
    "listo_para_ejecutivo",
    "en_gestion_ejecutivo",
    "resuelto",
    "rechazado_ml",
    "rechazado_ejecutivo",
    "requiere_nueva_evidencia",
]


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
    api_get_case_detail.clear()



def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) and not df.empty else pd.DataFrame()



def badge_estado(estado: str) -> str:
    color_map = {
        "pendiente_medicion": "#f59e0b",
        "requiere_nueva_evidencia": "#f97316",
        "medido_pendiente_validacion": "#3b82f6",
        "validado_supervisor": "#10b981",
        "listo_para_actualizar_medidas": "#0ea5e9",
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
            "Resueltos",
            int((df.get("estado_actual", pd.Series(dtype=str)) == "resuelto").sum()) if not df.empty else 0,
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


@st.cache_data(ttl=300, show_spinner=False)
def fetch_image_bytes(url: str) -> Optional[bytes]:
    if not url:
        return None
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content



def render_evidence_gallery(evidencias_df: pd.DataFrame) -> bool:
    orden = ["alto", "ancho", "profundidad", "peso"]
    faltan_fotos = False

    if evidencias_df.empty:
        st.warning("No hay fotos disponibles")
        return True

    work = evidencias_df.copy()
    if "fecha_carga" in work.columns:
        work = work.sort_values("fecha_carga")
    work = work.drop_duplicates(subset=["tipo_foto"], keep="last")
    map_evidencias = {str(r["tipo_foto"]).lower(): r for _, r in work.iterrows()}

    faltan_requeridas = [t for t in orden if t not in map_evidencias]
    faltan_fotos = len(faltan_requeridas) > 0

    if faltan_fotos:
        st.error(f"Faltan fotos obligatorias: {', '.join(faltan_requeridas)}")

    cols = st.columns(4)
    for i, tipo in enumerate(orden):
        with cols[i]:
            st.markdown(f"**{tipo.upper()}**")
            if tipo not in map_evidencias:
                st.warning("Falta foto")
                continue

            row_evi = map_evidencias[tipo]
            try:
                image_bytes = fetch_image_bytes(str(row_evi.get("drive_link", "")))
                if image_bytes:
                    st.image(image_bytes, use_container_width=True)
                else:
                    st.warning("No se pudo mostrar imagen")
            except Exception:
                st.warning("No se pudo mostrar imagen")

    return faltan_fotos



def require_role(allowed_roles: List[str]) -> Dict[str, Any]:
    user = st.session_state.get("auth_user")
    if not user:
        st.error("Sesión no iniciada")
        st.stop()
    if user.get("rol") not in allowed_roles:
        st.error("No tienes permisos para este módulo")
        st.stop()
    return user


# =========================================================
# LOGIN
# =========================================================
def render_login() -> None:
    col1, col2, col3 = st.columns([1, 1.1, 1])
    with col2:
        st.title("Control Medidas ML")
        st.caption("Ingreso con PIN por rol")

        with st.form("login_form"):
            usuario = st.text_input("Usuario")
            pin = st.text_input("PIN", type="password")
            submit = st.form_submit_button("Ingresar", use_container_width=True)

        if submit:
            try:
                result = api_login_with_pin(usuario.strip(), pin.strip())
                st.session_state.auth_user = result.get("user", {})
                clear_caches()
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo iniciar sesión: {exc}")


# =========================================================
# ADMIN
# =========================================================
def render_import_section(usuario_actual: str) -> None:
    st.subheader("Importar publicaciones Mercado Libre")
    uploaded_file = st.file_uploader("Sube Excel real de publicaciones ML", type=["xlsx"], key="excel_import_ml")

    if uploaded_file is not None:
        st.info(f"Archivo listo: {uploaded_file.name}")
        if st.button("Procesar importación", use_container_width=True, key="btn_import_ml"):
            try:
                rows = load_excel_as_rows(uploaded_file)
                with st.spinner("Cargando staging..."):
                    r1 = api_replace_base_import_ml_raw(rows)
                with st.spinner("Aplicando UPSERT..."):
                    r2 = api_process_import_ml_to_base(uploaded_file.name, usuario_actual)

                clear_caches()
                st.success(
                    "Importación OK | "
                    f"raw: {r1.get('inserted', 0)} | "
                    f"actualizados: {r2.get('updated', 0)} | "
                    f"nuevos: {r2.get('inserted', 0)} | "
                    f"sin cambios: {r2.get('unchanged', 0)} | "
                    f"omitidos: {r2.get('skipped', 0)}"
                )
            except Exception as exc:
                st.error(f"Error importando: {exc}")



def render_admin() -> None:
    user = require_role(["admin"])
    usuario_actual = str(user.get("usuario_id", "admin"))

    st.title("Panel Administrador")
    render_import_section(usuario_actual)

    try:
        df = safe_df(api_get_all_products())
    except Exception as exc:
        st.error(f"No se pudo leer la API: {exc}")
        st.stop()

    show_kpi_row(df)

    if df.empty:
        st.warning("No hay productos en base_productos_ml")
        return

    st.subheader("Filtros")
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        texto = st.text_input("Buscar SKU / MLC / título")
    with f2:
        estados = sorted([x for x in df.get("estado_actual", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        estados_sel = st.multiselect("Estado", estados, default=estados)
    with f3:
        prioridad_vals = sorted([x for x in df.get("prioridad", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        prioridad_sel = st.multiselect("Prioridad", prioridad_vals, default=prioridad_vals)
    with f4:
        operador_vals = sorted([x for x in df.get("operador_asignado", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        operador_filter = st.multiselect("Operador asignado", operador_vals, default=operador_vals)

    df_filtrado = df.copy()
    if texto:
        titulo_col = df_filtrado["titulo"] if "titulo" in df_filtrado.columns else pd.Series([""] * len(df_filtrado))
        mask = (
            df_filtrado["sku"].astype(str).str.contains(texto, case=False, na=False)
            | df_filtrado["mlc"].astype(str).str.contains(texto, case=False, na=False)
            | titulo_col.astype(str).str.contains(texto, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]

    if estados_sel and "estado_actual" in df_filtrado.columns:
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
        c
        for c in [
            "sku",
            "mlc",
            "titulo",
            "categoria",
            "ventas",
            "visitas",
            "estado_actual",
            "prioridad",
            "operador_asignado",
        ]
        if c in df_filtrado.columns
    ]

    edited = st.data_editor(
        df_filtrado[cols_view].assign(seleccionar=False),
        use_container_width=True,
        hide_index=True,
        column_config={"seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
        disabled=cols_view,
        key="admin_data_editor",
    )

    if asignar_btn:
        seleccionados = edited[edited["seleccionar"] == True]  # noqa: E712
        if seleccionados.empty:
            st.warning("No seleccionaste productos")
        else:
            try:
                items = seleccionados[["sku", "mlc"]].to_dict(orient="records")
                result = api_assign_tasks(items, operador_destino, prioridad_destino, usuario_actual)
                clear_caches()
                st.success(f"Tareas asignadas: {result.get('assigned', 0)}")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo asignar: {exc}")

    st.subheader("Gestión rápida de estado")
    if df_filtrado.empty:
        st.info("No hay casos con los filtros aplicados")
        return

    opciones = df_filtrado.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r.get('titulo', '')}", axis=1).tolist()
    opcion = st.selectbox("Selecciona un caso", options=opciones)
    sku_sel, mlc_sel, *_ = opcion.split(" | ")
    fila = df_filtrado[(df_filtrado["sku"].astype(str) == sku_sel) & (df_filtrado["mlc"].astype(str) == mlc_sel)].iloc[0]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {fila['sku']}")
        st.markdown(f"**MLC:** {fila['mlc']}")
        st.markdown(f"**Título:** {fila.get('titulo', '')}")
        st.markdown(badge_estado(str(fila.get('estado_actual', ''))), unsafe_allow_html=True)
    with c2:
        nuevo_estado = st.selectbox("Nuevo estado", ESTADOS_ADMIN)
        ticket = st.text_input("Ticket ejecutivo", value=str(fila.get("ticket_ejecutivo", "")))
        comentario = st.text_area("Comentario admin", value="")
        if st.button("Actualizar estado", use_container_width=True):
            try:
                result = api_update_status(sku_sel, mlc_sel, nuevo_estado, usuario_actual, comentario, ticket)
                clear_caches()
                st.success(f"Estado actualizado a {result.get('estado_nuevo')}")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo actualizar estado: {exc}")


# =========================================================
# OPERADOR
# =========================================================
def render_operator() -> None:
    user = require_role(["operador"])
    operador = str(user.get("operador_codigo") or user.get("usuario_id") or "")

    st.title("Módulo Operador PDA")
    st.caption(f"Operador autenticado: {operador}")

    try:
        tareas = safe_df(api_get_tasks_by_operator(operador))
    except Exception as exc:
        st.error(f"No se pudo cargar tareas: {exc}")
        st.stop()

    st.metric("Mis tareas", len(tareas))
    if tareas.empty:
        st.info("No tienes tareas pendientes")
        return

    tareas["label"] = tareas.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r.get('titulo', '')}", axis=1)
    selected_label = st.selectbox("Selecciona producto", tareas["label"].tolist())
    fila = tareas[tareas["label"] == selected_label].iloc[0]

    st.markdown("### Información actual")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {fila['sku']}")
        st.markdown(f"**MLC:** {fila['mlc']}")
        st.markdown(f"**Título:** {fila.get('titulo', '')}")
        st.markdown(f"**Categoría:** {fila.get('categoria', '')}")
    with c2:
        st.markdown(f"**Peso ML:** {fila.get('peso_ml_kg', '')}")
        st.markdown(
            f"**Dimensiones ML:** {fila.get('alto_ml_cm', '')} x {fila.get('ancho_ml_cm', '')} x {fila.get('profundidad_ml_cm', '')}"
        )
        st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)

    st.markdown("### Ingresar medidas reales")
    form_key = f"form_medicion_{fila['sku']}_{fila['mlc']}"
    with st.form(form_key):
        col1, col2 = st.columns(2)
        with col1:
            alto = st.number_input("Alto real (cm)", min_value=0.0, step=0.1, format="%.2f", key=f"alto_{fila['sku']}_{fila['mlc']}")
            ancho = st.number_input("Ancho real (cm)", min_value=0.0, step=0.1, format="%.2f", key=f"ancho_{fila['sku']}_{fila['mlc']}")
        with col2:
            profundidad = st.number_input(
                "Profundidad real (cm)", min_value=0.0, step=0.1, format="%.2f", key=f"prof_{fila['sku']}_{fila['mlc']}"
            )
            peso = st.number_input("Peso real (kg)", min_value=0.0, step=0.001, format="%.3f", key=f"peso_{fila['sku']}_{fila['mlc']}")

        observacion = st.text_area("Observación operador", key=f"obs_{fila['sku']}_{fila['mlc']}")

        st.markdown("### Fotos de respaldo")
        foto_alto = st.file_uploader("Foto alto", type=["jpg", "jpeg", "png"], key=f"foto_alto_{fila['sku']}_{fila['mlc']}")
        foto_ancho = st.file_uploader("Foto ancho", type=["jpg", "jpeg", "png"], key=f"foto_ancho_{fila['sku']}_{fila['mlc']}")
        foto_profundidad = st.file_uploader(
            "Foto profundidad", type=["jpg", "jpeg", "png"], key=f"foto_prof_{fila['sku']}_{fila['mlc']}"
        )
        foto_peso = st.file_uploader("Foto peso", type=["jpg", "jpeg", "png"], key=f"foto_peso_{fila['sku']}_{fila['mlc']}")

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
            return

        try:
            result_med = api_save_measurement(
                sku=str(fila["sku"]),
                mlc=str(fila["mlc"]),
                operador=operador,
                alto_real_cm=float(alto),
                ancho_real_cm=float(ancho),
                profundidad_real_cm=float(profundidad),
                peso_real_kg=float(peso),
                observacion_operador=observacion,
            )
            medicion_id = result_med.get("medicion_id", "")

            with st.spinner("Subiendo fotos..."):
                api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "alto", foto_alto, operador, medicion_id)
                api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "ancho", foto_ancho, operador, medicion_id)
                api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "profundidad", foto_profundidad, operador, medicion_id)
                api_upload_photo(str(fila["sku"]), str(fila["mlc"]), "peso", foto_peso, operador, medicion_id)

            clear_caches()
            st.success(f"Medición guardada y fotos subidas. ID: {medicion_id}")
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo guardar la medición/fotos: {exc}")


# =========================================================
# SUPERVISOR
# =========================================================
def render_supervisor() -> None:
    user = require_role(["supervisor", "admin"])
    supervisor = str(user.get("usuario_id") or user.get("nombre") or "supervisor")

    st.title("Módulo Supervisor")

    try:
        pendientes = safe_df(api_get_pending_validation())
    except Exception as exc:
        st.error(f"No se pudo cargar pendientes: {exc}")
        st.stop()

    st.metric("Pendientes validación", len(pendientes))
    if pendientes.empty:
        st.info("No hay mediciones pendientes de validación")
        return

    pendientes["label"] = pendientes.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r.get('titulo', '')}", axis=1)
    selected_label = st.selectbox("Caso a revisar", pendientes["label"].tolist())
    fila = pendientes[pendientes["label"] == selected_label].iloc[0]

    try:
        case_detail = api_get_case_detail(str(fila["sku"]), str(fila["mlc"]))
    except Exception as exc:
        st.error(f"No se pudo cargar detalle del caso: {exc}")
        st.stop()

    product = case_detail.get("product", {}) or {}
    evidencias = safe_df(pd.DataFrame(case_detail.get("evidencias", [])))
    mediciones = safe_df(pd.DataFrame(case_detail.get("mediciones", [])))
    historial = safe_df(pd.DataFrame(case_detail.get("historial", [])))

    st.markdown("### Comparativo ML vs Real")
    comp = pd.DataFrame(
        [
            ["Alto", product.get("alto_ml_cm", ""), product.get("alto_real_cm", "")],
            ["Ancho", product.get("ancho_ml_cm", ""), product.get("ancho_real_cm", "")],
            ["Profundidad", product.get("profundidad_ml_cm", ""), product.get("profundidad_real_cm", "")],
            ["Peso", product.get("peso_ml_kg", ""), product.get("peso_real_kg", "")],
        ],
        columns=["Campo", "ML", "Real"],
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)

    st.markdown("### Evidencia fotográfica")
    faltan_fotos = render_evidence_gallery(evidencias)

    if not mediciones.empty:
        st.markdown("### Mediciones registradas")
        cols = [
            c
            for c in [
                "fecha_medicion",
                "operador",
                "alto_real_cm",
                "ancho_real_cm",
                "profundidad_real_cm",
                "peso_real_kg",
                "observacion_operador",
            ]
            if c in mediciones.columns
        ]
        st.dataframe(mediciones[cols].sort_values(by=cols[0], ascending=False), use_container_width=True, hide_index=True)

    if not historial.empty:
        st.markdown("### Historial del caso")
        cols = [
            c for c in ["fecha_cambio", "estado_anterior", "estado_nuevo", "usuario", "comentario"] if c in historial.columns
        ]
        st.dataframe(historial[cols].sort_values(by=cols[0], ascending=False), use_container_width=True, hide_index=True)

    comentario = st.text_area("Comentario supervisor", key=f"comentario_supervisor_{fila['sku']}_{fila['mlc']}")
    c1, c2 = st.columns(2)

    with c1:
        if st.button(
            "Aprobar",
            use_container_width=True,
            disabled=faltan_fotos,
            key=f"btn_aprobar_supervisor_{fila['sku']}_{fila['mlc']}",
        ):
            try:
                result = api_validate_measurement(
                    sku=str(fila["sku"]),
                    mlc=str(fila["mlc"]),
                    supervisor=supervisor,
                    aprobar=True,
                    comentario=comentario,
                )
                clear_caches()
                st.success(f"Estado: {result.get('estado_nuevo')}")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo aprobar: {exc}")

    with c2:
        if st.button(
            "Solicitar nueva evidencia",
            use_container_width=True,
            key=f"btn_nueva_evidencia_supervisor_{fila['sku']}_{fila['mlc']}",
        ):
            try:
                result = api_validate_measurement(
                    sku=str(fila["sku"]),
                    mlc=str(fila["mlc"]),
                    supervisor=supervisor,
                    aprobar=False,
                    comentario=comentario or "Se solicita nueva evidencia",
                )
                clear_caches()
                st.warning(f"Estado: {result.get('estado_nuevo')}")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo devolver: {exc}")


# =========================================================
# APP SHELL
# =========================================================
def init_session() -> None:
    if "auth_user" not in st.session_state:
        st.session_state.auth_user = None



def render_sidebar() -> str:
    st.sidebar.title("Control Medidas ML")
    user = st.session_state.get("auth_user")

    if not user:
        st.sidebar.info("Sesión no iniciada")
        return "login"

    st.sidebar.success(f"{user.get('nombre', '')}")
    st.sidebar.caption(f"Rol: {user.get('rol', '')}")
    st.sidebar.caption(f"Usuario: {user.get('usuario_id', '')}")

    role = str(user.get("rol", ""))
    if role == "admin":
        allowed = ["Administrador", "Supervisor"]
    elif role == "supervisor":
        allowed = ["Supervisor"]
    elif role == "operador":
        allowed = ["Operador"]
    else:
        allowed = []

    modulo = st.sidebar.radio("Módulo", allowed)

    if st.sidebar.button("Recargar datos", use_container_width=True):
        clear_caches()
        st.rerun()

    if st.sidebar.button("Cerrar sesión", use_container_width=True):
        st.session_state.auth_user = None
        clear_caches()
        st.rerun()

    return modulo



def main() -> None:
    init_session()
    modulo = render_sidebar()

    if modulo == "login":
        render_login()
        return

    if modulo == "Administrador":
        render_admin()
    elif modulo == "Operador":
        render_operator()
    else:
        render_supervisor()


if __name__ == "__main__":
    main()
