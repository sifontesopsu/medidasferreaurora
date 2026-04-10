import streamlit as st
import pandas as pd
import requests

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")
APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")


# =========================
# API
# =========================
def api_post(payload):
    if not APPS_SCRIPT_URL:
        raise RuntimeError("Falta APPS_SCRIPT_URL en st.secrets")

    response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=90)
    response.raise_for_status()
    data = response.json()

    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Error API"))

    return data


def api_get_all_products() -> pd.DataFrame:
    data = api_post({"action": "get_all_products"})
    return pd.DataFrame(data.get("data", []))


def api_get_tasks_by_operator(operador: str) -> pd.DataFrame:
    data = api_post({"action": "get_tasks_by_operator", "operador": operador})
    return pd.DataFrame(data.get("items", []))


def api_get_pending_validation() -> pd.DataFrame:
    data = api_post({"action": "get_pending_validation"})
    return pd.DataFrame(data.get("items", []))


def api_get_case_detail(sku: str, mlc: str) -> dict:
    return api_post({
        "action": "get_case_detail",
        "sku": sku,
        "mlc": mlc,
    })


def api_get_evidencias(sku: str, mlc: str) -> pd.DataFrame:
    data = api_post({
        "action": "get_evidencias",
        "sku": sku,
        "mlc": mlc,
    })
    return pd.DataFrame(data.get("data", []))


def api_validate_measurement(sku: str, mlc: str, supervisor: str, aprobar: bool, comentario: str):
    return api_post({
        "action": "validate_measurement",
        "sku": sku,
        "mlc": mlc,
        "supervisor": supervisor,
        "aprobar": aprobar,
        "comentario": comentario,
    })


# =========================
# HELPERS
# =========================
def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    return df if not df.empty else pd.DataFrame()


def safe_text(value, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text and text.lower() != "nan" else default


def safe_number(value):
    if value is None or value == "":
        return ""
    try:
        n = float(value)
        if n.is_integer():
            return int(n)
        return round(n, 3)
    except Exception:
        return value


def extract_case_payload(detail: dict) -> dict:
    for key in ["item", "case", "data", "detail", "row"]:
        if isinstance(detail.get(key), dict):
            return detail[key]
    return detail if isinstance(detail, dict) else {}


def get_field(case_data: dict, fallback_row: pd.Series, field: str):
    if field in case_data and case_data.get(field) not in [None, ""]:
        return case_data.get(field)
    if field in fallback_row.index:
        return fallback_row.get(field)
    return ""


def show_kpi_row(df: pd.DataFrame):
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Total", len(df))

    with c2:
        total = int((df.get("estado_actual", pd.Series(dtype=str)).astype(str) == "pendiente_medicion").sum()) if not df.empty else 0
        st.metric("Pendiente medición", total)

    with c3:
        total = int((df.get("estado_actual", pd.Series(dtype=str)).astype(str) == "medido_pendiente_validacion").sum()) if not df.empty else 0
        st.metric("Pendiente validación", total)

    with c4:
        total = int((df.get("estado_actual", pd.Series(dtype=str)).astype(str) == "validado_supervisor").sum()) if not df.empty else 0
        st.metric("Pendiente gestión administrativa", total)


def clear_and_rerun():
    st.cache_data.clear()
    st.rerun()


@st.cache_data(ttl=10)
def cached_all_products() -> pd.DataFrame:
    return api_get_all_products()


@st.cache_data(ttl=10)
def cached_tasks_by_operator(operador: str) -> pd.DataFrame:
    return api_get_tasks_by_operator(operador)


@st.cache_data(ttl=10)
def cached_pending_validation() -> pd.DataFrame:
    return api_get_pending_validation()


@st.cache_data(ttl=10)
def cached_case_detail(sku: str, mlc: str) -> dict:
    return api_get_case_detail(sku, mlc)


@st.cache_data(ttl=10)
def cached_evidencias(sku: str, mlc: str) -> pd.DataFrame:
    return api_get_evidencias(sku, mlc)


# =========================
# SIDEBAR
# =========================
st.sidebar.title("Control Medidas ML")

modo = st.sidebar.selectbox(
    "Módulo",
    ["Administrador", "Operador", "Supervisor"],
)

usuario = st.sidebar.text_input("Usuario", value=st.session_state.get("usuario_nombre", "admin"))
st.session_state["usuario_nombre"] = usuario

if st.sidebar.button("Recargar"):
    clear_and_rerun()


# =========================
# ADMINISTRADOR
# =========================
if modo == "Administrador":
    st.title("Administrador")

    try:
        df = safe_df(cached_all_products())
    except Exception as e:
        st.error(f"No se pudo cargar la base: {e}")
        st.stop()

    if df.empty:
        st.warning("No hay productos cargados")
        st.stop()

    show_kpi_row(df)

    st.subheader("Base operativa")
    texto = st.text_input("Buscar SKU / MLC / título")

    df_filtrado = df.copy()
    if texto:
        mask = (
            df_filtrado.get("sku", pd.Series(dtype=str)).astype(str).str.contains(texto, case=False, na=False)
            | df_filtrado.get("mlc", pd.Series(dtype=str)).astype(str).str.contains(texto, case=False, na=False)
            | df_filtrado.get("titulo", pd.Series(dtype=str)).astype(str).str.contains(texto, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]

    cols = [c for c in ["sku", "mlc", "titulo", "estado_actual", "operador_asignado"] if c in df_filtrado.columns]
    st.dataframe(df_filtrado[cols], use_container_width=True, hide_index=True)


# =========================
# OPERADOR
# =========================
elif modo == "Operador":
    st.title("Operador")

    operador_default = st.session_state.get("nombre_operador", "")
    operador = st.text_input("Nombre del pickeador", value=operador_default, key="nombre_operador")

    if not operador.strip():
        st.warning("Debes ingresar el nombre del pickeador para procesar la tarea")
        st.stop()

    try:
        tareas = safe_df(cached_tasks_by_operator(operador.strip()))
    except Exception as e:
        st.error(f"No se pudieron cargar las tareas: {e}")
        st.stop()

    if tareas.empty:
        st.info("Sin tareas pendientes para este pickeador")
        st.stop()

    tareas["label"] = tareas.apply(
        lambda r: f"{safe_text(r.get('sku'))} | {safe_text(r.get('mlc'))} | {safe_text(r.get('titulo'))}",
        axis=1,
    )

    selected = st.selectbox("Selecciona producto", tareas["label"].tolist())
    fila = tareas[tareas["label"] == selected].iloc[0]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**SKU:** {safe_text(fila.get('sku'))}")
        st.markdown(f"**MLC:** {safe_text(fila.get('mlc'))}")
        st.markdown(f"**Título:** {safe_text(fila.get('titulo'))}")
    with c2:
        st.markdown(f"**Estado:** {safe_text(fila.get('estado_actual'))}")
        st.markdown(f"**Operador:** {operador}")

    st.dataframe(
        pd.DataFrame(
            [
                ["Alto ML", safe_number(fila.get("alto_ml_cm", ""))],
                ["Ancho ML", safe_number(fila.get("ancho_ml_cm", ""))],
                ["Profundidad ML", safe_number(fila.get("profundidad_ml_cm", ""))],
                ["Peso ML", safe_number(fila.get("peso_ml_kg", ""))],
            ],
            columns=["Campo", "Valor"],
        ),
        use_container_width=True,
        hide_index=True,
    )


# =========================
# SUPERVISOR
# =========================
else:
    st.title("Módulo Supervisor")

    try:
        pendientes = safe_df(cached_pending_validation())
    except Exception as e:
        st.error(f"No se pudieron cargar pendientes de validación: {e}")
        st.stop()

    st.metric("Pendientes validación", len(pendientes))

    if pendientes.empty:
        st.info("No hay pendientes")
        st.stop()

    pendientes["label"] = pendientes.apply(
        lambda r: f"{safe_text(r.get('sku'))} | {safe_text(r.get('mlc'))} | {safe_text(r.get('titulo'))}",
        axis=1,
    )

    selected = st.selectbox("Caso a revisar", pendientes["label"].tolist())
    fila = pendientes[pendientes["label"] == selected].iloc[0]

    sku = safe_text(fila.get("sku"))
    mlc = safe_text(fila.get("mlc"))

    case_data = {}
    try:
        detail_raw = cached_case_detail(sku, mlc)
        case_data = extract_case_payload(detail_raw)
    except Exception as e:
        st.warning(f"No se pudo cargar el detalle del caso: {e}")

    st.subheader("Comparativo ML vs Real")
    comp = pd.DataFrame(
        [
            ["Alto", safe_number(get_field(case_data, fila, "alto_ml_cm")), safe_number(get_field(case_data, fila, "alto_real_cm"))],
            ["Ancho", safe_number(get_field(case_data, fila, "ancho_ml_cm")), safe_number(get_field(case_data, fila, "ancho_real_cm"))],
            ["Profundidad", safe_number(get_field(case_data, fila, "profundidad_ml_cm")), safe_number(get_field(case_data, fila, "profundidad_real_cm"))],
            ["Peso", safe_number(get_field(case_data, fila, "peso_ml_kg")), safe_number(get_field(case_data, fila, "peso_real_kg"))],
        ],
        columns=["Campo", "ML", "Real"],
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)

    st.subheader("Evidencia fotográfica")
    try:
        evidencias = safe_df(cached_evidencias(sku, mlc))
    except Exception as e:
        st.error(f"No se pudieron cargar evidencias: {e}")
        evidencias = pd.DataFrame()

    orden = ["alto", "ancho", "profundidad", "peso"]
    cols = st.columns(4)

    if evidencias.empty:
        for i, tipo in enumerate(orden):
            with cols[i]:
                st.markdown(f"**{tipo.upper()}**")
                st.warning("Falta")
    else:
        evidencias["tipo_foto"] = evidencias["tipo_foto"].astype(str).str.lower().str.strip()
        if "fecha_carga" in evidencias.columns:
            evidencias = evidencias.sort_values("fecha_carga")
        evidencias = evidencias.drop_duplicates(subset=["tipo_foto"], keep="last")
        map_evidencias = {str(r["tipo_foto"]): r for _, r in evidencias.iterrows()}

        for i, tipo in enumerate(orden):
            with cols[i]:
                st.markdown(f"**{tipo.upper()}**")
                if tipo not in map_evidencias:
                    st.warning("Falta")
                    continue

                row = map_evidencias[tipo]
                drive_link = safe_text(row.get("drive_link"))
                if drive_link:
                    st.image(drive_link, use_container_width=True)
                else:
                    st.warning("Sin link")

    comentario = st.text_area("Comentario supervisor")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Aprobar", use_container_width=True):
            try:
                api_validate_measurement(sku, mlc, usuario, True, comentario)
                st.success("Caso aprobado")
                clear_and_rerun()
            except Exception as e:
                st.error(f"No se pudo aprobar: {e}")

    with c2:
        if st.button("Solicitar nueva evidencia", use_container_width=True):
            try:
                api_validate_measurement(sku, mlc, usuario, False, comentario or "Se solicita nueva evidencia")
                st.warning("Caso devuelto a nueva evidencia")
                clear_and_rerun()
            except Exception as e:
                st.error(f"No se pudo devolver: {e}")
