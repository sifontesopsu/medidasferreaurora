import json
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Control Medidas ML", page_icon="📦", layout="wide")

# =========================================================
# CONFIG
# =========================================================
# En Streamlit Cloud, guarda esto en secrets.toml como:
# APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzgW62wqP7-RdDGH5Gd6YJ0Y7r636pLq9Lh1tcfZZzXclhKaEozA2Yq6xDiSKYl2amckw/exec"
APPS_SCRIPT_URL = st.secrets.get("https://script.google.com/macros/s/AKfycbx_JcGXJ9e8A30y-M2ddT6xwCNVNRXfr8gcAtx-pYW-n4UGOAb2nIad-1EqwiaGSFCj4g/exec", "")

ESTADOS_PDA = ["pendiente_medicion", "requiere_nueva_evidencia"]
PRIORIDADES = ["alta", "media", "baja"]


# =========================================================
# API
# =========================================================
def api_post(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not APPS_SCRIPT_URL:
        raise RuntimeError("Falta APPS_SCRIPT_URL en st.secrets")

    response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=60)
    response.raise_for_status()
    data = response.json()

    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Error desconocido en API"))

    return data


def api_get_all_products() -> pd.DataFrame:
    data = api_post({"action": "get_all_products"})
    return pd.DataFrame(data.get("data", []))


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


def api_get_tasks_by_operator(operador: str) -> pd.DataFrame:
    data = api_post({"action": "get_tasks_by_operator", "operador": operador})
    return pd.DataFrame(data.get("items", []))


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


def api_update_status(sku: str, mlc: str, nuevo_estado: str, usuario: str, comentario: str = "", ticket_ejecutivo: str = "") -> Dict[str, Any]:
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
# HELPERS UI
# =========================================================
def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    return df if not df.empty else pd.DataFrame()


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
    return f"<span style='background:{color};color:white;padding:4px 8px;border-radius:999px;font-size:12px'>{estado}</span>"


def show_kpi_row(df: pd.DataFrame):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total", len(df))
    with c2:
        st.metric("Pendiente medición", int((df.get("estado_actual", pd.Series(dtype=str)) == "pendiente_medicion").sum()) if not df.empty else 0)
    with c3:
        st.metric("Pendiente validación", int((df.get("estado_actual", pd.Series(dtype=str)) == "medido_pendiente_validacion").sum()) if not df.empty else 0)
    with c4:
        st.metric("Resueltos", int((df.get("estado_actual", pd.Series(dtype=str)) == "resuelto").sum()) if not df.empty else 0)


# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.title("Control Medidas ML")
modo = st.sidebar.radio("Módulo", ["Administrador", "Operador", "Supervisor"])
usuario_actual = st.sidebar.text_input("Usuario actual", value="admin_demo")
recargar = st.sidebar.button("Recargar datos")

if recargar:
    st.rerun()


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
        prioridad_vals = sorted([x for x in df["prioridad"].dropna().astype(str).unique().tolist() if x])
        prioridad_sel = st.multiselect("Prioridad", prioridad_vals, default=prioridad_vals)
    with f4:
        operador_vals = sorted([x for x in df["operador_asignado"].dropna().astype(str).unique().tolist() if x])
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

    cols_view = [c for c in ["sku", "mlc", "titulo", "categoria", "ventas", "visitas", "estado_actual", "prioridad", "operador_asignado"] if c in df_filtrado.columns]
    edited = st.data_editor(
        df_filtrado[cols_view].assign(seleccionar=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)
        },
        disabled=[c for c in cols_view],
    )

    if asignar_btn:
        seleccionados = edited[edited["seleccionar"] == True]  # noqa: E712
        if seleccionados.empty:
            st.warning("No seleccionaste productos")
        else:
            items = seleccionados[["sku", "mlc"]].to_dict(orient="records")
            try:
                result = api_assign_tasks(items, operador_destino, prioridad_destino, usuario_actual)
                st.success(f"Tareas asignadas: {result.get('assigned', 0)}")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo asignar: {e}")

    st.subheader("Gestión rápida de estado")
    if not df_filtrado.empty:
        opcion = st.selectbox(
            "Selecciona un caso",
            options=df_filtrado.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1).tolist(),
        )
        sku_sel, mlc_sel, *_ = opcion.split(" | ")
        fila = df_filtrado[(df_filtrado["sku"].astype(str) == sku_sel) & (df_filtrado["mlc"].astype(str) == mlc_sel)].iloc[0]

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**SKU:** {fila['sku']}")
            st.markdown(f"**MLC:** {fila['mlc']}")
            st.markdown(f"**Título:** {fila['titulo']}")
            st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)
        with c2:
            nuevo_estado = st.selectbox(
                "Nuevo estado",
                [
                    "listo_para_actualizar_medidas",
                    "listo_para_ejecutivo",
                    "en_gestion_ejecutivo",
                    "resuelto",
                    "rechazado_ml",
                    "rechazado_ejecutivo",
                    "requiere_nueva_evidencia",
                ],
            )
            ticket = st.text_input("Ticket ejecutivo", value=str(fila.get("ticket_ejecutivo", "")))
            comentario = st.text_area("Comentario admin", value="")
            if st.button("Actualizar estado", use_container_width=True):
                try:
                    result = api_update_status(sku_sel, mlc_sel, nuevo_estado, usuario_actual, comentario, ticket)
                    st.success(f"Estado actualizado a {result.get('estado_nuevo')}")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo actualizar estado: {e}")


# =========================================================
# OPERADOR
# =========================================================
elif modo == "Operador":
    st.title("Módulo Operador PDA")
    operador = st.text_input("Operador", value=usuario_actual)

    try:
        tareas = api_get_tasks_by_operator(operador)
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
        st.markdown(f"**Dimensiones ML:** {fila.get('alto_ml_cm', '')} x {fila.get('ancho_ml_cm', '')} x {fila.get('profundidad_ml_cm', '')}")
        st.markdown(badge_estado(str(fila.get("estado_actual", ""))), unsafe_allow_html=True)

    st.markdown("### Ingresar medidas reales")
    with st.form("form_medicion"):
        col1, col2 = st.columns(2)
        with col1:
            alto = st.number_input("Alto real (cm)", min_value=0.0, step=0.1, format="%.2f")
            ancho = st.number_input("Ancho real (cm)", min_value=0.0, step=0.1, format="%.2f")
        with col2:
            profundidad = st.number_input("Profundidad real (cm)", min_value=0.0, step=0.1, format="%.2f")
            peso = st.number_input("Peso real (kg)", min_value=0.0, step=0.001, format="%.3f")
        observacion = st.text_area("Observación operador")
        submitted = st.form_submit_button("Guardar medición", use_container_width=True)

    if submitted:
        try:
            result = api_save_measurement(
                sku=str(fila["sku"]),
                mlc=str(fila["mlc"]),
                operador=operador,
                alto_real_cm=float(alto),
                ancho_real_cm=float(ancho),
                profundidad_real_cm=float(profundidad),
                peso_real_kg=float(peso),
                observacion_operador=observacion,
            )
            st.success(f"Medición guardada. ID: {result.get('medicion_id')}")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo guardar la medición: {e}")


# =========================================================
# SUPERVISOR
# =========================================================
else:
    st.title("Módulo Supervisor")
    supervisor = st.text_input("Supervisor", value=usuario_actual)

    try:
        df = api_get_all_products()
    except Exception as e:
        st.error(f"No se pudo cargar la base: {e}")
        st.stop()

    df = safe_df(df)
    pendientes = df[df["estado_actual"].astype(str) == "medido_pendiente_validacion"].copy() if not df.empty else pd.DataFrame()

    st.metric("Pendientes validación", len(pendientes))

    if pendientes.empty:
        st.info("No hay mediciones pendientes de validación")
        st.stop()

    pendientes["label"] = pendientes.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)
    selected_label = st.selectbox("Caso a revisar", pendientes["label"].tolist())
    fila = pendientes[pendientes["label"] == selected_label].iloc[0]

    st.markdown("### Comparativo ML vs Real")
    comp = pd.DataFrame(
        [
            ["Alto", fila.get("alto_ml_cm", ""), fila.get("alto_real_cm", "")],
            ["Ancho", fila.get("ancho_ml_cm", ""), fila.get("ancho_real_cm", "")],
            ["Profundidad", fila.get("profundidad_ml_cm", ""), fila.get("profundidad_real_cm", "")],
            ["Peso", fila.get("peso_ml_kg", ""), fila.get("peso_real_kg", "")],
        ],
        columns=["Campo", "ML", "Real"],
    )
    st.dataframe(comp, use_container_width=True, hide_index=True)

    comentario = st.text_area("Comentario supervisor")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Aprobar", use_container_width=True):
            try:
                result = api_validate_measurement(
                    sku=str(fila["sku"]),
                    mlc=str(fila["mlc"]),
                    supervisor=supervisor,
                    aprobar=True,
                    comentario=comentario,
                )
                st.success(f"Estado: {result.get('estado_nuevo')}")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo aprobar: {e}")
    with c2:
        if st.button("Solicitar nueva evidencia", use_container_width=True):
            try:
                result = api_validate_measurement(
                    sku=str(fila["sku"]),
                    mlc=str(fila["mlc"]),
                    supervisor=supervisor,
                    aprobar=False,
                    comentario=comentario or "Se solicita nueva evidencia",
                )
                st.warning(f"Estado: {result.get('estado_nuevo')}")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo devolver: {e}")
