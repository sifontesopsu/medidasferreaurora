import streamlit as st
import pandas as pd
import requests
import base64

# =========================
# CONFIG
# =========================
APPS_SCRIPT_URL = st.secrets.get("APPS_SCRIPT_URL", "")

# =========================
# API
# =========================
def api_post(payload):
    r = requests.post(APPS_SCRIPT_URL, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise Exception(data.get("error", "Error API"))
    return data

def api_get_all_products():
    return pd.DataFrame(api_post({"action": "get_all_products"})["data"])

def api_get_tasks_by_operator(op):
    return pd.DataFrame(api_post({"action": "get_tasks_by_operator", "operador": op})["items"])

def api_get_pending_validation():
    return pd.DataFrame(api_post({"action": "get_pending_validation"})["items"])

def api_get_case_detail(sku, mlc):
    return api_post({
        "action": "get_case_detail",
        "sku": sku,
        "mlc": mlc
    })

def api_get_evidencias(sku, mlc):
    return pd.DataFrame(api_post({
        "action": "get_evidencias",
        "sku": sku,
        "mlc": mlc
    })["data"])

def api_validate_measurement(sku, mlc, supervisor, aprobar, comentario):
    return api_post({
        "action": "validate_measurement",
        "sku": sku,
        "mlc": mlc,
        "supervisor": supervisor,
        "aprobar": aprobar,
        "comentario": comentario
    })

# =========================
# UI HELPERS
# =========================
def show_kpi_row(df):
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Total", len(df))

    with c2:
        st.metric("Pendiente medición", int((df["estado_actual"] == "pendiente_medicion").sum()))

    with c3:
        st.metric("Pendiente validación", int((df["estado_actual"] == "medido_pendiente_validacion").sum()))

    with c4:
        st.metric("Pendiente gestión administrativa", int((df["estado_actual"] == "validado_supervisor").sum()))

# =========================
# SIDEBAR
# =========================
st.sidebar.title("Control Medidas ML")

modo = st.sidebar.selectbox(
    "Módulo",
    ["Administrador", "Operador", "Supervisor"]
)

usuario = st.sidebar.text_input("Usuario", value="admin")

# =========================
# ADMIN
# =========================
if modo == "Administrador":
    st.title("Administrador")

    df = api_get_all_products()

    show_kpi_row(df)

    cols = ["sku", "mlc", "titulo", "estado_actual", "operador_asignado"]
    st.dataframe(df[cols], use_container_width=True)

# =========================
# OPERADOR
# =========================
elif modo == "Operador":
    st.title("Operador")

    operador = st.text_input("Nombre operador")

    if not operador:
        st.warning("Debes ingresar tu nombre")
        st.stop()

    tareas = api_get_tasks_by_operator(operador)

    if tareas.empty:
        st.info("Sin tareas")
        st.stop()

    tareas["label"] = tareas.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)

    selected = st.selectbox("Selecciona producto", tareas["label"])

    fila = tareas[tareas["label"] == selected].iloc[0]

    st.write(fila)

# =========================
# SUPERVISOR
# =========================
else:
    st.title("Supervisor")

    df = api_get_pending_validation()

    st.metric("Pendientes validación", len(df))

    if df.empty:
        st.info("No hay pendientes")
        st.stop()

    df["label"] = df.apply(lambda r: f"{r['sku']} | {r['mlc']} | {r['titulo']}", axis=1)

    selected = st.selectbox("Caso a revisar", df["label"])

    fila = df[df["label"] == selected].iloc[0]

    sku = str(fila["sku"])
    mlc = str(fila["mlc"])

    # 🔴 AQUÍ ESTABA EL PROBLEMA
    try:
        detail = api_get_case_detail(sku, mlc)
    except Exception as e:
        st.error("No se pudo cargar el caso")
        st.stop()

    # =========================
    # COMPARATIVO
    # =========================
    comp = pd.DataFrame([
        ["Alto", detail.get("alto_ml_cm"), detail.get("alto_real_cm")],
        ["Ancho", detail.get("ancho_ml_cm"), detail.get("ancho_real_cm")],
        ["Profundidad", detail.get("profundidad_ml_cm"), detail.get("profundidad_real_cm")],
        ["Peso", detail.get("peso_ml_kg"), detail.get("peso_real_kg")]
    ], columns=["Campo", "ML", "Real"])

    st.subheader("Comparación")
    st.dataframe(comp, use_container_width=True)

    # =========================
    # EVIDENCIAS
    # =========================
    st.subheader("Evidencia")

    evidencias = api_get_evidencias(sku, mlc)

    if evidencias.empty:
        st.warning("No hay fotos")
    else:
        cols = st.columns(4)
        tipos = ["alto", "ancho", "profundidad", "peso"]

        for i, t in enumerate(tipos):
            with cols[i]:
                st.write(t.upper())
                row = evidencias[evidencias["tipo_foto"] == t]

                if not row.empty:
                    st.image(row.iloc[-1]["drive_link"], use_container_width=True)
                else:
                    st.warning("Falta")

    # =========================
    # ACCIONES
    # =========================
    comentario = st.text_area("Comentario")

    c1, c2 = st.columns(2)

    with c1:
        if st.button("Aprobar"):
            api_validate_measurement(sku, mlc, usuario, True, comentario)
            st.success("Aprobado")
            st.rerun()

    with c2:
        if st.button("Rechazar"):
            api_validate_measurement(sku, mlc, usuario, False, comentario)
            st.warning("Rechazado")
            st.rerun()