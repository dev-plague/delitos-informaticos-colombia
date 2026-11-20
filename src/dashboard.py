"""FASE 8 - Tablero Streamlit (lee únicamente la zona de servicio).

Ejecutar con:  streamlit run src/dashboard.py
"""
from __future__ import annotations

import json
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

PG = {
    "host": os.getenv("PGHOST", "postgres"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "delitos"),
    "user": os.getenv("PGUSER", "delitos"),
    "password": os.getenv("PGPASSWORD", "delitos"),
}
COLORES = ["#2563eb", "#0ea5e9", "#14b8a6", "#f59e0b", "#ef4444", "#8b5cf6", "#64748b"]

st.set_page_config(
    page_title="Delitos informáticos en Colombia",
    page_icon="🛡️",
    layout="wide",
)


@st.cache_data(ttl=60)
def consultar(sql: str) -> pd.DataFrame:
    with psycopg2.connect(**PG) as conn:
        return pd.read_sql(sql, conn)


def kpis() -> pd.Series | None:
    df = consultar("SELECT * FROM service.ind_kpis WHERE id = 1")
    return df.iloc[0] if not df.empty else None


def _texto_fecha(valor) -> str:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return "—"
    return pd.to_datetime(valor).strftime("%Y-%m-%d %H:%M")


def estilo(fig: go.Figure, alto: int = 380) -> go.Figure:
    fig.update_layout(
        height=alto,
        margin=dict(l=55, r=10, t=45, b=10),
        legend_title_text="",
        hoverlabel=dict(font_size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.25)", zeroline=False)
    return fig


def main() -> None:
    try:
        fila_kpi = kpis()
    except Exception as exc:
        st.error(f"No se pudo leer la zona de servicio. ¿Está corriendo PostgreSQL? ({exc})")
        return

    if fila_kpi is None:
        st.warning("La zona de servicio está vacía. Ejecute el pipeline y recargue.")
        return

    st.title("¿Cómo evolucionan los delitos informáticos en Colombia y dónde se concentran?")
    st.caption(
        f"**Fuente:** datos.gov.co · DELITOS INFORMÁTICOS (Ministerio de Defensa Nacional) · "
        f"**Corte de las cifras:** {fila_kpi['fecha_corte']} · "
        f"**Actualizado:** {_texto_fecha(fila_kpi['actualizado'])} · "
        f"Zona de servicio PostgreSQL"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Casos registrados", f"{int(fila_kpi['total_casos']):,}".replace(",", "."))
    c2.metric("Registros únicos", f"{int(fila_kpi['total_registros']):,}".replace(",", "."))
    c3.metric("Conducta más frecuente", str(fila_kpi["conducta_top"]).split(" - ")[0])
    c4.metric(
        f"Año pico ({int(fila_kpi['anio_pico'])})",
        f"{int(fila_kpi['casos_anio_pico']):,}".replace(",", "."),
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["📈 Tendencia", "🧩 Conductas", "📍 Territorio", "⚡ Capa rápida", "✅ Calidad"]
    )

    with tab1:
        por_anio = consultar("SELECT * FROM service.ind_casos_anio ORDER BY anio")
        mensual = consultar("SELECT * FROM service.ind_mensual ORDER BY anio_mes")

        fig = go.Figure()
        fig.add_bar(
            x=por_anio["anio"].astype(str), y=por_anio["casos"], name="Casos por año",
            marker_color=COLORES[0], text=por_anio["casos"], textposition="outside",
        )
        fig.update_traces(texttemplate="%{text:,}", textfont_size=10)
        fig.update_layout(title="Casos de delitos informáticos por año")
        fig.update_xaxes(type="category")
        st.plotly_chart(estilo(fig), use_container_width=True, theme="streamlit")

        fig2 = px.line(
            mensual, x="anio_mes", y="casos", markers=True,
            title="Evolución mensual de casos", color_discrete_sequence=[COLORES[1]],
        )
        fig2.update_xaxes(tickangle=-45, nticks=24)
        st.plotly_chart(estilo(fig2), use_container_width=True, theme="streamlit")

        st.dataframe(
            por_anio.rename(columns={
                "anio": "Año", "casos": "Casos", "registros": "Registros",
                "participacion": "Participación",
            }),
            use_container_width=True, hide_index=True,
        )

    with tab2:
        conductas = consultar(
            """
            SELECT codigo, conducta, SUM(casos) AS casos
            FROM service.ind_conducta_anio
            GROUP BY codigo, conducta
            ORDER BY casos DESC
            """
        )
        conductas["etiqueta"] = conductas["codigo"] + " · " + conductas["conducta"].str.replace(
            r"^ARTICULO\s+269[A-J]\.\s*", "", regex=True
        ).str.title()

        fig = px.bar(
            conductas.sort_values("casos"), x="casos", y="etiqueta", orientation="h",
            title="Casos por conducta (todo el periodo)",
            color="casos", color_continuous_scale="Blues", text="casos",
            labels={"etiqueta": "", "casos": "Casos"},
        )
        fig.update_layout(coloraxis_showscale=False)
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(estilo(fig, 430), use_container_width=True, theme="streamlit")

        por_anio = consultar("SELECT * FROM service.ind_conducta_anio ORDER BY anio")
        fig2 = px.bar(
            por_anio, x=por_anio["anio"].astype(str), y="casos", color="codigo",
            title="Composición por conducta y año", barmode="stack",
            color_discrete_sequence=COLORES,
            labels={"anio": "Año", "casos": "Casos", "codigo": "Artículo"},
        )
        fig2.update_xaxes(type="category")
        st.plotly_chart(estilo(fig2, 380), use_container_width=True, theme="streamlit")

    with tab3:
        anios = consultar("SELECT DISTINCT anio FROM service.ind_departamento_anio ORDER BY anio")
        anio_sel = st.selectbox("Año", anios["anio"].tolist(), index=len(anios) - 1)
        deptos = consultar(
            f"""
            SELECT departamento, casos, ranking
            FROM service.ind_departamento_anio
            WHERE anio = {int(anio_sel)}
            ORDER BY ranking
            """
        )
        fig = px.bar(
            deptos.head(12).sort_values("casos"), x="casos", y="departamento",
            orientation="h", title=f"Departamentos con más casos en {int(anio_sel)}",
            color="casos", color_continuous_scale="Teal", text="casos",
        )
        fig.update_layout(coloraxis_showscale=False)
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(estilo(fig, 430), use_container_width=True, theme="streamlit")

        munis = consultar(
            """
            SELECT ranking, municipio, departamento, casos
            FROM service.ind_municipio
            ORDER BY ranking
            LIMIT 30
            """
        )
        st.subheader("Top 30 municipios del periodo")
        st.dataframe(
            munis.rename(columns={
                "ranking": "#", "municipio": "Municipio",
                "departamento": "Departamento", "casos": "Casos",
            }),
            use_container_width=True, hide_index=True,
        )
        st.download_button(
            "Descargar indicadores por departamento (CSV)",
            deptos.to_csv(index=False).encode("utf-8"),
            file_name=f"delitos_por_departamento_{anio_sel}.csv",
            mime="text/csv",
        )

    with tab4:
        stream = consultar(
            """
            SELECT fecha, codigo, conducta, casos
            FROM service.stream_diario
            ORDER BY fecha
            """
        )
        if stream.empty:
            st.info(
                "La capa rápida aún no tiene datos. Ejecute: "
                "`python -m src.streaming --duracion 60`"
            )
        else:
            total = int(stream["casos"].sum())
            st.metric("Casos procesados por el flujo (ventana diaria)", f"{total:,}".replace(",", "."))
            diario = stream.groupby("fecha", as_index=False)["casos"].sum()
            fig = px.area(
                diario, x="fecha", y="casos",
                title="Capa rápida: casos por día (Structured Streaming sobre Kafka)",
                color_discrete_sequence=[COLORES[3]],
                labels={"fecha": "", "casos": "Casos"},
            )
            fig.update_xaxes(tickformat="%d/%m")
            st.plotly_chart(estilo(fig), use_container_width=True, theme="streamlit")
            st.caption(
                "Arquitectura **Lambda**: esta capa procesa la cola reciente del tópico "
                "`delitos-stream` con ventana diaria, watermark y checkpoint en HDFS; "
                "el histórico se sirve desde la capa batch (Parquet + PostgreSQL)."
            )

    with tab5:
        ruta = "/app/evidencias/calidad_reporte.json"
        if os.path.exists(ruta):
            with open(ruta, encoding="utf-8") as fh:
                reporte = json.load(fh)
            estado = reporte.get("estado", "?")
            (st.success if estado == "ok" else st.error)(
                f"Estado de calidad: {estado.upper()} · "
                f"{reporte.get('filas_curadas', 0):,} filas curadas · "
                f"{reporte.get('registros_descartados_en_curacion', 0):,} descartadas en la curaduría"
            )
            st.dataframe(
                pd.DataFrame(reporte.get("checks", []))[
                    ["nombre", "critico", "fallos", "descripcion"]
                ].rename(columns={
                    "nombre": "Validación", "critico": "Crítica",
                    "fallos": "Fallos", "descripcion": "Descripción",
                }),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Aún no hay reporte de calidad. Ejecute `python -m src.quality`.")

    st.divider()
    st.caption(
        "Proyecto Pipeline de Datos · Gestión de Datos Masivos G002 · "
        "Datos abiertos: https://www.datos.gov.co/Seguridad-y-Defensa/DELITOS-INFORM-TICOS/4v6r-wu98 · "
        "Licencia CC BY-SA 4.0"
    )


if __name__ == "__main__":
    main()
