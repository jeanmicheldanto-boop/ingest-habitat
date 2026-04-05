from __future__ import annotations

import json
import math
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv


OUTPUT_ROOT = Path("outputs")
GEOJSON_CACHE = Path("data") / "departements.geojson"
DEPARTEMENTS_GEOJSON_URL = "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"

POINT_MAP_NAME = "01_gestionnaires_points_tension_financiere.png"
CHOROPLETH_MAP_NAME = "02_taux_tension_financiere_par_departement.png"
POINTS_CSV_NAME = "gestionnaires_hors_saa_metropole.csv"
DEPT_CSV_NAME = "taux_tension_financiere_par_departement.csv"


def metro_department_codes() -> set[str]:
    codes = {f"{idx:02d}" for idx in range(1, 96)}
    codes.discard("20")
    codes.update({"2A", "2B"})
    return codes


def normalize_department_code(value: object) -> str | None:
    if value is None:
        return None
    code = str(value).strip().upper()
    if not code:
        return None
    if code.isdigit() and len(code) == 1:
        return code.zfill(2)
    if code in {"2A", "2B"}:
        return code
    if code.isdigit() and len(code) in {2, 3}:
        return code
    return code


def get_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def fetch_scope_dataframe() -> pd.DataFrame:
    sql = """
    WITH scope AS (
      SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        g.departement_code,
        g.departement_nom,
        g.region,
        g.commune,
        g.latitude::float8 AS latitude,
        g.longitude::float8 AS longitude,
        COALESCE(g.geocode_precision, 'unknown') AS geocode_precision,
        COALESCE(g.signal_financier, FALSE) AS signal_financier
      FROM public.finess_gestionnaire g
      JOIN public.finess_etablissement e ON e.id_gestionnaire = g.id_gestionnaire
      WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
      GROUP BY
        g.id_gestionnaire,
        g.raison_sociale,
        g.departement_code,
        g.departement_nom,
        g.region,
        g.commune,
        g.latitude,
        g.longitude,
        g.geocode_precision,
        g.signal_financier
    )
    SELECT *
    FROM scope
    ORDER BY id_gestionnaire
    """

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return pd.DataFrame(rows)


def load_department_geojson() -> dict:
    GEOJSON_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if not GEOJSON_CACHE.exists():
        response = requests.get(DEPARTEMENTS_GEOJSON_URL, timeout=30)
        response.raise_for_status()
        GEOJSON_CACHE.write_text(response.text, encoding="utf-8")
    return json.loads(GEOJSON_CACHE.read_text(encoding="utf-8"))


def build_output_dir() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUT_ROOT / f"cartes_tension_financiere_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def add_radial_offsets(group: pd.DataFrame) -> pd.DataFrame:
    if len(group) == 1:
        group = group.copy()
        group["plot_latitude"] = group["latitude"]
        group["plot_longitude"] = group["longitude"]
        return group

    group = group.sort_values(["signal_financier", "id_gestionnaire"], ascending=[False, True]).copy()
    lat = float(group.iloc[0]["latitude"])
    lon = float(group.iloc[0]["longitude"])
    offsets_lat: list[float] = []
    offsets_lon: list[float] = []
    placed = 0
    ring = 1

    while placed < len(group):
        ring_capacity = min(6 * ring, len(group) - placed)
        radius_km = 2.5 * ring
        start_angle = (math.pi / 8.0) * (ring % 2)
        for idx in range(ring_capacity):
            angle = start_angle + (2.0 * math.pi * idx / ring_capacity)
            delta_lat = (radius_km / 111.0) * math.sin(angle)
            cos_lat = math.cos(math.radians(lat))
            cos_lat = cos_lat if abs(cos_lat) > 0.01 else 0.01
            delta_lon = (radius_km / (111.0 * cos_lat)) * math.cos(angle)
            offsets_lat.append(lat + delta_lat)
            offsets_lon.append(lon + delta_lon)
        placed += ring_capacity
        ring += 1

    group["plot_latitude"] = offsets_lat
    group["plot_longitude"] = offsets_lon
    return group


def prepare_point_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    point_df = df.dropna(subset=["latitude", "longitude"]).copy()
    packed_groups = []
    for _, group in point_df.groupby(["latitude", "longitude"], sort=False):
        packed_groups.append(add_radial_offsets(group))
    packed = pd.concat(packed_groups, ignore_index=True)
    return packed


def filter_metropole(df: pd.DataFrame) -> pd.DataFrame:
    allowed = metro_department_codes()
    metro_df = df.copy()
    metro_df["departement_code"] = metro_df["departement_code"].map(normalize_department_code)
    return metro_df[metro_df["departement_code"].isin(allowed)].copy()


def build_point_map(df: pd.DataFrame, geojson: dict, output_path: Path) -> None:
    geo_allowed = metro_department_codes()
    features = [
        feature
        for feature in geojson["features"]
        if feature.get("properties", {}).get("code") in geo_allowed
    ]
    metro_geojson = {"type": "FeatureCollection", "features": features}

    grey = df[~df["signal_financier"]]
    red = df[df["signal_financier"]]

    fig = go.Figure()
    fig.add_trace(
        go.Choropleth(
            geojson=metro_geojson,
            locations=[feature["properties"]["code"] for feature in features],
            z=[1] * len(features),
            featureidkey="properties.code",
            colorscale=[[0.0, "#F5F0E8"], [1.0, "#F5F0E8"]],
            showscale=False,
            marker_line_color="#D9D1C5",
            marker_line_width=0.7,
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scattergeo(
            lon=grey["plot_longitude"],
            lat=grey["plot_latitude"],
            mode="markers",
            marker=dict(size=3.3, color="#9FA8A8", opacity=0.42),
            hoverinfo="skip",
            showlegend=True,
            name="Sans tension financiere visible",
        )
    )
    fig.add_trace(
        go.Scattergeo(
            lon=red["plot_longitude"],
            lat=red["plot_latitude"],
            mode="markers",
            marker=dict(size=4.1, color="#D84C3E", opacity=0.88),
            hoverinfo="skip",
            showlegend=True,
            name="Avec tension financiere visible",
        )
    )

    total = len(df)
    total_fin = int(df["signal_financier"].sum())
    fig.update_layout(
        title=dict(
            text=(
                "Gestionnaires ESSMS hors SAA en tension financiere<br>"
                f"<sup>{total:,} gestionnaires localises en metropole, dont {total_fin:,} en tension financiere</sup>"
            ).replace(",", " "),
            x=0.5,
            xanchor="center",
            y=0.96,
            yanchor="top",
            font=dict(size=28, color="#1F2A2E"),
        ),
        width=1600,
        height=1200,
        paper_bgcolor="#FCFAF6",
        plot_bgcolor="#FCFAF6",
        margin=dict(l=20, r=20, t=110, b=30),
        legend=dict(
            orientation="h",
            x=0.5,
            xanchor="center",
            y=0.03,
            yanchor="bottom",
            bgcolor="rgba(252,250,246,0.92)",
            bordercolor="rgba(0,0,0,0)",
            font=dict(size=16),
        ),
        annotations=[
            dict(
                text="Duplication visuelle uniquement quand plusieurs gestionnaires partagent la meme coordonnee",
                x=0.5,
                y=0.0,
                xref="paper",
                yref="paper",
                showarrow=False,
                yshift=8,
                font=dict(size=14, color="#6E7477"),
            )
        ],
    )
    fig.update_geos(
        fitbounds="locations",
        projection_type="mercator",
        showland=False,
        showcountries=False,
        showcoastlines=False,
        showframe=False,
        bgcolor="#FCFAF6",
        resolution=50,
    )
    fig.write_image(str(output_path), width=1600, height=1200, scale=2)


def build_choropleth(df: pd.DataFrame, geojson: dict, output_path: Path) -> pd.DataFrame:
    geo_allowed = metro_department_codes()
    features = [
        feature
        for feature in geojson["features"]
        if feature.get("properties", {}).get("code") in geo_allowed
    ]
    metro_geojson = {"type": "FeatureCollection", "features": features}

    dept = (
        df.groupby(["departement_code", "departement_nom"], as_index=False)
        .agg(total_gestionnaires=("id_gestionnaire", "count"), nb_financiers=("signal_financier", "sum"))
    )
    dept["taux_financier"] = 100.0 * dept["nb_financiers"] / dept["total_gestionnaires"]
    dept = dept.sort_values("departement_code")

    zmax = max(20.0, float(dept["taux_financier"].quantile(0.95)))
    national_rate = 100.0 * float(df["signal_financier"].sum()) / max(len(df), 1)

    fig = go.Figure(
        go.Choropleth(
            geojson=metro_geojson,
            locations=dept["departement_code"],
            z=dept["taux_financier"],
            featureidkey="properties.code",
            colorscale=[
                [0.00, "#FFF3EB"],
                [0.20, "#F6D7C3"],
                [0.40, "#F0AB87"],
                [0.65, "#DE6C52"],
                [1.00, "#8F201F"],
            ],
            zmin=0,
            zmax=zmax,
            marker_line_color="#FFFDF9",
            marker_line_width=1.2,
            colorbar=dict(
                title="% en tension",
                tickformat=".0f",
                thickness=22,
                len=0.68,
                bgcolor="rgba(252,250,246,0.92)",
            ),
            customdata=dept[["departement_nom", "total_gestionnaires", "nb_financiers"]].to_numpy(),
            hovertemplate=(
                "%{customdata[0]} (%{location})<br>"
                "%{customdata[2]} / %{customdata[1]} gestionnaires en tension<br>"
                "%{z:.1f}%<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title=dict(
            text=(
                "Part des gestionnaires ESSMS hors SAA en tension financiere<br>"
                f"<sup>Lecture departementale, France metropolitaine - moyenne metropole: {national_rate:.1f}%</sup>"
            ),
            x=0.5,
            xanchor="center",
            y=0.96,
            yanchor="top",
            font=dict(size=28, color="#1F2A2E"),
        ),
        width=1600,
        height=1200,
        paper_bgcolor="#FCFAF6",
        plot_bgcolor="#FCFAF6",
        margin=dict(l=20, r=20, t=110, b=30),
        annotations=[
            dict(
                text="Taux = gestionnaires avec signal_financier / gestionnaires hors SAA du departement",
                x=0.5,
                y=0.01,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color="#6E7477"),
            )
        ],
    )
    fig.update_geos(
        fitbounds="locations",
        visible=False,
        bgcolor="#FCFAF6",
        showcountries=False,
        showcoastlines=False,
        showland=False,
    )
    fig.write_image(str(output_path), width=1600, height=1200, scale=2)
    return dept


def main() -> None:
    out_dir = build_output_dir()
    raw_df = fetch_scope_dataframe()
    metro_df = filter_metropole(raw_df)
    point_df = prepare_point_dataframe(metro_df)
    geojson = load_department_geojson()

    point_csv_path = out_dir / POINTS_CSV_NAME
    dept_csv_path = out_dir / DEPT_CSV_NAME
    point_map_path = out_dir / POINT_MAP_NAME
    choropleth_map_path = out_dir / CHOROPLETH_MAP_NAME

    point_df.to_csv(point_csv_path, index=False, encoding="utf-8-sig")
    dept_df = build_choropleth(metro_df, geojson, choropleth_map_path)
    dept_df.to_csv(dept_csv_path, index=False, encoding="utf-8-sig")
    build_point_map(point_df, geojson, point_map_path)

    total_scope = len(raw_df)
    metro_scope = len(metro_df)
    point_scope = len(point_df)
    print(f"Output directory: {out_dir}")
    print(f"Gestionnaires hors SAA (France entiere): {total_scope}")
    print(f"Gestionnaires hors SAA retenus sur la carte metropole: {metro_scope}")
    print(f"Gestionnaires metropole localises pour la carte de points: {point_scope}")
    print(f"Point map: {point_map_path}")
    print(f"Choropleth map: {choropleth_map_path}")
    print(f"Point data CSV: {point_csv_path}")
    print(f"Department CSV: {dept_csv_path}")


if __name__ == "__main__":
    main()