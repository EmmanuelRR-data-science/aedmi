from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class UploadPreview:
    columnas_detectadas: list[str]
    filas_preview: list[dict[str, Any]]
    total_filas: int
    formato: str  # "xlsx" o "csv"


@dataclass
class EstructuraDiff:
    columnas_faltantes: list[str] = field(default_factory=list)
    columnas_nuevas: list[str] = field(default_factory=list)
    hay_diferencias: bool = False


def parse_file(content: bytes, filename: str) -> pd.DataFrame:
    """Parsea un archivo XLSX o CSV y retorna un DataFrame."""
    ext = filename.lower().rsplit(".", 1)[-1]
    if ext in ("xlsx", "xls"):
        return pd.read_excel(io.BytesIO(content))
    elif ext == "csv":
        return pd.read_csv(io.BytesIO(content))
    else:
        raise ValueError(f"Formato no soportado: '{ext}'. Use XLSX o CSV.")


def get_preview(content: bytes, filename: str, n_rows: int = 5) -> UploadPreview:
    """Retorna preview de las primeras n_rows filas y columnas detectadas."""
    df = parse_file(content, filename)
    ext = filename.lower().rsplit(".", 1)[-1]
    formato = "xlsx" if ext in ("xlsx", "xls") else "csv"

    preview_df = df.head(n_rows).where(pd.notnull(df.head(n_rows)), None)
    filas = preview_df.to_dict(orient="records")

    return UploadPreview(
        columnas_detectadas=list(df.columns),
        filas_preview=filas,
        total_filas=len(df),
        formato=formato,
    )


def diff_estructura(
    columnas_detectadas: list[str],
    columnas_esperadas: list[str],
) -> EstructuraDiff:
    """Compara columnas detectadas vs esperadas y retorna las diferencias."""
    detectadas = set(columnas_detectadas)
    esperadas = set(columnas_esperadas)

    faltantes = sorted(esperadas - detectadas)
    nuevas = sorted(detectadas - esperadas)

    return EstructuraDiff(
        columnas_faltantes=faltantes,
        columnas_nuevas=nuevas,
        hay_diferencias=bool(faltantes or nuevas),
    )


def load_dataframe(
    content: bytes,
    filename: str,
    indicador_id: int,
    nivel_geografico: str,
    columnas_mapeo: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Carga un archivo y retorna lista de registros listos para insertar en BD.
    columnas_mapeo: {"columna_archivo": "columna_bd"}
    """
    df = parse_file(content, filename)
    df = df.rename(columns=columnas_mapeo)
    df["indicador_id"] = indicador_id
    df["nivel_geografico"] = nivel_geografico

    # Eliminar filas completamente vacías
    df = df.dropna(how="all")

    return df.where(pd.notnull(df), None).to_dict(orient="records")
