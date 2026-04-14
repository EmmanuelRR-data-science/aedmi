# api/core/models.py
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from core.db import Base


class FuenteDatos(Base):
    __tablename__ = "fuentes_datos"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200), nullable=False)
    url_referencia: Mapped[str | None] = mapped_column(Text)
    periodicidad: Mapped[str] = mapped_column(String(20), nullable=False)
    ultima_carga: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    modulo_etl: Mapped[str] = mapped_column(String(100), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default="pendiente")
    activo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notas: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    indicadores: Mapped[list["Indicador"]] = relationship(back_populates="fuente")
    etl_logs: Mapped[list["ETLLog"]] = relationship(back_populates="fuente")


class Indicador(Base):
    __tablename__ = "indicadores"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clave: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    nombre: Mapped[str] = mapped_column(String(300), nullable=False)
    categoria: Mapped[str] = mapped_column(String(50), nullable=False)
    nivel_geografico: Mapped[str] = mapped_column(String(30), nullable=False)
    unidad: Mapped[str | None] = mapped_column(String(100))
    fuente_id: Mapped[int | None] = mapped_column(ForeignKey("public.fuentes_datos.id"))
    descripcion: Mapped[str | None] = mapped_column(Text)
    tipo_grafica: Mapped[str] = mapped_column(String(30), default="bar")
    activo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    fuente: Mapped["FuenteDatos | None"] = relationship(back_populates="indicadores")
    analisis: Mapped[list["Analisis"]] = relationship(back_populates="indicador")


class Analisis(Base):
    __tablename__ = "analisis"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    indicador_id: Mapped[int] = mapped_column(ForeignKey("public.indicadores.id"), nullable=False)
    nivel_geografico: Mapped[str] = mapped_column(String(30), nullable=False)
    entidad_clave: Mapped[str | None] = mapped_column(String(200))
    analisis_ia: Mapped[str | None] = mapped_column(Text)
    analisis_revisado: Mapped[str | None] = mapped_column(Text)
    ia_generado_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revisado_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    indicador: Mapped["Indicador"] = relationship(back_populates="analisis")


class ETLLog(Base):
    __tablename__ = "etl_logs"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fuente_id: Mapped[int | None] = mapped_column(ForeignKey("public.fuentes_datos.id"))
    tipo_ejecucion: Mapped[str] = mapped_column(String(20), nullable=False)
    inicio: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fin: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exitoso: Mapped[bool | None] = mapped_column(Boolean)
    registros_cargados: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errores: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mensaje: Mapped[str | None] = mapped_column(Text)
    usuario: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    fuente: Mapped["FuenteDatos | None"] = relationship(back_populates="etl_logs")
