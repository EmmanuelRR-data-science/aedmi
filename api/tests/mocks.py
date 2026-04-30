# api/tests/mocks.py
from __future__ import annotations

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock


def row_result(
    all_rows: list | None = None,
    one: object = None,
    scalars_list: list | None = None,
) -> MagicMock:
    m = MagicMock()
    s = m.scalars.return_value
    if scalars_list is not None:
        s.all.return_value = scalars_list
    else:
        s.all.return_value = all_rows or []
    m.scalar_one_or_none.return_value = one
    m.mappings.return_value.all.return_value = []
    return m


def make_async_db_session(execute_handler: Callable | None = None) -> AsyncMock:
    session = AsyncMock()
    if execute_handler is None:
        default_r = row_result(scalars_list=[])
        session.execute = AsyncMock(return_value=default_r)
    else:
        session.execute = AsyncMock(side_effect=execute_handler)
    session.get = AsyncMock(return_value=None)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    def _add_assign_id(obj: object) -> None:
        if obj is not None and getattr(obj, "id", None) is None:
            try:
                object.__setattr__(obj, "id", 1)
            except Exception:
                try:
                    setattr(obj, "id", 1)
                except Exception:
                    pass

    session.add = MagicMock(side_effect=_add_assign_id)

    async def _refresh_set_id(obj: object) -> None:
        _add_assign_id(obj)

    session.refresh = AsyncMock(side_effect=_refresh_set_id)
    return session
