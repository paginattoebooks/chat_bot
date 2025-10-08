# webhook.py
import os
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request, HTTPException
from psycopg_pool import ConnectionPool
from psycopg.types.json import Json

router = APIRouter()

# Autorização simples (mesmo valor que você usa pra enviar via Z-API)
CLIENT_TOKEN = (os.getenv("ZAPI_CLIENT_TOKEN") or os.getenv("ZAPI_TOKEN") or "").strip()

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS cartpanda_events(
  id_evento TEXT PRIMARY KEY,
  tipo_evento TEXT,
  ts TIMESTAMPTZ DEFAULT now(),
  telefone_e164 TEXT,
  email TEXT,
  nome TEXT,
  produto_id TEXT,
  produto_nome TEXT,
  checkout_url TEXT,
  valor NUMERIC,
  moeda TEXT,
  status_pagamento TEXT,
  raw JSONB
);
CREATE INDEX IF NOT EXISTS idx_cp_tel  ON cartpanda_events(telefone_e164);
CREATE INDEX IF NOT EXISTS idx_cp_tipo ON cartpanda_events(tipo_evento);
CREATE INDEX IF NOT EXISTS idx_cp_stat ON cartpanda_events(status_pagamento);
CREATE INDEX IF NOT EXISTS idx_cp_ts   ON cartpanda_events(ts DESC);
"""

INSERT_SQL = """
INSERT INTO cartpanda_events(
  id_evento, tipo_evento, ts, telefone_e164, email, nome,
  produto_id, produto_nome, checkout_url, valor, moeda, status_pagamento, raw
) VALUES (
  %(id_evento)s, %(tipo_evento)s, %(ts)s, %(telefone_e164)s, %(email)s, %(nome)s,
  %(produto_id)s, %(produto_nome)s, %(checkout_url)s, %(valor)s, %(moeda)s, %(status_pagamento)s, %(raw)s
) ON CONFLICT (id_evento) DO NOTHING;
"""


def _get_pool(req: Request) -> ConnectionPool:
    """Pega o pool criado no main.py (app.state.pool)."""
    pool = getattr(req.app.state, "pool", None)
    if not isinstance(pool, ConnectionPool):
        # Isso indica que o main.py não configurou o pool no startup.
        raise HTTPException(status_code=500, detail="DB pool não inicializado (app.state.pool ausente).")
    return pool


def _ensure_schema(req: Request) -> None:
    """Cria a tabela/índices uma única vez por processo."""
    if getattr(req.app.state, "cp_schema_ready", False):
        return
    pool = _get_pool(req)
    with pool.connection() as c, c.cursor() as cur:
        cur.execute(CREATE_SQL)
    req.app.state.cp_schema_ready = True


def _token(req: Request) -> Optional[str]:
    # Aceita header ou querystring (?token=...)
    return req.headers.get("x-client-token") or req.query_params.get("token")


@router.post("/cartpanda")
async def cartpanda(req: Request):
    # Auth simples (opcional): se quiser desativar, remova este bloco
    if CLIENT_TOKEN and _token(req) != CLIENT_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    # Garante schema
    _ensure_schema(req)

    body: Dict[str, Any] = await req.json()

    # Gera um id estável pro evento
    eid = str(
        body.get("id")
        or body.get("event_id")
        or f"{body.get('checkout_id','')}_{body.get('event_type','')}"
    ).strip() or f"evt_{int(datetime.utcnow().timestamp())}"

    customer = body.get("customer") or {}
    product  = body.get("product") or {}

    data = {
        "id_evento": eid,
        "tipo_evento": (body.get("event_type") or body.get("event") or "").strip(),
        "ts": datetime.utcnow(),
        "telefone_e164": (customer.get("phone") or "").strip(),
        "email": (customer.get("email") or "").strip(),
        "nome": (customer.get("name") or "").strip(),
        "produto_id": (product.get("id") or "").strip(),
        "produto_nome": (product.get("name") or "").strip(),
        "checkout_url": (body.get("checkout_url") or product.get("checkout_url") or "").strip(),
        "valor": body.get("amount"),
        "moeda": (body.get("currency") or "").strip(),
        "status_pagamento": (body.get("payment_status") or "").strip(),
        "raw": Json(body),  # guarda JSON bruto
    }

    pool = _get_pool(req)
    with pool.connection() as c, c.cursor() as cur:
        cur.execute(INSERT_SQL, data)

    return {"ok": True, "id_evento": eid}
