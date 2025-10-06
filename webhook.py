# webhook.py
import os
from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import APIRouter, Request, HTTPException
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")
CLIENT_TOKEN = os.getenv("CLIENT_TOKEN", "")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL ausente")

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5, kwargs={"connect_timeout": 5})
router = APIRouter()

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
  id_evento,tipo_evento,ts,telefone_e164,email,nome,
  produto_id,produto_nome,checkout_url,valor,moeda,status_pagamento,raw
) VALUES (
  %(id_evento)s,%(tipo_evento)s,%(ts)s,%(telefone_e164)s,%(email)s,%(nome)s,
  %(produto_id)s,%(produto_nome)s,%(checkout_url)s,%(valor)s,%(moeda)s,%(status_pagamento)s,%(raw)s
) ON CONFLICT (id_evento) DO NOTHING;
"""

@router.on_event("startup")
def _startup():
    with pool.connection() as c, c.cursor() as cur:
        cur.execute(CREATE_SQL)

def _token(req: Request) -> Optional[str]:
    return req.headers.get("x-client-token") or req.query_params.get("token")

@router.post("/cartpanda")
async def cartpanda(req: Request):
    if _token(req) != CLIENT_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    body: Dict[str, Any] = await req.json()
    eid = str(body.get("id") or body.get("event_id") or f"{body.get('checkout_id','')}_{body.get('event_type','')}")
    data = {
        "id_evento": eid,
        "tipo_evento": body.get("event_type"),
        "ts": datetime.utcnow(),
        "telefone_e164": (body.get("customer") or {}).get("phone"),
        "email": (body.get("customer") or {}).get("email"),
        "nome": (body.get("customer") or {}).get("name"),
        "produto_id": (body.get("product") or {}).get("id"),
        "produto_nome": (body.get("product") or {}).get("name"),
        "checkout_url": body.get("checkout_url"),
        "valor": body.get("amount"),
        "moeda": body.get("currency"),
        "status_pagamento": body.get("payment_status"),
        "raw": body,
    }
    with pool.connection() as c, c.cursor() as cur:
        cur.execute(INSERT_SQL, data)
    return {"ok": True, "id_evento": eid}
