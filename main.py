"""
Paginatto — Bot simples de WhatsApp
Fluxos: Carrinho Abandonado e PIX Pendente
Stack: FastAPI + Z-API + (Redis opcional)
"""

import os, re, json, asyncio, logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from offer_rules import build_offer

# Cache/state
REDIS_URL = os.getenv("REDIS_URL")
try:
    import redis  # type: ignore
    rds = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
except Exception:
    rds = None

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("paginatto")

ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")

ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE", "")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN", "")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "")
ZAPI_BASE = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}"

CARTPANDA_WEBHOOK_SECRET = os.getenv("CARTPANDA_WEBHOOK_SECRET", "")
ZAPI_WEBHOOK_SECRET = os.getenv("ZAPI_WEBHOOK_SECRET", "")

from .offer_rules import build_offer

app = FastAPI(title="Paginatto Bot", version="1.0.0")

# ---------------- Utils ----------------
def normalize_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"\D", "", raw)
    if s.startswith("55"):
        return s
    if len(s) >= 10:
        return "55" + s
    return None

def ttl_minutes(minutes: int = 90) -> int:
    return minutes * 60

def store_ctx(phone: str, ctx: Dict[str, Any], minutes: int = 90):
    key = f"ctx:{phone}"
    data = json.dumps(ctx)
    if rds:
        rds.setex(key, ttl_minutes(minutes), data)
    else:
        # in-memory fallback
        _MEM[key] = (datetime.utcnow() + timedelta(minutes=minutes), data)

def read_ctx(phone: str) -> Optional[Dict[str, Any]]:
    key = f"ctx:{phone}"
    if rds:
        raw = rds.get(key)
        return json.loads(raw) if raw else None
    else:
        exp_data = _MEM.get(key)
        if not exp_data:
            return None
        exp, raw = exp_data
        if datetime.utcnow() > exp:
            _MEM.pop(key, None)
            return None
        return json.loads(raw)

def clear_ctx(phone: str):
    key = f"ctx:{phone}"
    if rds:
        rds.delete(key)
    else:
        _MEM.pop(key, None)

# in-memory state if Redis ausente
_MEM: Dict[str, Any] = {}

async def zapi_send_text(phone: str, message: str) -> dict:
    url = f"{ZAPI_BASE}/send-text"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN, "Content-Type": "application/json"}
    payload = {"phone": phone, "message": message}
    async with httpx.AsyncClient(timeout=20) as http:
        resp = await http.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()

# ---------------- Health ----------------
@app.get("/health")
async def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

# ---------------- Webhook CartPanda ----------------
"""
Esperado: CartPanda envia um webhook quando:
- checkout_abandoned
- pix_pending (pedido gerou link e não pagou)
Payload mínimo esperado (adapte aos seus campos reais):
{
  "event": "checkout_abandoned" | "pix_pending",
  "customer": {"name": "Fulano", "phone": "11988887777"},
  "product": {"name": "Tabib 2025", "checkout_url": "https://..."},
  "order": {"id": "ABC123", "total": 29.9}
}
"""

@app.post("/webhook/cartpanda")
async def cartpanda_webhook(
    request: Request,
    x_cartpanda_secret: Optional[str] = Header(None)
):
    if CARTPANDA_WEBHOOK_SECRET and x_cartpanda_secret != CARTPANDA_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

    payload = await request.json()
    event = payload.get("event", "")
    cust = payload.get("customer", {}) or {}
    prod = payload.get("product", {}) or {}
    order = payload.get("order", {}) or {}

    phone = normalize_phone(cust.get("phone"))
    name = cust.get("name", "").strip()
    product_name = prod.get("name", "").strip()
    checkout_url = prod.get("checkout_url", "").strip()

    if not phone or not product_name:
        return JSONResponse({"ok": False, "reason": "missing phone or product"}, status_code=200)

    ctx = {
        "flow": "abandoned" if event == "checkout_abandoned" else "pix_pending" if event == "pix_pending" else "unknown",
        "name": name,
        "product_name": product_name,
        "checkout_url": checkout_url,
        "order": order,
        "created_at": datetime.utcnow().isoformat()
    }
    store_ctx(phone, ctx, minutes=180)

    # Disparo da 1ª mensagem:
    first = f"Oi, esse número é de {name}? Sou {ASSISTANT_NAME} da {BRAND_NAME}."
    await zapi_send_text(phone, first)

    return {"ok": True}

# ---------------- Webhook Z-API (mensagens recebidas) ----------------
"""
Z-API → configure um webhook para mensagens recebidas apontando para /webhook/zapi.
Payload típico:
{
  "message": { "phone": "5511988887777", "text": "sim" }
}
"""

YES_PATTERNS = [r"\bsim\b", r"\bisso\b", r"\bconfirmo\b", r"\bclaro\b", r"\bpositivo\b", r"\bafirmativo\b", r"\bcert[oa]\b"]
BOUGHT_PATTERNS = [r"já comprei", r"ja comprei", r"comprei", r"realizei a compra", r"paguei", r"finalizei"]
QUIT_PATTERNS = [r"desisti", r"não vou", r"nao vou", r"não quero", r"nao quero", r"deixa pra depois"]

def matches(text: str, pats) -> bool:
    t = text.lower()
    return any(re.search(p, t) for p in pats)

@app.post("/webhook/zapi")
async def zapi_webhook(
    request: Request,
    x_zapi_secret: Optional[str] = Header(None)
):
    if ZAPI_WEBHOOK_SECRET and x_zapi_secret != ZAPI_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

    body = await request.json()
    msg = (body.get("message") or {}) if isinstance(body.get("message"), dict) else body
    raw_phone = msg.get("phone") or msg.get("from") or ""
    text = (msg.get("text") or msg.get("body") or "").strip()

    phone = normalize_phone(raw_phone)
    if not phone or not text:
        return {"ok": True}

    ctx = read_ctx(phone)

    # Se não há contexto, responda neutro
    if not ctx:
        await zapi_send_text(phone, f"Oi, aqui é {ASSISTANT_NAME} da {BRAND_NAME}. Como posso ajudar?")
        return {"ok": True}

    name = ctx.get("name") or ""
    product_name = ctx.get("product_name") or ""
    checkout_url = ctx.get("checkout_url") or ""
    flow = ctx.get("flow")

    # 1) Confirmação do número
    if matches(text, YES_PATTERNS) or text.lower() in {"sou eu", "isso mesmo", "eu"}:
        q = f"Você desistiu da compra de '{product_name}'?"
        await zapi_send_text(phone, q)
        return {"ok": True}

    # 2) Cliente afirma que já comprou
    if matches(text, BOUGHT_PATTERNS):
        clear_ctx(phone)
        thanks = "Obrigada pela compra. Qualquer dúvida, a equipe Paginatto está à disposição."
        await zapi_send_text(phone, thanks)
        return {"ok": True}

    # 3) Cliente diz que desistiu → oferecer promoção
    if matches(text, QUIT_PATTERNS):
        headline, detail = build_offer(product_name)
        offer_msg = (
            f"{headline}\n{detail}\n\n"
            f"Seu carrinho: {checkout_url}\n"
            f"Posso aplicar agora e finalizar com você?"
        )
        await zapi_send_text(phone, offer_msg)
        return {"ok": True}

    # 4) Cliente confirma que quer retomar
    if any(k in text.lower() for k in ["quero retomar", "quero comprar", "manda o link", "pode aplicar", "vamos fechar"]):
        go = f"Perfeito, {name}. Segue seu link para concluir: {checkout_url}"
        await zapi_send_text(phone, go)
        return {"ok": True}

    # 5) Falas comuns de dúvida
    if any(k in text.lower() for k in ["qual o preço", "valor", "tem desconto", "até quando", "formas de pagamento", "pix", "cartão"]):
        headline, detail = build_offer(product_name)
        info = (
            f"{headline}\n{detail}\n"
            f"Pagamento por PIX ou cartão. Caso prefira, envio o QR ou o link direto."
        )
        await zapi_send_text(phone, info)
        return {"ok": True}

    # 6) Fallback educado conforme fluxo
    if flow == "pix_pending":
        msg = f"Detectei um PIX pendente do '{product_name}'. Quer que eu reenvie o QR/link agora?"
    else:
        msg = f"Posso te ajudar a concluir '{product_name}'. Prefere link direto ou quer tirar alguma dúvida primeiro?"
    await zapi_send_text(phone, msg)
    return {"ok": True}
