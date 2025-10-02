"""
Paginatto — Bot simples de WhatsApp
Fluxos: Carrinho Abandonado e PIX Pendente
Stack: FastAPI + Z-API + (Redis opcional)
"""

# -------------------- Imports --------------------
import os, re, json, logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from offer_rules import build_offer

# -------------------- Bootstrap (.env, log) --------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("paginatto")

# -------------------- Config --------------------
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara")
BRAND_NAME = os.getenv("BRAND_NAME", "Paginatto")

# Z-API
ZAPI_INSTANCE = (os.getenv("ZAPI_INSTANCE") or "").strip()
ZAPI_TOKEN = (os.getenv("ZAPI_TOKEN") or "").strip()
CLIENT_TOKEN = (os.getenv("ZAPI_CLIENT_TOKEN") or os.getenv("ZAPI_TOKEN") or "").strip()
ZAPI_BASE = (os.getenv("ZAPI_BASE") or "").strip()

if ZAPI_BASE:
    ZAPI_MSG_URL = ZAPI_BASE.rstrip("/")
else:
    # fallback para o endpoint testado via PowerShell (send-text)
    ZAPI_MSG_URL = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-text"

# Webhook secrets (opcionais)
CARTPANDA_WEBHOOK_SECRET = (os.getenv("CARTPANDA_WEBHOOK_SECRET") or "").strip()
ZAPI_WEBHOOK_SECRET = (os.getenv("ZAPI_WEBHOOK_SECRET") or "").strip()

# Redis (opcional)
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()
try:
    import redis  # type: ignore

    rds = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
except Exception:
    rds = None

# -------------------- App --------------------
app = FastAPI(title="Paginatto Bot", version="1.0.0")

# -------------------- Helpers (estado) --------------------
_MEM: Dict[str, Tuple[datetime, str]] = {}  # fallback em memória


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"\D+", "", raw)
    if s.startswith("55"):
        return s
    if len(s) >= 10:
        return "55" + s
    return None


def ttl_seconds(minutes: int = 90) -> int:
    return minutes * 60


def store_ctx(phone: str, ctx: Dict[str, Any], minutes: int = 180) -> None:
    key = f"ctx:{phone}"
    data = json.dumps(ctx, ensure_ascii=False)
    try:
        if rds:
            rds.setex(key, ttl_seconds(minutes), data)
            return
    except Exception as e:
        log.warning("Redis indisponível (store), usando memória: %s", e)
    _MEM[key] = (datetime.utcnow() + timedelta(minutes=minutes), data)


def read_ctx(phone: str) -> Optional[Dict[str, Any]]:
    key = f"ctx:{phone}"
    try:
        if rds:
            raw = rds.get(key)
            return json.loads(raw) if raw else None
    except Exception as e:
        log.warning("Redis indisponível (read), usando memória: %s", e)
    exp_raw = _MEM.get(key)
    if not exp_raw:
        return None
    exp, raw = exp_raw
    if datetime.utcnow() > exp:
        _MEM.pop(key, None)
        return None
    return json.loads(raw)


def clear_ctx(phone: str) -> None:
    key = f"ctx:{phone}"
    try:
        if rds:
            rds.delete(key)
            return
    except Exception as e:
        log.warning("Redis indisponível (clear), usando memória: %s", e)
    _MEM.pop(key, None)

# -------------------- Helpers (Z-API) --------------------
async def zapi_send_text(phone: str, message: str) -> dict:
    if not (ZAPI_MSG_URL and CLIENT_TOKEN):
        log.error("Z-API não configurada (ZAPI_MSG_URL/CLIENT_TOKEN).")
        return {"ok": False, "error": "zapi_not_configured"}

    headers = {"Content-Type": "application/json", "Client-Token": CLIENT_TOKEN}
    payload = {"phone": phone, "message": message}
    log.info("Z-API POST %s payload=%s", ZAPI_MSG_URL, payload)

    try:
        async with httpx.AsyncClient(timeout=20) as http:
            resp = await http.post(ZAPI_MSG_URL, headers=headers, json=payload)
            txt = (resp.text or "").strip()
            if resp.status_code >= 400:
                log.error("Z-API erro %s: %s", resp.status_code, txt)
                return {"ok": False, "status": resp.status_code, "text": txt}
            try:
                return {"ok": True, "status": resp.status_code, "data": resp.json()}
            except Exception:
                return {"ok": True, "status": resp.status_code, "text": txt}
    except Exception as e:
        log.exception("Falha ao chamar Z-API:")
        return {"ok": False, "error": str(e)}

# -------------------- Patterns & Intents --------------------
YES_PATTERNS = [
    r"\bsim\b",
    r"\bisso\b",
    r"\bconfirmo\b",
    r"\bclaro\b",
    r"\bpositivo\b",
    r"\bafirmativo\b",
    r"\bcert[oa]\b",
]
NO_PATTERNS = [
    r"\bn[aã]o\b",
    r"\bnegativo\b",
    r"\bnope\b",
]
BOUGHT_PATTERNS = [
    r"já comprei",
    r"ja comprei",
    r"comprei",
    r"realizei a compra",
    r"paguei",
    r"finalizei",
]
QUIT_PATTERNS = [
    r"desisti",
    r"não vou",
    r"nao vou",
    r"não quero",
    r"nao quero",
    r"deixa pra depois",
]
STOP_PATTERNS = [
    r"\bpare\b", r"\bpara\b", r"\bstop\b", r"\bcancelar\b", r"\bcancele\b",
    r"\bsair\b", r"\bremover\b", r"\bexcluir\b", r"\bdescadastrar\b",
    r"n[aã]o quero (conversar|falar)", r"n[aã]o me (chame|incomode|envie|mande)",
]

INTENTS: Dict[str, List[str]] = {
    # saudações e educação
    "greeting": [r"\b(oi|ol[aá]|eai|boa\s+noite|boa\s+tarde|bom\s+dia)\b"],
    "thanks": [r"\bobrigad[oa]\b", r"\bvaleu\b"],
    # encerrar/opt-out
    "stop": STOP_PATTERNS,
    # entrega / rastreio (produto é digital)
    "shipping": [
        r"\b(entrega|frete|prazo|chega|chegada|quando\s+chega|vai\s+chegar)\b",
        r"\b(rastreio|rastreamento|c[oó]digo\s+de\s+rastreio|correios|sedex)\b",
        r"\b(endere[cç]o|resid[eê]ncia|receber\s+em\s+casa|envio\s+f[ií]sico)\b",
    ],
    # formas de pagamento
    "payment": [
        r"\bpix\b", r"\bcart[aã]o\b", r"\bcr[eé]dito\b", r"\bdebito\b",
        r"\bparcel", r"forma(s)? de pagamento", r"\bboleto\b",
    ],
    # preço / desconto / promo
    "price": [r"\b(pre[cç]o|valor|quanto\s+custa|qnto)\b"],
    "discount": [r"\b(desconto|cupom|promo[cç][aã]o|oferta)\b"],
    "deadline": [r"\bat[ée]\s+quando\b", r"\b(valid[ao]|vence|prazo\s+da\s+promo)\b"],
    # link / retomada
    "link": [r"\blink\b", r"manda(r)?\s+o?\s*link", r"quero\s+comprar", r"pode\s+aplicar", r"vamos\s+fechar"],
    # dúvidas gerais
    "product_info": [r"como\s+funciona", r"o\s+que\s+[ée]", r"conte[úu]do", r"do\s+que\s+se\s+trata"],
    # pós-compra: não recebeu e-mail / reenvio
    "email_missing": [r"n[aã]o\s+(recebi|chegou).*(email|e-?mail|link)", r"cade\s+o\s+(email|e-?mail|link)"],
    "resend": [r"\b(reenvia|reenviar|enviar\s+de\s+novo|manda\s+de\s+novo|mandar\s+novamente)\b"],
}

def matches(text: str, pats: List[str]) -> bool:
    t = text.lower()
    return any(re.search(p, t) for p in pats)

def detect_intent(text: str) -> str:
    for intent, patterns in INTENTS.items():
        if matches(text, patterns):
            return intent
    # apoia classificações "sim/não" e fluxos específicos
    if matches(text, YES_PATTERNS):
        return "yes"
    if matches(text, NO_PATTERNS):
        return "no"
    if matches(text, BOUGHT_PATTERNS):
        return "bought"
    if matches(text, QUIT_PATTERNS):
        return "quit"
    return "unknown"

# -------------------- Normalizador do corpo Z-API --------------------
def extract_phone_and_text(body: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Normaliza vários formatos da Z-API:
      - { "phone": "...", "text": "..." }
      - { "phone": "...", "text": {"message": "..."} }
      - { "message": {"phone": "...", "text": "..."} }
      - { "data": {"message": {"from": "...", "text": "..."} } }
    """
    def as_text(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, str):
            return x
        if isinstance(x, dict):
            for k in ("message", "text", "body", "caption"):
                v = x.get(k)
                if isinstance(v, str):
                    return v
        return None

    phone = body.get("phone") or body.get("from")
    text = body.get("text") or body.get("body")

    if not phone or not text:
        msg = body.get("message") or {}
        if isinstance(msg, dict):
            phone = phone or msg.get("phone") or msg.get("from")
            text = text or msg.get("text") or msg.get("body")

    if not phone or not text:
        data = body.get("data") or {}
        m2 = (data.get("message") or {}) if isinstance(data, dict) else {}
        if isinstance(m2, dict):
            phone = phone or m2.get("phone") or m2.get("from")
            text = text or m2.get("text") or m2.get("body")

    text = as_text(text)
    return normalize_phone(phone), (text.strip() if isinstance(text, str) else None)

# -------------------- Health & Root --------------------
@app.get("/health")
async def health():
    return {
        "ok": True,
        "ts": datetime.utcnow().isoformat(),
        "zapi_instance_set": bool(ZAPI_INSTANCE),
        "zapi_token_set": bool(CLIENT_TOKEN),
        "redis": bool(rds),
    }


@app.get("/")
async def root():
    return {
        "service": "paginatto-bot",
        "docs": ["/health", "/webhook/cartpanda", "/webhook/zapi"],
        "brand": BRAND_NAME,
        "assistant": ASSISTANT_NAME,
    }

# -------------------- CartPanda webhook (cria contexto e dispara 1ª msg) --------------------
"""
Exemplo (ajuste conforme sua loja):
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
    x_cartpanda_secret: Optional[str] = Header(None),
):
    if CARTPANDA_WEBHOOK_SECRET and x_cartpanda_secret != CARTPANDA_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

    payload = await request.json()
    log.info("CartPanda payload: %s", payload)

    event = (payload.get("event") or "").strip().lower()
    cust = payload.get("customer") or {}
    prod = payload.get("product") or {}
    order = payload.get("order") or {}

    phone = normalize_phone(cust.get("phone"))
    name = (cust.get("name") or "").strip()
    product_name = (prod.get("name") or "").strip()
    checkout_url = (prod.get("checkout_url") or "").strip()

    if not phone or not product_name:
        return JSONResponse({"ok": False, "reason": "missing phone or product"}, status_code=200)

    flow = "abandoned" if event == "checkout_abandoned" else "pix_pending" if event == "pix_pending" else "unknown"
    ctx = {
        "flow": flow,
        "name": name,
        "product_name": product_name,
        "checkout_url": checkout_url,
        "order": order,
        "created_at": datetime.utcnow().isoformat(),
        # estado de conversa
        "confirmed_owner": False,
        "asked": None,
        "last_intent": None,
    }
    store_ctx(phone, ctx, minutes=180)

    first = f"Oi, esse número é de {name}? Sou {ASSISTANT_NAME} da {BRAND_NAME}."
    await zapi_send_text(phone, first)
    return {"ok": True, "flow": flow}

# -------------------- Router de intenções --------------------
async def handle_intent(phone: str, ctx: Dict[str, Any], text: str, intent: str) -> Dict[str, Any]:
    name         = ctx.get("name") or ""
    product_name = ctx.get("product_name") or ""
    checkout_url = ctx.get("checkout_url") or ""
    flow         = ctx.get("flow") or "unknown"

    # ----- opt-out / stop -----
    if intent == "stop" or intent == "quit":
        await zapi_send_text(phone, "Entendido. Vou encerrar por aqui. 🙏 Se mudar de ideia, é só chamar. Boa semana!")
        clear_ctx(phone)
        return {"ok": True}

    # ----- pós-compra informado -----
    if intent == "bought":
        clear_ctx(phone)
        await zapi_send_text(phone, "Obrigada pela compra! 🎉 Qualquer dúvida, estamos à disposição por aqui.")
        return {"ok": True}

    # ----- shipping (produto digital) -----
    if intent == "shipping":
        await zapi_send_text(
            phone,
            "Nosso produto é **100% digital (PDF/e-book)** — não existe frete nem envio físico.\n"
            "Assim que o pagamento é aprovado, você recebe o **link de download** no seu e-mail cadastrado "
            "e, se preferir, posso te enviar o link por aqui também.\n\n"
            "Se você já pagou e não encontrou o e-mail, olhe a caixa *Spam/Lixo/Promoções*. Quer que eu reenvie?"
        )
        ctx["asked"] = "resend_link"
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- pagamento -----
    if intent == "payment":
        await zapi_send_text(
            phone,
            "Aceitamos **PIX** e **cartão** (com parcelamento). Como é digital, a liberação é **imediata** após aprovação: "
            "você recebe o link de download no e-mail cadastrado (posso mandar por aqui também). "
            "Prefere pagar via PIX ou cartão?"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- preço / desconto / deadline -----
    if intent in ("price", "discount", "deadline"):
        headline, detail = build_offer(product_name)
        extra = ""
        if intent == "deadline":
            extra = "\n*Validade:* promoções podem ser por tempo limitado."
        await zapi_send_text(phone, f"{headline}\n{detail}{extra}\nQuer que eu gere o link com a condição?")
        ctx["asked"] = "apply_offer"
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- link direto / retomar -----
    if intent == "link":
        await zapi_send_text(phone, f"Perfeito, {name}. Segue seu link para concluir: {checkout_url}")
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- dúvidas gerais -----
    if intent == "product_info":
        await zapi_send_text(
            phone,
            f"O *{product_name}* é um material digital (PDF) com conteúdo prático para você aplicar hoje mesmo. "
            "Se quiser, te envio um resumo do conteúdo e o link para concluir quando for melhor para você."
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- e-mail não recebido / reenvio -----
    if intent == "email_missing":
        await zapi_send_text(
            phone,
            "Se o pagamento já foi aprovado e o e-mail não chegou, confira *Spam/Lixo/Promoções*. "
            "Se preferir, posso **reencaminhar** o link por aqui. Quer que eu envie agora?"
        )
        ctx["asked"] = "resend_link"
        store_ctx(phone, ctx)
        return {"ok": True}

    if intent == "resend":
        await zapi_send_text(phone, "Claro! Pode me confirmar o e-mail cadastrado ou prefere que eu mande o link por aqui mesmo?")
        ctx["asked"] = "get_email_or_whatsapp"
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- educação -----
    if intent == "greeting":
        # Evita repetir saudação se já temos contexto ativo
        if not ctx.get("confirmed_owner"):
            await zapi_send_text(phone, f"Oi, aqui é {ASSISTANT_NAME} da {BRAND_NAME}. Como posso ajudar?")
        else:
            await zapi_send_text(phone, "Oi! 😊 Como posso ajudar você a finalizar?")
        store_ctx(phone, ctx)
        return {"ok": True}

    if intent == "thanks":
        await zapi_send_text(phone, "Imagina! Qualquer coisa é só chamar. 🙌")
        store_ctx(phone, ctx)
        return {"ok": True}

    # ----- yes/no contextual -----
    asked = ctx.get("asked")
    if intent == "yes":
        if asked == "confirm_desist":
            # cliente confirmou que desistiu
            headline, detail = build_offer(product_name)
            await zapi_send_text(
                phone,
                f"{headline}\n{detail}\n\nSeu carrinho: {checkout_url}\nPosso aplicar agora e finalizar com você?"
            )
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "apply_offer":
            await zapi_send_text(phone, f"Perfeito! Aqui está o link atualizado para concluir: {checkout_url}")
            ctx["asked"] = None
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "resend_link":
            await zapi_send_text(phone, "Fechado! Vou reencaminhar por aqui assim que possível. 👍")
            ctx["asked"] = None
            store_ctx(phone, ctx)
            return {"ok": True}

    if intent == "no":
        if asked in ("confirm_desist", "apply_offer", "resend_link"):
            await zapi_send_text(phone, "Sem problemas! Se surgir qualquer dúvida, estou por aqui. 😉")
            ctx["asked"] = None
            store_ctx(phone, ctx)
            return {"ok": True}

    # ----- fallback conforme fluxo -----
    if flow == "pix_pending":
        msg = f"Detectei um PIX pendente do *{product_name}*. Quer que eu reenvie o QR/link agora?"
    else:
        msg = f"Posso te ajudar a concluir *{product_name}*. Prefere link direto ou quer tirar alguma dúvida primeiro?"
    await zapi_send_text(phone, msg)
    store_ctx(phone, ctx)
    return {"ok": True}

# -------------------- Z-API inbound webhook (mensagens recebidas) --------------------
@app.post("/webhook/zapi")
async def zapi_webhook(
    request: Request,
    x_zapi_secret: Optional[str] = Header(None),
):
    # Durante testes, não falhe com 401 (a Z-API marca como erro).
    if ZAPI_WEBHOOK_SECRET and x_zapi_secret != ZAPI_WEBHOOK_SECRET:
        log.warning("Z-API secret inválido (ignorado em teste).")
        return {"ok": True, "note": "invalid secret (ignored in test)"}

    body: Dict[str, Any] = {}
    ctype = (request.headers.get("content-type") or "").lower()

    try:
        if "application/json" in ctype:
            body = await request.json()
        elif "application/x-www-form-urlencoded" in ctype or "multipart/form-data" in ctype:
            form = await request.form()
            body = {}
            for k, v in form.items():
                if k.startswith("message[") and k.endswith("]"):
                    key = k[8:-1]
                    body.setdefault("message", {})[key] = str(v)
                else:
                    body[k] = str(v)
        else:
            raw = (await request.body()).decode("utf-8", "ignore")
            log.warning("Z-API inbound (raw/unknown ctype): %s", raw)
            return {"ok": True}
    except Exception as e:
        raw = (await request.body()).decode("utf-8", "ignore")
        log.warning("Z-API inbound (parse fail): %s | err=%s", raw, e)
        return {"ok": True}

    log.info("Z-API inbound body: %s", body)

    phone, text = extract_phone_and_text(body)
    if not phone or not text:
        return {"ok": True, "note": "missing phone/text"}

    ctx = read_ctx(phone)

    # Se não há contexto (mensagem solta), cumprimente uma vez
    if not ctx:
        await zapi_send_text(phone, f"Oi, aqui é {ASSISTANT_NAME} da {BRAND_NAME}. Como posso ajudar?")
        # cria um contexto mínimo para não saudar em loop
        ctx = {
            "flow": "unknown",
            "name": "",
            "product_name": "",
            "checkout_url": "",
            "created_at": datetime.utcnow().isoformat(),
            "confirmed_owner": True,
            "asked": None,
            "last_intent": None,
        }
        store_ctx(phone, ctx)
        return {"ok": True}

    # confirmação de titularidade (primeiro contato pós-disparo)
    if not ctx.get("confirmed_owner"):
        if matches(text, YES_PATTERNS) or text.lower() in {"sou eu", "isso mesmo", "eu"}:
            ctx["confirmed_owner"] = True
            ctx["asked"] = "confirm_desist"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, f"Você desistiu da compra de '{ctx.get('product_name')}'?")
            return {"ok": True}
        else:
            # ainda não confirmou — mantenha a conversa aberta, sem looping de saudação
            await zapi_send_text(phone, f"Sou {ASSISTANT_NAME} da {BRAND_NAME}. Posso te ajudar a finalizar o pedido?")
            return {"ok": True}

    # classificar e rotear
    intent = detect_intent(text)
    ctx["last_intent"] = intent
    store_ctx(phone, ctx)
    return await handle_intent(phone, ctx, text, intent)

# Evita 404 da Z-API para pings de status
@app.post("/webhook/zapi/status")
async def zapi_status():
    return {"ok": True}

