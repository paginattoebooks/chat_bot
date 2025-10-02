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
BRAND_NAME     = os.getenv("BRAND_NAME", "Paginatto")

# Z-API
ZAPI_INSTANCE  = (os.getenv("ZAPI_INSTANCE") or "").strip()
ZAPI_TOKEN     = (os.getenv("ZAPI_TOKEN") or "").strip()
CLIENT_TOKEN   = (os.getenv("ZAPI_CLIENT_TOKEN") or os.getenv("ZAPI_TOKEN") or "").strip()
ZAPI_BASE      = (os.getenv("ZAPI_BASE") or "").strip()

if ZAPI_BASE:
    ZAPI_MSG_URL = ZAPI_BASE.rstrip("/")
else:
    # fallback para o endpoint testado via PowerShell (send-text)
    ZAPI_MSG_URL = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-text"

# Webhook secrets (opcionais)
CARTPANDA_WEBHOOK_SECRET = (os.getenv("CARTPANDA_WEBHOOK_SECRET") or "").strip()
ZAPI_WEBHOOK_SECRET      = (os.getenv("ZAPI_WEBHOOK_SECRET") or "").strip()

# ====== URLs de checkout por produto (opcionais, via .env) ======
URL_AIRFRYER      = (os.getenv("URL_AIRFRYER") or "").strip()
URL_TABIB_BUNDLE  = (os.getenv("URL_TABIB_BUNDLE") or "").strip()  # "todos por 19,90"
URL_TABIB_UNIT    = (os.getenv("URL_TABIB_UNIT") or "").strip()    # unitários
URL_MASTERCHEF    = (os.getenv("URL_MASTERCHEF") or "").strip()

PRODUCTS: Dict[str, Dict[str, str]] = {
    "airfryer":   {"name": "Airfryer do Chef"},
    "tabib":      {"name": "Tabib"},
    "masterchef": {"name": "MasterChef"},
}

# Ofertas: TTL (min) para expirar link/condição
OFFER_TTL_MIN = int(os.getenv("OFFER_TTL_MIN", "60"))

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

def now_utc() -> datetime:
    return datetime.utcnow()

def minutes_from_now(m: int) -> datetime:
    return now_utc() + timedelta(minutes=m)

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
    _MEM[key] = (now_utc() + timedelta(minutes=minutes), data)

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
    if now_utc() > exp:
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

# -------------------- Helpers (produtos/oferta) --------------------
def detect_product_key(text: str) -> Optional[str]:
    t = text.lower()
    if "airfryer" in t or "fritadeira" in t:
        return "airfryer"
    if "tabib" in t:
        return "tabib"
    if "masterchef" in t or "master chef" in t:
        return "masterchef"
    return None

def get_checkout_link(product_key: str, *, bundle: bool = False, fallback: str = "") -> str:
    if product_key == "airfryer" and URL_AIRFRYER:
        return URL_AIRFRYER
    if product_key == "tabib":
        if bundle and URL_TABIB_BUNDLE:
            return URL_TABIB_BUNDLE
        if (not bundle) and URL_TABIB_UNIT:
            return URL_TABIB_UNIT
    if product_key == "masterchef" and URL_MASTERCHEF:
        return URL_MASTERCHEF
    return fallback  # cai no checkout_url vindo do CartPanda, se existir

def build_offer_ext(product_key: str, *, bundle: bool = False) -> Tuple[str, str]:
    """
    Usa offer_rules.build_offer(nome_do_produto) para gerar headline/detail.
    Para Tabib bundle, acrescenta a condição 'todos por 19,90'.
    """
    name = PRODUCTS.get(product_key, {}).get("name", product_key)
    headline, detail = build_offer(name)
    if product_key == "tabib" and bundle:
        detail = f"Todos os e-books do Tabib de R$ 49,90 por **R$ 19,90** hoje.\n{detail}"
    return headline, detail

def set_checkout_stage(ctx: Dict[str, Any], *, minutes: int = OFFER_TTL_MIN) -> None:
    ctx["stage"] = "checkout"
    ctx["offer_expires_at"] = minutes_from_now(minutes).isoformat()
    ctx["reminded"] = ctx.get("reminded") or False

def offer_expired(ctx: Dict[str, Any]) -> bool:
    iso = ctx.get("offer_expires_at")
    if not iso:
        return False
    try:
        return now_utc() > datetime.fromisoformat(iso)
    except Exception:
        return False

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
    r"\bsim\b", r"\bisso\b", r"\bconfirmo\b", r"\bclaro\b",
    r"\bpositivo\b", r"\bafirmativo\b", r"\bcert[oa]\b",
]
NO_PATTERNS = [r"\bn[aã]o\b", r"\bnegativo\b", r"\bnope\b"]
BOUGHT_PATTERNS = [r"já comprei", r"ja comprei", r"comprei", r"realizei a compra", r"paguei", r"finalizei"]
QUIT_PATTERNS   = [r"desisti", r"não vou", r"nao vou", r"não quero", r"nao quero", r"deixa pra depois"]
STOP_PATTERNS   = [
    r"\bpare\b", r"\bpara\b", r"\bstop\b", r"\bcancelar\b", r"\bcancele\b",
    r"\bsair\b", r"\bremover\b", r"\bexcluir\b", r"\bdescadastrar\b",
    r"n[aã]o quero (conversar|falar)", r"n[aã]o me (chame|incomode|envie|mande)"
]

INTENTS: Dict[str, List[str]] = {
    "greeting": [r"\b(oi|ol[aá]|eai|boa\s+noite|boa\s+tarde|bom\s+dia)\b"],
    "thanks":   [r"\bobrigad[oa]\b", r"\bvaleu\b"],
    "stop":     STOP_PATTERNS,
    "shipping": [
        r"\b(entrega|frete|prazo|chega|chegada|quando\s+chega|vai\s+chegar)\b",
        r"\b(rastreio|rastreamento|c[oó]digo\s+de\s+rastreio|correios|sedex)\b",
        r"\b(endere[cç]o|resid[eê]ncia|receber\s+em\s+casa|envio\s+f[ií]sico)\b",
    ],
    "payment":  [r"\bpix\b", r"\bcart[aã]o\b", r"\bcr[eé]dito\b", r"\bdebito\b", r"\bparcel", r"forma(s)? de pagamento", r"\bboleto\b"],
    "price":    [r"\b(pre[cç]o|valor|quanto\s+custa|qnto)\b"],
    "discount": [r"\b(desconto|cupom|promo[cç][aã]o|oferta)\b"],
    "deadline": [r"\bat[ée]\s+quando\b", r"\b(valid[ao]|vence|prazo\s+da\s+promo)\b"],
    "link":     [r"\blink\b", r"manda(r)?\s+o?\s*link", r"quero\s+comprar", r"pode\s+aplicar", r"vamos\s+fechar"],
    "product_info": [r"como\s+funciona", r"o\s+que\s+[ée]", r"conte[úu]do", r"do\s+que\s+se\s+trata"],
    "email_missing": [r"n[aã]o\s+(recebi|chegou).*(email|e-?mail|link)", r"cad[êe]\s+o\s+(email|e-?mail|link)"],
    "resend":   [r"\b(reenvia|reenviar|enviar\s+de\s+novo|manda\s+de\s+novo|mandar\s+novamente)\b"],
}

def matches(text: str, pats: List[str]) -> bool:
    t = text.lower()
    return any(re.search(p, t) for p in pats)

def detect_intent(text: str) -> str:
    for intent, patterns in INTENTS.items():
        if matches(text, patterns):
            return intent
    if matches(text, YES_PATTERNS): return "yes"
    if matches(text, NO_PATTERNS):  return "no"
    if matches(text, BOUGHT_PATTERNS): return "bought"
    if matches(text, QUIT_PATTERNS):   return "quit"
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
    text  = body.get("text")  or body.get("body")

    if not phone or not text:
        msg = body.get("message") or {}
        if isinstance(msg, dict):
            phone = phone or msg.get("phone") or msg.get("from")
            text  = text  or msg.get("text")  or msg.get("body")

    if not phone or not text:
        data = body.get("data") or {}
        m2   = (data.get("message") or {}) if isinstance(data, dict) else {}
        if isinstance(m2, dict):
            phone = phone or m2.get("phone") or m2.get("from")
            text  = text  or m2.get("text")  or m2.get("body")

    text = as_text(text)
    return normalize_phone(phone), (text.strip() if isinstance(text, str) else None)

# -------------------- Health & Root --------------------
@app.get("/health")
async def health():
    return {
        "ok": True,
        "ts": now_utc().isoformat(),
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
  "product":  {"name": "Tabib 2025", "checkout_url": "https://..."},
  "order":    {"id": "ABC123", "total": 29.9}
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
    cust  = payload.get("customer") or {}
    prod  = payload.get("product")  or {}
    order = payload.get("order")    or {}

    phone        = normalize_phone(cust.get("phone"))
    name         = (cust.get("name") or "").strip()
    product_name = (prod.get("name") or "").strip()
    checkout_url = (prod.get("checkout_url") or "").strip()

    if not phone or not product_name:
        return JSONResponse({"ok": False, "reason": "missing phone or product"}, status_code=200)

    flow = "abandoned" if event == "checkout_abandoned" else ("pix_pending" if event == "pix_pending" else "unknown")
    product_key = detect_product_key(product_name) or ""

    ctx: Dict[str, Any] = {
        "flow": flow,
        "name": name,
        "product_name": product_name,
        "product_key": product_key,           # "airfryer" | "tabib" | "masterchef"
        "selected_product": product_key or "",# escolhido pelo cliente depois (se vazio, pedimos)
        "checkout_url": checkout_url,
        "created_at": now_utc().isoformat(),
        # conversa
        "stage": "verify",                    # verify -> pick_product -> tabib_menu -> checkout
        "confirmed_owner": False,
        "asked": None,
        "last_intent": None,
        "offer_expires_at": None,
        "reminded": False,
    }
    store_ctx(phone, ctx, minutes=180)

    first = f"Bom dia! Esse número é de {name}? Sou {ASSISTANT_NAME} da {BRAND_NAME}. Responda 1) Sim  2) Não"
    await zapi_send_text(phone, first)
    return {"ok": True, "flow": flow}

# -------------------- Intent handler (global) --------------------
async def handle_intent(phone: str, ctx: Dict[str, Any], text: str, intent: str) -> Dict[str, Any]:
    name           = ctx.get("name") or ""
    product_name   = ctx.get("product_name") or ""
    product_key    = ctx.get("selected_product") or ctx.get("product_key") or ""
    checkout_url   = ctx.get("checkout_url") or ""
    flow           = ctx.get("flow") or "unknown"
    stage          = ctx.get("stage") or "verify"

    # opt-out / parar
    if intent in ("stop", "quit"):
        await zapi_send_text(phone, "Entendido. Vou encerrar por aqui. 🙏 Se mudar de ideia, é só chamar. Boa semana!")
        clear_ctx(phone)
        return {"ok": True}

    # pós-compra
    if intent == "bought":
        clear_ctx(phone)
        await zapi_send_text(phone, "Obrigada pela compra! 🎉 Qualquer dúvida, estamos à disposição por aqui.")
        return {"ok": True}

    # entrega (produto digital)
    if intent == "shipping":
        await zapi_send_text(
            phone,
            "Nosso produto é **100% digital (PDF/e-book)** — não existe frete, rastreio ou envio físico.\n"
            "Assim que o pagamento é aprovado, você recebe o **link de download** no e-mail cadastrado "
            "e, se preferir, posso enviar o link por aqui também. Quer que eu envie por aqui?"
        )
        ctx["asked"] = "resend_link"
        store_ctx(phone, ctx)
        return {"ok": True}

    # pagamento
    if intent == "payment":
        await zapi_send_text(
            phone,
            "Aceitamos **PIX** e **cartão** (com parcelamento). Por ser digital, a liberação é **imediata** após aprovação. "
            "Prefere pagar via PIX ou cartão?"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    # preço / desconto / validade
    if intent in ("price", "discount", "deadline"):
        if not product_key:
            ctx["stage"] = "pick_product"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, "Para te falar o preço certinho, qual produto você quer?\n1) Airfryer  2) Tabib  3) MasterChef")
            return {"ok": True}
        headline, detail = build_offer_ext(product_key, bundle=False)
        extra = "\n*Validade:* promoções podem ser por tempo limitado." if intent == "deadline" else ""
        await zapi_send_text(phone, f"{headline}\n{detail}{extra}\nQuer que eu gere o link com a condição?")
        ctx["asked"] = "apply_offer"
        store_ctx(phone, ctx)
        return {"ok": True}

    # link direto
    if intent == "link":
        # escolhe link de acordo com produto/estágio
        link = checkout_url
        if product_key:
            if product_key == "tabib" and stage == "tabib_menu" and ctx.get("tabib_bundle"):
                link = get_checkout_link("tabib", bundle=True, fallback=checkout_url)
            else:
                link = get_checkout_link(product_key, bundle=False, fallback=checkout_url)
        await zapi_send_text(phone, f"Perfeito, {name}. Segue seu link para concluir: {link}")
        store_ctx(phone, ctx)
        return {"ok": True}

    # dúvidas gerais
    if intent == "product_info":
        base = product_name or PRODUCTS.get(product_key, {}).get("name", "")
        await zapi_send_text(
            phone,
            f"O *{base or 'produto'}* é um material digital (PDF) com conteúdo prático para aplicar hoje mesmo. "
            "Se quiser, te envio um resumo e o link para concluir quando for melhor para você."
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    # e-mail não recebido / reenvio
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
        await zapi_send_text(phone, "Claro! Posso mandar o link por aqui mesmo. Quer que eu envie agora?")
        ctx["asked"] = "resend_link"
        store_ctx(phone, ctx)
        return {"ok": True}

    # educação
    if intent == "greeting":
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

    # confirmação contextual (yes/no)
    asked = ctx.get("asked")
    if intent == "yes":
        if asked == "confirm_desist":
            # cliente disse que desistiu -> oferta
            pk = product_key or detect_product_key(product_name) or ""
            headline, detail = build_offer_ext(pk or "tabib", bundle=False)
            link = get_checkout_link(pk or "tabib", bundle=False, fallback=ctx.get("checkout_url",""))
            await zapi_send_text(phone, f"{headline}\n{detail}\n\nSeu carrinho: {link}\nPosso aplicar agora e finalizar com você?")
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "apply_offer":
            # reenvia link
            pk = product_key or detect_product_key(product_name) or ""
            link = get_checkout_link(pk or "tabib", bundle=bool(ctx.get("tabib_bundle")), fallback=ctx.get("checkout_url",""))
            await zapi_send_text(phone, f"Perfeito! Aqui está o link atualizado para concluir: {link}")
            ctx["asked"] = None
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "resend_link":
            pk = product_key or detect_product_key(product_name) or ""
            link = get_checkout_link(pk or "tabib", bundle=bool(ctx.get("tabib_bundle")), fallback=ctx.get("checkout_url",""))
            await zapi_send_text(phone, f"Aqui está: {link}\nSe preferir, posso te acompanhar até a confirmação. 🙂")
            ctx["asked"] = None
            store_ctx(phone, ctx)
            return {"ok": True}

    if intent == "no":
        if asked in ("confirm_desist", "apply_offer", "resend_link"):
            await zapi_send_text(phone, "Sem problemas! Se surgir qualquer dúvida, estou por aqui. 😉")
            ctx["asked"] = None
            store_ctx(phone, ctx)
            return {"ok": True}

    # fallback por fluxo
    if flow == "pix_pending":
        msg = f"Detectei um PIX pendente do *{product_name or PRODUCTS.get(product_key,{}).get('name','produto')}*. Quer que eu reenvie o QR/link agora?"
    else:
        base = product_name or PRODUCTS.get(product_key, {}).get("name", "produto")
        msg = f"Posso te ajudar a concluir *{base}*. Prefere link direto ou quer tirar alguma dúvida primeiro?"
    await zapi_send_text(phone, msg)
    store_ctx(phone, ctx)
    return {"ok": True}

# -------------------- Stage router --------------------
def is_option(text: str, *options: str) -> bool:
    t = text.strip().lower()
    return t in options

async def route_stage(phone: str, ctx: Dict[str, Any], text: str) -> Dict[str, Any]:
    stage = ctx.get("stage") or "verify"
    tlow  = text.lower()

    # ===== CHECKOUT: trata expiração, compra, parar, reenvio, dúvidas =====
    if stage == "checkout":
        if offer_expired(ctx):
            await zapi_send_text(phone, "A condição anterior expirou. Posso **renovar** a oferta e te mandar o link atualizado?")
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        # deixa o handler global cuidar do restante
        return await handle_intent(phone, ctx, text, detect_intent(text))

    # ===== VERIFY: confirmar se é o dono do número =====
    if stage == "verify":
        if is_option(tlow, "1", "sim", "sou eu", "isso mesmo", "eu"):
            ctx["confirmed_owner"] = True
            # Se já temos um produto vindo do webhook, seguimos direto
            pk = ctx.get("product_key") or ""
            if pk == "tabib":
                ctx["stage"] = "tabib_menu"
                ctx["tabib_bundle"] = False
                store_ctx(phone, ctx)
                await zapi_send_text(
                    phone,
                    "Tenho uma **mega promoção** pra você no Tabib.\n"
                    "Como prefere?\n1) Todos os e-books por 19,90\n2) Ver opções unitárias\n3) Voltar"
                )
                return {"ok": True}
            elif pk in ("airfryer", "masterchef"):
                headline, detail = build_offer_ext(pk, bundle=False)
                link = get_checkout_link(pk, bundle=False, fallback=ctx.get("checkout_url",""))
                set_checkout_stage(ctx)
                store_ctx(phone, ctx)
                await zapi_send_text(phone, f"{headline}\n{detail}\n\nLink para concluir: {link}")
                return {"ok": True}
            else:
                ctx["stage"] = "pick_product"
                store_ctx(phone, ctx)
                await zapi_send_text(phone, "Qual produto você quer?\n1) Airfryer  2) Tabib  3) MasterChef")
                return {"ok": True}

        if is_option(tlow, "2", "não", "nao", "não sou", "nao sou"):
            await zapi_send_text(phone, "Sem problemas, obrigado! Se precisar, é só chamar. 🙏")
            clear_ctx(phone)
            return {"ok": True}

        # não entendeu -> tenta intenções globais (stop, shipping, etc.)
        return await handle_intent(phone, ctx, text, detect_intent(text))

    # ===== PICK_PRODUCT: escolher 1/2/3 ou por palavra =====
    if stage == "pick_product":
        choice = None
        if is_option(tlow, "1") or "airfryer" in tlow:
            choice = "airfryer"
        elif is_option(tlow, "2") or "tabib" in tlow:
            choice = "tabib"
        elif is_option(tlow, "3") or "masterchef" in tlow or "master chef" in tlow:
            choice = "masterchef"

        if not choice:
            await zapi_send_text(phone, "Não entendi. Pode me responder com 1) Airfryer  2) Tabib  3) MasterChef?")
            return {"ok": True}

        ctx["selected_product"] = choice

        if choice == "tabib":
            ctx["stage"] = "tabib_menu"
            ctx["tabib_bundle"] = False
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                "Perfeito! Para o Tabib, prefere:\n1) **Todos** os e-books por 19,90\n2) Ver **unitários**\n3) Voltar"
            )
            return {"ok": True}

        # Airfryer / MasterChef -> oferta direta
        headline, detail = build_offer_ext(choice, bundle=False)
        link = get_checkout_link(choice, bundle=False, fallback=ctx.get("checkout_url",""))
        set_checkout_stage(ctx)
        store_ctx(phone, ctx)
        await zapi_send_text(phone, f"{headline}\n{detail}\n\nLink para concluir: {link}")
        return {"ok": True}

    # ===== TABIB_MENU: bundle 19,90, unitários ou voltar =====
    if stage == "tabib_menu":
        if is_option(tlow, "1"):
            ctx["tabib_bundle"] = True
            headline, detail = build_offer_ext("tabib", bundle=True)
            link = get_checkout_link("tabib", bundle=True, fallback=ctx.get("checkout_url",""))
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(phone, f"{headline}\n{detail}\n\nLink para concluir: {link}")
            return {"ok": True}
        if is_option(tlow, "2"):
            ctx["tabib_bundle"] = False
            headline, detail = build_offer_ext("tabib", bundle=False)
            link = get_checkout_link("tabib", bundle=False, fallback=ctx.get("checkout_url",""))
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(phone, f"{headline}\n{detail}\n\nOpções unitárias aqui: {link}")
            return {"ok": True}
        if is_option(tlow, "3", "voltar"):
            ctx["stage"] = "pick_product"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, "Certo! Qual produto você quer?\n1) Airfryer  2) Tabib  3) MasterChef")
            return {"ok": True}

        # qualquer outra coisa -> deixa intenções globais tentarem
        return await handle_intent(phone, ctx, text, detect_intent(text))

    # fallback se stage desconhecido
    return await handle_intent(phone, ctx, text, detect_intent(text))

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

    # Mensagem solta (sem contexto) → saudação única + cria contexto mínimo
    if not ctx:
        await zapi_send_text(phone, f"Oi, aqui é {ASSISTANT_NAME} da {BRAND_NAME}. Como posso ajudar?")
        ctx = {
            "flow": "unknown",
            "name": "",
            "product_name": "",
            "product_key": "",
            "selected_product": "",
            "checkout_url": "",
            "created_at": now_utc().isoformat(),
            "stage": "verify",
            "confirmed_owner": True,
            "asked": None,
            "last_intent": None,
            "offer_expires_at": None,
            "reminded": False,
        }
        store_ctx(phone, ctx)
        return {"ok": True}

    # confirmação inicial do dono do número (pós-disparo)
    if not ctx.get("confirmed_owner"):
        if matches(text, YES_PATTERNS) or text.lower() in {"sou eu", "isso mesmo", "eu", "1"}:
            ctx["confirmed_owner"] = True
            ctx["asked"] = "confirm_desist"
            store_ctx(phone, ctx)
            base = ctx.get("product_name") or PRODUCTS.get(ctx.get("product_key",""), {}).get("name", "produto")
            await zapi_send_text(phone, f"Você desistiu da compra de '{base}'? 1) Sim  2) Não")
            return {"ok": True}
        else:
            # ainda não confirmou — manter conversa aberta sem loop
            await zapi_send_text(phone, f"Sou {ASSISTANT_NAME} da {BRAND_NAME}. Posso te ajudar a finalizar o pedido?")
            return {"ok": True}

    # roteamento por estágio
    ctx["last_intent"] = detect_intent(text)
    store_ctx(phone, ctx)
    return await route_stage(phone, ctx, text)

# Evita 404 da Z-API para pings de status
@app.post("/webhook/zapi/status")
async def zapi_status():
    return {"ok": True}


