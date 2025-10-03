"""
Paginatto — Bot de WhatsApp
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

# Site institucional (pedido do cliente)
SITE_URL  = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/").strip()

# Dados legais (segurança/confiança)
LEGAL_NAME = os.getenv("LEGAL_NAME", "PAGINATTO").strip()
LEGAL_CNPJ = os.getenv("LEGAL_CNPJ", "57.941.903/0001-94").strip()

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
app = FastAPI(title="Paginatto Bot", version="2.0.0")

# -------------------- Helpers (tempo/estado) --------------------
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

# -------------------- Catálogo (JSON com aliases) --------------------
CATALOG_PATH  = os.getenv("CATALOG_PATH", "catalog.json").strip()
CATALOG_JSON_ENV = os.getenv("CATALOG_JSON", "").strip()

Catalog: List[Dict[str, Any]] = []
CatalogBySKU: Dict[str, Dict[str, Any]] = {}
AliasIndex: Dict[str, str] = {}          # alias/token -> sku
FamilyIndex: Dict[str, List[str]] = {}   # family -> [skus]

def _infer_family(item: Dict[str, Any]) -> str:
    fam = (item.get("family") or "").strip().lower()
    if fam:
        return fam
    name = (item.get("name") or "").lower()
    if "tabib" in name: return "tabib"
    if "airfryer" in name: return "airfryer"
    if "masterchef" in name or "master chef" in name: return "masterchef"
    return "outros"

def _safe_tokens(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower(), flags=re.UNICODE)

def _load_catalog() -> List[Dict[str, Any]]:
    # 1) variável de ambiente CATALOG_JSON (texto JSON)
    if CATALOG_JSON_ENV:
        try:
            return json.loads(CATALOG_JSON_ENV)
        except Exception as e:
            log.warning("Falha lendo CATALOG_JSON do .env: %s", e)
    # 2) arquivo catalog.json
    try:
        if os.path.exists(CATALOG_PATH):
            with open(CATALOG_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception as e:
        log.warning("Falha lendo %s: %s", CATALOG_PATH, e)
    # 3) fallback mínimo (baseado nos itens enviados)
    return [
      {
        "sku": "TABIB_V1",
        "name": "Tabib Volume 1: Tratamento de Dores e Inflamações",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919679:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-1.png",
        "aliases": ["tabib 1","tabib1","v1","volume 1","inflamacoes","tratamento de dores"],
        "description": "Guia para combater dores de cabeça, musculares e articulares, além de inflamações crônicas.",
        "family": "tabib"
      },
      {
        "sku": "TABIB_V2",
        "name": "Tabib Volume 2: Saúde Respiratória e Imunidade",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919682:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-2.png",
        "aliases": ["tabib 2","tabib2","v2","volume 2","respiratoria","imunidade"],
        "description": "Focado na saúde respiratória e fortalecimento da imunidade.",
        "family": "tabib"
      },
      {
        "sku": "TABIB_V3",
        "name": "Tabib Volume 3: Saúde Digestiva e Metabólica",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919686:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-3.png",
        "aliases": ["tabib 3","tabib3","v3","volume 3","digestiva","metabolica"],
        "description": "Receitas para equilíbrio digestivo e regulação do metabolismo.",
        "family": "tabib"
      },
      {
        "sku": "TABIB_V4",
        "name": "Tabib Volume 4: Saúde Mental e Energética",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/1669197:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-4.png",
        "aliases": ["tabib 4","tabib4","v4","volume 4","saude mental","energia"],
        "description": "Bem-estar emocional, redução do estresse e aumento da energia.",
        "family": "tabib"
      },
      {
        "sku": "TABIB_24_25_BUNDLE",
        "name": "Tabib 2025 + Bônus 19,90 + Tabib 2024",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229263:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-2025-bonus-2024.png",
        "aliases": ["tabib 2025","tabib 2024","tabib pacote","tabib combo","todos","bundle"],
        "description": "Pacote com as edições 2025 e 2024.",
        "family": "tabib"
      },
      {
        "sku": "TABIB_FULL",
        "name": "Tabib completo",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229277:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-completo.png",
        "aliases": ["tabib completo","colecao tabib","coleção tabib","bundle tabib","todos os volumes"],
        "description": "Coletânea com todos os volumes Tabib.",
        "family": "tabib"
      },
      {
        "sku": "ANTIDOTO",
        "name": "Antídoto - Antídotos indígenas",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919637:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/antidoto.png",
        "aliases": ["antidoto","antídoto","o livro antidoto","antidotos indigenas"],
        "description": "Receitas inspiradas em saberes indígenas para antídotos naturais.",
        "family": "outros"
      },
      {
        "sku": "KURIMA",
        "name": "Kurimã - Óleos essenciais",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919661:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/kurima.png",
        "aliases": ["kurima","oleos essenciais","óleos essenciais"],
        "description": "Guia prático de óleos essenciais com receitas e usos seguros.",
        "family": "outros"
      },
      {
        "sku": "BALSAMO",
        "name": "Bálsamo - Pomadas naturais",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919668:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/balsamo.png",
        "aliases": ["balsamo","pomadas naturais","pomada natural"],
        "description": "Fórmulas de pomadas naturais para dores, feridas e inflamações.",
        "family": "outros"
      },
      {
        "sku": "PRESSAO_ALTA_PLAN",
        "name": "Tratamento Natural Personalizado para Pressão Alta",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/174502432:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/pressao-alta.png",
        "aliases": ["pressao alta","hipertensao","tratamento personalizado"],
        "description": "Plano individualizado com alimentação, ervas e exercícios.",
        "family": "outros"
      },
      {
        "sku": "AIRFRYER_300",
        "name": "300 receitas para AirFryer",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/176702038:1",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/airfryer-300.png",
        "aliases": ["airfryer","receitas airfryer","300 receitas"],
        "description": "Coletânea prática de 300 receitas para airfryer.",
        "family": "airfryer"
      }
    ]

def _index_catalog() -> None:
    global Catalog, CatalogBySKU, AliasIndex, FamilyIndex
    Catalog       = _load_catalog()
    CatalogBySKU  = {}
    AliasIndex    = {}
    FamilyIndex   = {}

    for it in Catalog:
        sku  = it.get("sku") or it.get("SKU") or it.get("id")
        name = it.get("name") or ""
        if not sku:
            continue
        it["family"] = _infer_family(it)
        CatalogBySKU[sku] = it
        # indexa aliases + nome tokenizado
        aliases = list(set((it.get("aliases") or []) + [name]))
        # cria aliases automáticos para Tabib volumes
        if it["family"] == "tabib":
            m = re.search(r"volume\s*(\d+)", name.lower())
            if m:
                v = m.group(1)
                aliases += [f"v{v}", f"volume {v}", f"tabib {v}"]
        for a in aliases:
            for tok in _safe_tokens(a):
                AliasIndex[tok] = sku
        # família
        FamilyIndex.setdefault(it["family"], []).append(sku)

    log.info("Catálogo carregado: %d itens; famílias: %s", len(Catalog), list(FamilyIndex.keys()))

_index_catalog()

def find_by_text(text: str) -> List[Dict[str, Any]]:
    toks = _safe_tokens(text)
    hits = []
    for t in toks:
        sku = AliasIndex.get(t)
        if sku and sku in CatalogBySKU:
            hits.append(CatalogBySKU[sku])
    # se não achou por alias, tenta por nome contendo todos tokens
    if not hits:
        for it in Catalog:
            nm = (it.get("name") or "").lower()
            if all(tok in nm for tok in toks):
                hits.append(it)
    # remove duplicados preservando ordem
    seen = set()
    out = []
    for it in hits:
        s = it["sku"]
        if s not in seen:
            out.append(it); seen.add(s)
    return out

def menu_tabib_text() -> str:
    # ordena volumes V1..V4 se existirem + opção 5 (bundle 24/25 ou full)
    vols = []
    for v in ["TABIB_V1","TABIB_V2","TABIB_V3","TABIB_V4"]:
        if v in CatalogBySKU:
            vols.append(CatalogBySKU[v]["name"])
    opt5 = None
    if "TABIB_24_25_BUNDLE" in CatalogBySKU:
        opt5 = CatalogBySKU["TABIB_24_25_BUNDLE"]["name"]
    elif "TABIB_FULL" in CatalogBySKU:
        opt5 = CatalogBySKU["TABIB_FULL"]["name"]
    lines = []
    for i, nm in enumerate(vols, start=1):
        lines.append(f"{i}) {nm}")
    if opt5:
        lines.append(f"5) {opt5}")
    if not lines:
        lines = ["1) Tabib (todos)", "2) Volumes unitários", "3) Voltar"]
    header = "Temos estas opções do *Tabib*: \n"
    footer = "\nResponda com o número (1–5) ou diga, por ex., 'v3' / 'volume 3'."
    return header + "\n".join(lines) + footer

def find_tabib_choice_by_number(num: str) -> Optional[Dict[str, Any]]:
    m = re.fullmatch(r"[1-5]", num)
    if not m:
        return None
    i = int(num)
    mapping = {
        1: "TABIB_V1",
        2: "TABIB_V2",
        3: "TABIB_V3",
        4: "TABIB_V4",
        5: "TABIB_24_25_BUNDLE" if "TABIB_24_25_BUNDLE" in CatalogBySKU else "TABIB_FULL"
    }
    sku = mapping.get(i)
    if sku and sku in CatalogBySKU:
        return CatalogBySKU[sku]
    return None

# -------------------- Helpers (oferta/estágio) --------------------
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

# segurança/confiança
TRUST_PATTERNS = [
    r"\bgolpe(s)?\b", r"\bfraude(s)?\b", r"\bscam\b", r"\bfake\b",
    r"\bseguran[çc]a\b", r"\bsegur[ao]\b", r"\bconfi[aá]vel\b", r"\bconfian[çc]a\b"
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
    "trust":    TRUST_PATTERNS,
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
        "catalog_items": len(Catalog),
        "families": list(FamilyIndex.keys()),
    }

@app.get("/")
async def root():
    return {
        "service": "paginatto-bot",
        "docs": ["/health", "/webhook/cartpanda", "/webhook/zapi"],
        "brand": BRAND_NAME,
        "assistant": ASSISTANT_NAME,
        "site": SITE_URL,
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

    # tenta inferir família do produto
    fam_guess = _infer_family({"name": product_name})

    ctx: Dict[str, Any] = {
        "flow": flow,
        "name": name,
        "product_name": product_name,
        "product_key": fam_guess,             # "airfryer" | "tabib" | "masterchef" | "outros"
        "selected_product": fam_guess or "",
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
    checkout_url   = ctx.get("checkout_url") or ""
    flow           = ctx.get("flow") or "unknown"
    stage          = ctx.get("stage") or "verify"

if intent == "unknown":
    await zapi_send_text(phone, "nao entendi, pode me dizer novamente")
    store_ctx(phone, ctx)
    return {"ok": True}

# detectar produto mencionado a qualquer momento
pk_from_text = detect_product_key(text or "")
if pk_from_text:
    ctx["selected_product"] = pk_from_text
    if pk_from_text == "tabib":
        ctx["stage"] = "tabib_menu"
        ctx["tabib_bundle"] = False
        store_ctx(phone, ctx)
        await zapi_send_text(
            phone,
            "Perfeito! Para o Tabib, prefere:\n1) **Todos** os e-books por 19,90\n2) Ver **unitários**\n3) Voltar"
        )
        return {"ok": True}
    else:
        headline, detail = build_offer_ext(pk_from_text, bundle=False)
        set_checkout_stage(ctx)
        store_ctx(phone, ctx)
        await zapi_send_text(
            phone,
            f"{headline}\n{detail}\n\n"
            f"Se quiser, já pode conferir e comprar pelo nosso site: {SITE_URL}"
        )
        return {"ok": True}


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

    # -------- SEGURANÇA / CONFIANÇA --------
    if intent == "trust":
        msg = (
            f"Entendo sua preocupação. Pode ficar tranquilo(a)! Somos a *{LEGAL_NAME}* (CNPJ **{LEGAL_CNPJ}**) — empresa real, "
            "produto **100% digital (PDF)** e entrega garantida no seu e-mail após aprovação. "
            "Se preferir, posso enviar o link por aqui também. Qualquer dúvida, é só falar. 🙂"
        )
        await zapi_send_text(phone, msg)
        ctx["asked"] = None
        store_ctx(phone, ctx)
        return {"ok": True}

    # entrega (produto digital)
    if intent == "shipping":
        await zapi_send_text(
            phone,
            "Nosso produto é **100% digital (PDF/e-book)** — não existe frete, rastreio ou envio físico.\n"
            "Assim que o pagamento é aprovado, você recebe o **link de download** no e-mail cadastrado "
            "e, se preferir, posso enviar por aqui também. Quer que eu envie por aqui?"
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
        headline, detail = build_offer(product_name or "seu produto")
        extra = "\n*Validade:* promoções podem ser por tempo limitado." if intent == "deadline" else ""
        await zapi_send_text(phone, f"{headline}\n{detail}{extra}\nQuer que eu gere o link com a condição?")
        ctx["asked"] = "apply_offer"
        store_ctx(phone, ctx)
        return {"ok": True}

    # link direto / intenção de compra — enviar site (pedido do cliente)
    if intent == "link":
        await zapi_send_text(
            phone,
            f"Perfeito, {name}. Você pode comprar pelo nosso site: {SITE_URL}\n"
            "Você pode conferir no nosso site e qualquer dúvida que você tiver, você pode me falar. 😉"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    # dúvidas gerais
    if intent == "product_info":
        await zapi_send_text(
            phone,
            f"O *{product_name or 'produto'}* é um material digital (PDF) com conteúdo prático para aplicar hoje mesmo. "
            f"Se quiser, te envio um resumo e você pode conferir também no site: {SITE_URL}"
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
            headline, detail = build_offer(product_name or "seu produto")
            await zapi_send_text(
                phone,
                f"{headline}\n{detail}\n\n"
                f"Se preferir, você pode comprar direto pelo nosso site: {SITE_URL}\n"
                "Qualquer dúvida, me chama aqui. 😉"
            )
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "apply_offer":
            await zapi_send_text(
                phone,
                f"Perfeito! Você pode finalizar pelo nosso site: {SITE_URL}\n"
                "Você pode conferir no nosso site e qualquer dúvida que você tiver, você pode me falar."
            )
            ctx["asked"] = None
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            return {"ok": True}
        if asked == "resend_link":
            await zapi_send_text(
                phone,
                f"Aqui está o nosso site para acessar o conteúdo: {SITE_URL}\n"
                "Se preferir, posso te acompanhar por aqui até dar tudo certo. 🙂"
            )
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
        msg = f"Detectei um PIX pendente de *{product_name or 'seu pedido'}*. Quer que eu reenvie o QR/link agora?"
    else:
        msg = (
            f"Posso te ajudar a concluir *{product_name or 'seu pedido'}*. "
            f"Se quiser, já pode conferir e comprar pelo nosso site: {SITE_URL}\n"
            "Ou me diga qual dúvida você tem. 🙂"
        )
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

    # se o cliente nomear o produto em qualquer fase, priorize isso
pk_from_text = detect_product_key(text or "")
if pk_from_text:
    ctx["selected_product"] = pk_from_text
    if pk_from_text == "tabib":
        ctx["stage"] = "tabib_menu"
        ctx["tabib_bundle"] = False
        store_ctx(phone, ctx)
        await zapi_send_text(
            phone,
            "Perfeito! Para o Tabib, prefere:\n1) **Todos** os e-books por 19,90\n2) Ver **unitários**\n3) Voltar"
        )
        return {"ok": True}
    else:
        headline, detail = build_offer_ext(pk_from_text, bundle=False)
        set_checkout_stage(ctx)
        store_ctx(phone, ctx)
        await zapi_send_text(
            phone,
            f"{headline}\n{detail}\n\n"
            f"Para finalizar com segurança, acesse nosso site: {SITE_URL}\n"
            "Qualquer dúvida que você tiver, você pode me falar."
        )
        return {"ok": True}

    # ===== CHECKOUT =====
    if stage == "checkout":
        if offer_expired(ctx):
            await zapi_send_text(phone, "A condição anterior expirou. Posso **renovar** a oferta e te mandar o link atualizado?")
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        return await handle_intent(phone, ctx, text, detect_intent(text))

    # ===== VERIFY =====
    if stage == "verify":
        if is_option(tlow, "1", "sim", "sou eu", "isso mesmo", "eu"):
            ctx["confirmed_owner"] = True
            ctx["stage"] = "pick_product"
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                "Legal! Qual produto você quer?\n"
                "• Se for *Tabib*, posso te mostrar as 5 opções (volumes 1–4 + pacote 2025/2024)."
            )
            return {"ok": True}

        if is_option(tlow, "2", "não", "nao", "não sou", "nao sou"):
            await zapi_send_text(phone, "Sem problemas, obrigado! Se precisar, é só chamar. 🙏")
            clear_ctx(phone)
            return {"ok": True}

        return await handle_intent(phone, ctx, text, detect_intent(text))

    # ===== PICK_PRODUCT =====
    if stage == "pick_product":
        # Se o cliente falar "tabib", abre menu
        if "tabib" in tlow:
            ctx["stage"] = "tabib_menu"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, menu_tabib_text())
            return {"ok": True}

        # tenta identificar produto pelo catálogo
        items = find_by_text(text)
        if items:
            it = items[0]
            headline, detail = build_offer(it.get("name",""))
            link = it.get("checkout") or ctx.get("checkout_url","")
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                f"{headline}\n{detail}\n\n"
                f"Link para concluir: {link}\n"
                f"Site oficial: {SITE_URL}\n"
                "Quer ajuda para finalizar?"
            )
            return {"ok": True}

        await zapi_send_text(
            phone,
            "Não entendi o produto. Se for *Tabib*, responda 'tabib' que eu abro as opções pra você.\n"
            "Ou me diga palavras-chave (ex.: airfryer, antídoto, kurimã, bálsamo, pressão alta)."
        )
        return {"ok": True}

    # ===== TABIB_MENU =====
    if stage == "tabib_menu":
        # número direto 1–5
        if re.fullmatch(r"[1-5]", tlow):
            it = find_tabib_choice_by_number(tlow)
            if not it:
                await zapi_send_text(phone, "Opção indisponível no momento. Pode escolher outra (1–5)?")
                return {"ok": True}
            headline, detail = build_offer(it.get("name","Tabib"))
            link = it.get("checkout") or ctx.get("checkout_url","")
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                f"{headline}\n{detail}\n\n"
                f"Link para concluir: {link}\n"
                f"Você pode conferir no nosso site: {SITE_URL}\n"
                "Qualquer dúvida, me chama aqui. 🙂"
            )
            return {"ok": True}

        # palavras tipo "v3", "volume 2", etc.
        items = find_by_text(text)
        if items:
            it = items[0]
            headline, detail = build_offer(it.get("name","Tabib"))
            link = it.get("checkout") or ctx.get("checkout_url","")
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                f"{headline}\n{detail}\n\n"
                f"Link para concluir: {link}\n"
                f"Você pode conferir no nosso site: {SITE_URL}"
            )
            return {"ok": True}

        await zapi_send_text(phone, "Não peguei sua escolha. Responda 1–5 ou diga o volume desejado (ex.: v3, volume 3).")
        return {"ok": True}

    # fallback
    return await handle_intent(phone, ctx, text, detect_intent(text))

# -------------------- Z-API inbound webhook (mensagens recebidas) --------------------
@app.post("/webhook/zapi")
async def zapi_webhook(
    request: Request,
    x_zapi_secret: Optional[str] = Header(None),
):
    # Durante testes, não falhe com 401
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
            ctx["stage"] = "pick_product"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, "Ótimo! Qual produto você quer? Se for *Tabib*, posso listar as opções. 🙂")
            return {"ok": True}
        else:
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
