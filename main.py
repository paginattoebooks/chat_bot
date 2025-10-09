"""
Paginatto — Bot de WhatsApp
Fluxos: Carrinho Abandonado e PIX Pendente
Stack: FastAPI + Z-API + (Redis opcional) + Postgres (Supabase)
"""

# -------------------- Imports --------------------
import os
import re
import json
import logging
import socket
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import quote_plus

import httpx
from unidecode import unidecode
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool

from webhook import router as webhook_router
from offer_rules import build_offer

# -------------------- Bootstrap (.env, log) --------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("paginatto")

# -------------------- App --------------------
app = FastAPI(title="Paginatto Bot", version="2.3.0")

# -------------------- Marca/assistente --------------------
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "Iara").strip()
BRAND_NAME     = os.getenv("BRAND_NAME", "Paginatto").strip()

SITE_URL   = os.getenv("SITE_URL", "https://paginattoebooks.github.io/Paginatto.site.com.br/").strip()
LEGAL_NAME = os.getenv("LEGAL_NAME", "PAGINATTO").strip()
LEGAL_CNPJ = os.getenv("LEGAL_CNPJ", "57.941.903/0001-94").strip()

# -------------------- Z-API --------------------
ZAPI_INSTANCE = (os.getenv("ZAPI_INSTANCE") or "").strip()
ZAPI_TOKEN    = (os.getenv("ZAPI_TOKEN") or "").strip()
CLIENT_TOKEN  = (os.getenv("ZAPI_CLIENT_TOKEN") or os.getenv("ZAPI_TOKEN") or "").strip()
ZAPI_BASE     = (os.getenv("ZAPI_BASE") or "").strip().rstrip("/")

if ZAPI_BASE:
    ZAPI_MSG_URL = ZAPI_BASE
elif ZAPI_INSTANCE and ZAPI_TOKEN:
    ZAPI_MSG_URL = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-text"
else:
    ZAPI_MSG_URL = ""

# -------------------- Webhook secrets (opcionais) --------------------
CARTPANDA_WEBHOOK_SECRET = (os.getenv("CARTPANDA_WEBHOOK_SECRET") or "").strip()
ZAPI_WEBHOOK_SECRET      = (os.getenv("ZAPI_WEBHOOK_SECRET") or "").strip()

# -------------------- Ofertas --------------------
OFFER_TTL_MIN = int(os.getenv("OFFER_TTL_MIN", "60"))

# -------------------- Redis (opcional) --------------------
REDIS_URL = (os.getenv("REDIS_URL") or "").strip()
try:
    import redis  # type: ignore
    rds = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
except Exception:
    rds = None


def force_ipv4_in_dsn(url: str) -> str:
    u = urlparse(url)
    try:
        infos = socket.getaddrinfo(u.hostname, None, 0, 0, 0, 0)  # aceita qualquer família
    except Exception:
        # Não conseguiu resolver hostname agora → segue sem forçar IPv4
        return url

    ipv4 = None
    for fam, *_rest, sockaddr in infos:
        if fam == socket.AF_INET:
            ipv4 = sockaddr[0]
            break
    if not ipv4:
        # Não há A record disponível → segue sem forçar IPv4
        return url

    # Injeta hostaddr mantendo host para SNI/TLS
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q["hostaddr"] = ipv4
    new = u._replace(query=urlencode(q))
    return urlunparse(new)

# -------------------- DSN helpers --------------------
def build_dsn() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL não configurada")
    safe = re.sub(r":([^:@/]+)@", r":********@", url)
    log.info("Usando DSN: %s", safe)
    return url

def create_pool() -> ConnectionPool:
    dsn = build_dsn()
    return ConnectionPool(
        dsn,
        min_size=1,
        max_size=2,
        kwargs={
            "connect_timeout": 5,
            "prepare_threshold": 0,   # chave para PgBouncer
        },
    )
def _startup():
    app.state.pool = create_pool()
    with app.state.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            log.info("DB ok: %s", cur.fetchone())

# -------------------- Banco (pool no app.state) --------------------
# NÃO crie o pool aqui. Crie no startup com create_pool().

@app.on_event("startup")
def _startup():
    app.state.pool = create_pool()
    log.info("Pool de conexões criado.")

@app.on_event("shutdown")
def _shutdown():
    try:
        app.state.pool.close()
        log.info("Pool de conexões fechado.")
    except Exception:
        pass

@app.get("/ping")
def ping(request: Request):
    pool: ConnectionPool = request.app.state.pool
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    return {"ok": True}

# -------------------- Saúde/diagnóstico --------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "brand": BRAND_NAME,
        "assistant": ASSISTANT_NAME,
        "zapi_configured": bool(ZAPI_MSG_URL and CLIENT_TOKEN),
        "redis": bool(rds),
    }

# -------------------- Exceções globais --------------------
@app.exception_handler(Exception)
async def on_any_error(_, exc: Exception):
    log.exception("Erro não tratado")
    return JSONResponse(status_code=500, content={"ok": False, "error": "internal_error"})

# -------------------- Estado (Redis/memória) --------------------
_MEM: Dict[str, Tuple[datetime, str]] = {}

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

# -------------------- Catálogo --------------------
CATALOG_PATH  = os.getenv("CATALOG_PATH", "catalog.json").strip()
CATALOG_JSON_ENV = os.getenv("CATALOG_JSON", "").strip()

Catalog: List[Dict[str, Any]] = []
CatalogBySKU: Dict[str, Dict[str, Any]] = {}
AliasIndex: Dict[str, str] = {}
FamilyIndex: Dict[str, List[str]] = {}

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
    if CATALOG_JSON_ENV:
        try:
            return json.loads(CATALOG_JSON_ENV)
        except Exception as e:
            log.warning("Falha lendo CATALOG_JSON do .env: %s", e)
    try:
        if os.path.exists(CATALOG_PATH):
            with open(CATALOG_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception as e:
        log.warning("Falha lendo %s: %s", CATALOG_PATH, e)
    # fallback mínimo
    return [
      {
        "sku": "TABIB_V1",
        "family": "tabib",
        "type": "volume",
        "name": "Tabib Volume 1: Tratamento de Dores e Inflamações",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-1.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919679:1",
        "description": "Guia para combater dores de cabeça, musculares e articulares, além de inflamações crônicas.",
        "aliases": ["tabib 1","tabib1","v1","volume 1","inflamacoes","inflamações","tratamento de dores","dores","dor","inflamação","inflamacao"],
      },
      {
        "sku": "TABIB_V2",
        "family": "tabib",
        "type": "volume",
        "name": "Tabib Volume 2: Saúde Respiratória e Imunidade",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-2.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919682:1",
        "description": "Focado na saúde respiratória e fortalecimento da imunidade.",
        "aliases": ["tabib 2","tabib2","v2","vol 2","volume 2","respiratoria","respiratória","pulmão","pulmao","imunidade","gripe","resfriado"],
      },
      {
        "sku": "TABIB_V3",
        "family": "tabib",
        "type": "volume",
        "name": "Tabib Volume 3: Saúde Digestiva e Metabólica",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-3.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919686:1",
        "description": "Receitas para equilíbrio digestivo e regulação do metabolismo.",
        "aliases": ["tabib 3","tabib3","v3","vol 3","volume 3","digestiva","metabólica","metabolica","intestino","refluxo","azia"],
      },
      {
        "sku": "TABIB_V4",
        "family": "tabib",
        "type": "volume",
        "name": "Tabib Volume 4: Saúde Mental e Energética",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-4.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919701:1",
        "description": "Bem-estar emocional, redução do estresse e aumento da energia.",
        "aliases": ["tabib 4","tabib4","v4","vol 4","volume 4","saude mental","saúde mental","energia","estresse","ansiedade","sono"],
      },
      {
        "sku": "TABIB_FULL",
        "family": "tabib",
        "type": "bundle",
        "name": "Tabib completo",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-completo.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229277:1",
        "description": "Coletânea com todos os volumes Tabib.",
        "aliases": ["tabib completo","colecao tabib","coleção tabib","bundle tabib","todos os volumes"],
      },
      {
        "sku": "TABIB_24_25_BUNDLE",
        "family": "tabib",
        "type": "bundle",
        "name": "Tabib 2025 + Bônus 19,90 + Tabib 2024",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/tabib-2025-bonus-2024.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/184229263:1",
        "description": "Pacote com as edições 2025 e 2024 (mais de 960 receitas).",
        "aliases": ["tabib 2025","tabib 2024","tabib pacote","tabib combo","tabib 960 receitas","tabib todos por 19,90","tabib 19,90"],
      },
      {
        "sku": "ANTIDOTO",
        "family": "antidoto",
        "type": "standalone",
        "name": "Antídoto - Antídotos indígenas",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/antidoto.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919637:1",
        "description": "Receitas inspiradas em saberes indígenas para antídotos naturais.",
        "aliases": ["antidoto","antídoto","o livro antidoto","antidotos indigenas","antídotos indígenas"],
      },
      {
        "sku": "KURIMA",
        "family": "kurima",
        "type": "standalone",
        "name": "Kurimã - Óleos essenciais",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/kurima.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919661:1",
        "description": "Guia prático de óleos essenciais com receitas e usos seguros.",
        "aliases": ["kurima","oleos essenciais","óleos essenciais","oleo essencial"],
      },
      {
        "sku": "BALSAMO",
        "family": "balsamo",
        "type": "standalone",
        "name": "Bálsamo - Pomadas naturais",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/balsamo.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/166919668:1",
        "description": "Fórmulas de pomadas naturais para dores, feridas e inflamações.",
        "aliases": ["balsamo","pomadas naturais","pomada natural"],
      },
      {
        "sku": "PRESSAO_ALTA_PLAN",
        "family": "pressao",
        "type": "standalone",
        "name": "Tratamento Natural Personalizado para Pressão Alta",
        "image": "https://paginattoebooks.github.io/Paginatto.site.com.br/img/pressao-alta.png",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/174502432:1",
        "description": "Plano individualizado com alimentação, ervas e exercícios.",
        "aliases": ["pressao alta","hipertensao","tratamento personalizado","hipertensão"],
      },
      {
        "sku": "AIRFRYER_PREMIUM",
        "family": "airfryer",
        "type": "standalone",
        "name": "Airfryer do Chef PREMIUM",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/198180560:1",
        "description": "Este guia reúne receitas rápidas e variadas — de lanches a refeições completas — todas pensadas para reduzir calorias sem abrir mão do sabor",
        "aliases": ["airfryer","receitas airfryer","300 receitas","300 receitas para airfryer"],
      },
      {
        "sku": "DOCES_SEM_ACUCAR",
        "family": "airfryer",
        "type": "standalone",
        "name": "Doces sem açúcar - MAIS DE 110 RECEITAS",
        "checkout": "https://somasoundsolutions.mycartpanda.com/checkout/198181424:1",
        "description": "Prepare bolos, mousses, tortas e guloseimas usando adoçantes naturais e combinações equilibradas.",
        "aliases": ["airfryer","receitas airfryer","Doces sem açúcar","300 receitas para airfryer","doces","sem açúcar"],
      },
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

        aliases = list(set((it.get("aliases") or []) + [name]))
        if it["family"] == "tabib":
            m = re.search(r"volume\s*(\d+)", name.lower())
            if m:
                v = m.group(1)
                aliases += [f"v{v}", f"volume {v}", f"tabib {v}"]
        for a in aliases:
            for tok in _safe_tokens(a):
                AliasIndex[tok] = sku

        FamilyIndex.setdefault(it["family"], []).append(sku)

    log.info("Catálogo carregado: %d itens; famílias: %s", len(Catalog), list(FamilyIndex.keys()))

_index_catalog()

# ---------- Descrição ----------
def get_full_desc(it: Dict[str, Any]) -> str:
    for k in ("full_description","descricao_completa","descrição_completa","descricao","descrição","description","desc"):
        v = it.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def describe_item(it: Dict[str, Any]) -> str:
    name = it.get("name","")
    desc = get_full_desc(it)
    if desc:
        return f"*{name}*\n{desc}"
    return f"*{name}*"

# ---------- Busca ----------
def find_by_text(text: str) -> List[Dict[str, Any]]:
    toks = _safe_tokens(text)
    hits = []
    for t in toks:
        sku = AliasIndex.get(t)
        if sku and sku in CatalogBySKU:
            hits.append(CatalogBySKU[sku])
    if not hits and toks:
        for it in Catalog:
            nm = (it.get("name") or "").lower()
            if all(tok in nm for tok in toks):
                hits.append(it)
    seen, out = set(), []
    for it in hits:
        s = it["sku"]
        if s not in seen:
            out.append(it); seen.add(s)
    return out

def menu_tabib_text() -> str:
    vols = []
    for v in ["TABIB_V1","TABIB_V2","TABIB_V3","TABIB_V4"]:
        if v in CatalogBySKU:
            vols.append(CatalogBySKU[v]["name"])
    opt5 = "TABIB_24_25_BUNDLE" if "TABIB_24_25_BUNDLE" in CatalogBySKU else ("TABIB_FULL" if "TABIB_FULL" in CatalogBySKU else None)
    lines = [f"{i}) {nm}" for i, nm in enumerate(vols, start=1)]
    if opt5:
        lines.append(f"5) {CatalogBySKU[opt5]['name']}")
    header = "Temos estas opções do *Tabib*: \n"
    footer = "\nResponda com o número (1–5) ou diga, por ex., 'v3' / 'volume 3'."
    return header + "\n".join(lines) + footer

def tabib_unitarios_list_text() -> str:
    vols, idx = [], 1
    for sku in ["TABIB_V1", "TABIB_V2", "TABIB_V3", "TABIB_V4"]:
        if sku in CatalogBySKU:
            vols.append(f"{idx}) {CatalogBySKU[sku]['name']}")
            idx += 1
    if not vols:
        return "Ainda não tenho os volumes unitários cadastrados."
    txt = "Esses são os *volumes unitários* do Tabib:\n" + "\n".join(vols)
    txt += "\n\nResponda com o número (ex.: 2) ou com o volume (ex.: v2)."
    return txt

def pick_family_item(fam: str) -> Optional[Dict[str, Any]]:
    fam = (fam or "").lower()
    skus = FamilyIndex.get(fam) or []
    return CatalogBySKU.get(skus[0]) if skus else None

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
        5: "TABIB_24_25_BUNDLE" if "TABIB_24_25_BUNDLE" in CatalogBySKU else "TABIB_FULL",
    }
    sku = mapping.get(i)
    if sku and sku in CatalogBySKU:
        return CatalogBySKU[sku]
    return None

# -------------------- Helpers (produto/oferta) --------------------
def detect_product_key(text: str) -> Optional[str]:
    t = (text or "").lower()
    if "tabib" in t or re.search(r"\bv[1-4]\b|\bvolume\s*[1-4]\b", t):
        return "tabib"
    if "airfryer" in t or "fritadeira" in t:
        return "airfryer"
    if "masterchef" in t or "master chef" in t:
        return "masterchef"
    return None

def build_offer_ext(product_or_key: str, *, bundle: bool = False) -> Tuple[str, str]:
    name_map = {"tabib": "Tabib", "airfryer": "300 receitas para AirFryer", "masterchef": "MasterChef"}
    base_name = name_map.get(str(product_or_key).lower(), product_or_key)
    headline, detail = build_offer(base_name)
    if str(product_or_key).lower() == "tabib" and bundle:
        detail = f"Todos os e-books do Tabib de R$ 49,90 por **R$ 19,90** hoje.\n{detail}"
    return headline, detail

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
TRUST_PATTERNS = [
    r"\bgolpe(s)?\b", r"\bfraude(s)?\b", r"\bscam\b", r"\bfake\b",
    r"\bseguran[çc]a\b", r"\bsegur[ao]\b", r"\bconfi[aá]vel\b", r"\bconfian[çc]a\b"
]
INTENTS: Dict[str, List[str]] = {
    "greeting": [r"\b(oi|ola|eai|boa noite|boa tarde|bom dia)\b"],
    "thanks":   [r"\bobrigado?\b|\bvaleu\b"],
    "stop":     [r"\bstop\b|\bcancel(ar|e)\b|\bpare\b|\bdescadastrar\b|\bnao quero\b"],
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
_RE_URL        = re.compile(r"https?://\S+|www\.\S+", re.I)
_RE_MULTICHAR  = re.compile(r"(.)\1{2,}")
_RE_NON_ALNUM  = re.compile(r"[^a-z0-9]+", re.I)
_SYNONYMS = (
    (r"\bobg\b|\bvlw\b|\bvaleu\b", " obrigado "),
    (r"\bpfv\b|\bpls\b|\bpor favor\b", " por favor "),
    (r"\bq(?:l|ual)\b|\bqual\b", " qual "),
    (r"\bnao\b|\bn\b", " nao "),
    (r"\bsim\b|\byes\b", " sim "),
    (r"\blink\b|\bcheckout\b|\bcarrinho\b", " link "),
)

def normalize_text(s: str) -> str:
    s = unidecode(str(s or "").lower())
    s = _RE_URL.sub(" ", s)
    for pat, repl in _SYNONYMS:
        s = re.sub(pat, repl, s)
    s = _RE_MULTICHAR.sub(r"\1\1", s)
    s = _RE_NON_ALNUM.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def matches(text: str, patterns: List[str]) -> bool:
    t = normalize_text(text)
    return any(re.search(p, t) for p in patterns)

def detect_intent(text: str) -> str:
    t = normalize_text(text)
    for intent, patterns in INTENTS.items():
        if any(re.search(p, t) for p in patterns):
            return intent
    if re.search(r"\b(sim|1)\b", t):
        return "yes"
    if re.search(r"\b(nao|2)\b", t):
        return "no"
    if re.search(r"\b(comprei|paguei|finalizei|pagamento aprovado)\b", t):
        return "bought"
    if re.search(r"\b(desisti|nao vou|depois)\b", t):
        return "quit"
    return "unknown"

# -------------------- Normalizador Z-API --------------------
def extract_phone_and_text(body: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
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

def is_audio_or_call(body: Dict[str, Any]) -> bool:
    def _typ(d: Dict[str, Any]) -> str:
        return (d.get("type") or d.get("messageType") or "").lower()
    if _typ(body) in {"audio", "voice", "ptt", "call", "call_log"}:
        return True
    m = body.get("message") or {}
    if isinstance(m, dict) and (_typ(m) in {"audio", "voice", "ptt", "call"}):
        return True
    if isinstance(m, dict) and any(k in m for k in ("audio", "voice", "ptt")):
        return True
    return False

# -------------------- Webhook CartPanda --------------------
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
    fam_guess = _infer_family({"name": product_name})

    ctx: Dict[str, Any] = {
        "flow": flow,
        "name": name,
        "product_name": product_name,
        "product_key": fam_guess,
        "selected_product": fam_guess or "",
        "checkout_url": checkout_url,
        "created_at": now_utc().isoformat(),
        "stage": "verify",
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

# -------------------- Intent handler --------------------
async def handle_intent(phone: str, ctx: Dict[str, Any], text: str, intent: str) -> Dict[str, Any]:
    name           = ctx.get("name") or ""
    product_name   = ctx.get("product_name") or ""
    checkout_url   = ctx.get("checkout_url") or ""
    flow           = ctx.get("flow") or "unknown"
    stage          = ctx.get("stage") or "verify"

    items = find_by_text(text)
    if items:
        it = items[0]
        if it.get("family") == "tabib" and it.get("sku","").startswith("TABIB_V"):
            msg = describe_item(it) + "\n\nQuer ver *todos* por 19,90 ou *unitários*?"
            await zapi_send_text(phone, msg)
            ctx["stage"] = "tabib_menu"
            ctx["tabib_bundle"] = False
            ctx["asked"] = "tabib_after_desc"
            ctx["last_item_sku"] = it.get("sku")
            store_ctx(phone, ctx)
            return {"ok": True}

        headline, detail = build_offer(it.get("name",""))
        link = it.get("checkout") or ctx.get("checkout_url","")
        set_checkout_stage(ctx)
        store_ctx(phone, ctx)
        await zapi_send_text(phone, f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}")
        return {"ok": True}

    pk_from_text = detect_product_key(text or "")
    if pk_from_text:
        ctx["selected_product"] = pk_from_text
        ctx["product_key"] = pk_from_text

        if pk_from_text == "tabib":
            ctx["stage"] = "tabib_menu"
            ctx["tabib_bundle"] = False
            ctx["asked"] = "tabib_main"
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                "Perfeito! Para o Tabib, prefere:\n"
                "1) **Todos** os e-books por 19,90\n"
                "2) Ver **unitários**\n"
                "3) Voltar"
            )
            return {"ok": True}
        else:
            it = pick_family_item(pk_from_text)
            if it:
                headline, detail = build_offer(it.get("name",""))
                link = it.get("checkout") or ctx.get("checkout_url","")
                set_checkout_stage(ctx)
                store_ctx(phone, ctx)
                await zapi_send_text(phone, f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}")
            else:
                headline, detail = build_offer_ext(pk_from_text, bundle=False)
                await zapi_send_text(phone, f"{headline}\n{detail}\n\nAcesse: {SITE_URL}")
            return {"ok": True}

    if intent in ("stop", "quit"):
        await zapi_send_text(phone, "Entendido. Vou encerrar por aqui. 🙏 Se mudar de ideia, é só chamar. Boa semana!")
        clear_ctx(phone)
        return {"ok": True}

    if intent == "bought":
        clear_ctx(phone)
        await zapi_send_text(phone, "Obrigada pela compra! 🎉 Qualquer dúvida, estamos à disposição por aqui.")
        return {"ok": True}

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

    if intent == "payment":
        await zapi_send_text(
            phone,
            "Aceitamos **PIX** e **cartão** (com parcelamento). Por ser digital, a liberação é **imediata** após aprovação. "
            "Prefere pagar via PIX ou cartão?"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    if intent in ("price", "discount", "deadline"):
        headline, detail = build_offer(product_name or "seu produto")
        extra = "\n*Validade:* promoções podem ser por tempo limitado." if intent == "deadline" else ""
        await zapi_send_text(phone, f"{headline}\n{detail}{extra}\nQuer que eu gere o link com a condição?")
        ctx["asked"] = "apply_offer"
        store_ctx(phone, ctx)
        return {"ok": True}

    if intent == "link":
        await zapi_send_text(
            phone,
            f"Perfeito, {name}. Você pode comprar pelo nosso site: {SITE_URL}\n"
            "Você pode conferir no nosso site e qualquer dúvida que você tiver, você pode me falar. 😉"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

    if intent == "product_info":
        await zapi_send_text(
            phone,
            f"O *{product_name or 'produto'}* é um material digital (PDF) com conteúdo prático para aplicar hoje mesmo. "
            f"Se quiser, te envio um resumo e você pode conferir também no site: {SITE_URL}"
        )
        store_ctx(phone, ctx)
        return {"ok": True}

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

    if intent == "unknown":
        await zapi_send_text(phone, "Não entendi, pode me dizer novamente?")
        store_ctx(phone, ctx)
        return {"ok": True}

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
    tlow = (text or "").strip().lower()

    pk_from_text = detect_product_key(text or "")
    if pk_from_text:
        ctx["selected_product"] = pk_from_text
        ctx["product_key"] = pk_from_text

        if pk_from_text == "tabib":
            ctx["stage"] = "tabib_menu"
            ctx["tabib_bundle"] = False
            ctx["asked"] = "tabib_main"
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                "Perfeito! Para o Tabib, prefere:\n"
                "1) **Todos** os e-books por 19,90\n"
                "2) Ver **unitários**\n"
                "3) Voltar"
            )
            return {"ok": True}
        else:
            it = pick_family_item(pk_from_text)
            if it:
                headline, detail = build_offer(it.get("name", ""))
                link = it.get("checkout") or ctx.get("checkout_url", "")
                set_checkout_stage(ctx)
                store_ctx(phone, ctx)
                await zapi_send_text(
                    phone,
                    f"{describe_item(it)}\n\n{headline}\n{detail}\n\n"
                    f"Para finalizar com segurança, acesse: {link}\n"
                    "Qualquer dúvida que você tiver, você pode me falar."
                )
                return {"ok": True}

    if stage == "checkout":
        if offer_expired(ctx):
            await zapi_send_text(
                phone,
                "A condição anterior expirou. Posso **renovar** a oferta e te mandar o link atualizado?"
            )
            ctx["asked"] = "apply_offer"
            store_ctx(phone, ctx)
            return {"ok": True}
        return await handle_intent(phone, ctx, text, detect_intent(text))

    if stage == "verify":
        if is_option(tlow, "1", "sim", "sou eu", "isso mesmo", "eu"):
            ctx["confirmed_owner"] = True
            ctx["stage"] = "pick_product"
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                "Legal! Qual produto você quer?\n"
                "• Se for *Tabib*, posso te mostrar as opções (volumes + pacote)."
            )
            return {"ok": True}

        if is_option(tlow, "2", "não", "nao", "não sou", "nao sou"):
            await zapi_send_text(phone, "Sem problemas, obrigado! Se precisar, é só chamar. 🙏")
            clear_ctx(phone)
            return {"ok": True}

        return await handle_intent(phone, ctx, text, detect_intent(text))

    if stage == "pick_product":
        if "tabib" in tlow:
            ctx["stage"] = "tabib_menu"
            store_ctx(phone, ctx)
            await zapi_send_text(phone, menu_tabib_text())
            return {"ok": True}

        items = find_by_text(text)
        if items:
            it = items[0]
            headline, detail = build_offer(it.get("name", ""))
            link = it.get("checkout") or ctx.get("checkout_url", "")
            set_checkout_stage(ctx)
            store_ctx(phone, ctx)
            await zapi_send_text(
                phone,
                f"{describe_item(it)}\n\n{headline}\n{detail}\n\n"
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

    if stage == "tabib_menu":
        asked = ctx.get("asked") or ""

        if asked == "tabib_main":
            if tlow in {"1", "todos", "bundle", "pacote"}:
                it = CatalogBySKU.get("TABIB_24_25_BUNDLE") or CatalogBySKU.get("TABIB_FULL")
                if it:
                    headline, detail = build_offer(it.get("name", "Tabib"))
                    link = it.get("checkout") or ctx.get("checkout_url", "")
                    set_checkout_stage(ctx)
                    ctx["asked"] = None
                    store_ctx(phone, ctx)
                    await zapi_send_text(
                        phone,
                        f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}"
                    )
                    return {"ok": True}

            if tlow in {"2", "unitario", "unitarios", "unitários"}:
                ctx["asked"] = "tabib_pick_unit"
                store_ctx(phone, ctx)
                await zapi_send_text(phone, tabib_unitarios_list_text())
                return {"ok": True}

            if tlow in {"3", "voltar", "back"}:
                ctx["stage"] = "pick_product"
                ctx["asked"] = None
                store_ctx(phone, ctx)
                await zapi_send_text(phone, "Ok! Qual produto você quer ver agora?")
                return {"ok": True}

        if asked == "tabib_after_desc":
            if detect_intent(text) == "link":
                sku = ctx.get("last_item_sku")
                it = CatalogBySKU.get(sku) if sku else None
                if it:
                    headline, detail = build_offer(it.get("name", "Tabib"))
                    link = it.get("checkout") or ctx.get("checkout_url", "")
                    set_checkout_stage(ctx)
                    store_ctx(phone, ctx)
                    await zapi_send_text(phone, f"{headline}\n{detail}\n\nLink para concluir: {link}")
                    return {"ok": True}

            if re.search(r"\b(todos|bundle|pacote|19[,\.]?90)\b", tlow):
                it = CatalogBySKU.get("TABIB_24_25_BUNDLE") or CatalogBySKU.get("TABIB_FULL")
                if it:
                    headline, detail = build_offer(it.get("name", "Tabib"))
                    link = it.get("checkout") or ctx.get("checkout_url", "")
                    set_checkout_stage(ctx)
                    store_ctx(phone, ctx)
                    await zapi_send_text(
                        phone,
                        f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}"
                    )
                    return {"ok": True}

            if re.search(r"\bunit[aá]ri[oa]s?\b", tlow) or tlow in {"2", "unitario", "unitarios", "unitários"}:
                ctx["asked"] = "tabib_pick_unit"
                store_ctx(phone, ctx)
                await zapi_send_text(phone, tabib_unitarios_list_text())
                return {"ok": True}

            await zapi_send_text(phone, "Não entendi, prefere *todos* por 19,90 ou ver *unitários*?")
            return {"ok": True}

        if asked == "tabib_pick_unit":
            if re.fullmatch(r"[1-4]", tlow):
                it = find_tabib_choice_by_number(tlow)
                if it:
                    headline, detail = build_offer(it.get("name", "Tabib"))
                    link = it.get("checkout") or ctx.get("checkout_url", "")
                    set_checkout_stage(ctx)
                    ctx["asked"] = None
                    store_ctx(phone, ctx)
                    await zapi_send_text(
                        phone,
                        f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}"
                    )
                    return {"ok": True}

            items = find_by_text(text)
            if items and items[0].get("family") == "tabib":
                it = items[0]
                headline, detail = build_offer(it.get("name", "Tabib"))
                link = it.get("checkout") or ctx.get("checkout_url", "")
                set_checkout_stage(ctx)
                ctx["asked"] = None
                store_ctx(phone, ctx)
                await zapi_send_text(
                    phone,
                    f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}"
                )
                return {"ok": True}

            await zapi_send_text(
                phone,
                "Não peguei. Responda com o número (1–4) ou diga o volume, ex.: *v2* / *volume 2*."
            )
            return {"ok": True}

        if re.fullmatch(r"\s*[1-5]\s*", tlow):
            it = find_tabib_choice_by_number(tlow.strip())
            if it:
                headline, detail = build_offer(it.get("name", "Tabib"))
                link = it.get("checkout") or ctx.get("checkout_url", "")
                set_checkout_stage(ctx)
                ctx["asked"] = None
                store_ctx(phone, ctx)
                await zapi_send_text(
                    phone,
                    f"{describe_item(it)}\n\n{headline}\n{detail}\n\nLink para concluir: {link}"
                )
                return {"ok": True}

        await zapi_send_text(
            phone,
            "Não entendi. Responda *1* para todos, *2* para unitários, ou diga o volume (ex.: *v3*)."
        )
        return {"ok": True}

    return await handle_intent(phone, ctx, text, detect_intent(text))

# -------------------- Z-API inbound --------------------
@app.post("/webhook/zapi")
async def zapi_webhook(
    request: Request,
    x_zapi_secret: Optional[str] = Header(None),
):
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

    if is_audio_or_call(body):
        if phone:
            await zapi_send_text(phone, "No momento não conseguimos ouvir áudio e nem atender ligações. Pode escrever por aqui?")
        return {"ok": True}

    if not phone or not text:
        return {"ok": True, "note": "missing phone/text"}

    ctx = read_ctx(phone)

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

    ctx["last_intent"] = detect_intent(text)
    store_ctx(phone, ctx)
    return await route_stage(phone, ctx, text)

@app.post("/webhook/zapi/status")
async def zapi_status():
    return {"ok": True}

# -------------------- Outras rotas (persist/cartpanda) --------------------
app.include_router(webhook_router, prefix="/webhook")
