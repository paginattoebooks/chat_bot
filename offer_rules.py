from typing import Tuple

def classify_product(name: str) -> str:
    n=(name or "").lower()
    if "tabib" in n: return "tabib"
    if "air" in n or "fryer" in n or "airfryer" in n: return "airfryer"
    if "ouderbunps" in n or "bundle" in n or "combo" in n: return "bundle"
    return "generic"

def build_offer(product_name: str) -> Tuple[str, str]:
    p=classify_product(product_name)
    if p=="tabib":     return ("Cupom TABIB10 liberado","R$10 off hoje. Link válido por 60 min.")
    if p=="airfryer":  return ("Bônus Air Fryer","Ganhe 20 receitas bônus por 60 min.")
    if p=="bundle":    return ("Upgrade para Combo -25%","Aplique agora e garanta 25% off no combo.")
    return ("Condição especial","Desconto relâmpago ativo por 60 min.")
