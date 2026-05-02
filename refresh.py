"""Тянет лиды из amoCRM (воронка «Колл Центр(data leads)») и строит docs/data.json."""
import os, re, json
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timezone
from pathlib import Path

ROOT        = Path(__file__).parent
OUT_PATH    = ROOT / "docs" / "data.json"
MANUAL_PATH = ROOT / "manual_data.json"

AMO_DOMAIN  = os.environ.get("AMO_DOMAIN", "dmitriiostashov.amocrm.ru")
AMO_TOKEN   = os.environ["AMO_TOKEN"]
PIPELINE_ID = 10786530

HEADERS = {"Authorization": f"Bearer {AMO_TOKEN}"}

# ── Маппинг этапов воронки ──────────────────────────────────────────────────
# None = пропустить лид (не включать в статистику)
STAGE_MAP = {
    84926646: None,  # Неразобранное
    84926650: None,  # Входящий звонок/заявка
    85161758: dict(qual=None,     status="в работе",        yavka=None),   # Звонить после...
    84926654: dict(qual=None,     status="недозвон",         yavka=None),
    84926658: dict(qual="ПЦП",    status="думает",           yavka=None),   # пцп: думает
    84940842: dict(qual="НЕКВАЛ", status="неквал",           yavka=None),   # Другой город/инвалид/3 лицо
    84940846: dict(qual="ПЦП",    status="запись в клинику", yavka="Отмена записи"),
    84940850: dict(qual="ПЦП",    status="запись в клинику", yavka=None),   # Записались
    142:      dict(qual="ПЦП",    status="запись в клинику", yavka="Пришел"),  # Пришел на консультацию
    143:      dict(qual=None,      status="ОТКАЗ",            yavka=None),   # Отказ от консультации
}

CATEGORIES = [
    ("Технические/мусор",       r"\bтест\b|номер стоматологии|вычет заявки|проверка кк|марушенков"),
    ("Не оставлял заявку",      r"не оставлял заявку|откуда.*(?:наш|у нас).*номер|не понимает.*откуда"),
    ("Уже лечится",             r"уже.*(?:наш|являются) пациент|лечит(?:ся|ься) (?:в|на|у)|"
                                r"другой (?:клиник|врач)|ушёл к другому|другую клинику|племянница работает"),
    ("Цена / финансы",          r"дорог|нет.*возможност.*финанс|нет денег|кредит.*не хочет|слишком больш|"
                                r"цена.*больш|про вд даже слушать не стал|тратьте время"),
    ("Медицинские причины",     r"не годен.*мед|противопоказ|в силу возраст|не приживаются|пожилая|пенсионер"),
    ("Не актуально / не нужно", r"не актуальн|не интересн|ничего не нужно|не нужны услуг|ничего не надо|"
                                r"больше не беспокои|ничвего не беспокои|все не надо|сказал.*все.*не надо"),
]

OTVAL_YAVKA = {"Отмена записи"}

MONTH_RU = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
             "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]


# ── amoCRM API ──────────────────────────────────────────────────────────────

def amo_get(path, params=None):
    url = f"https://{AMO_DOMAIN}/api/v4/{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def paginate(path, key, base_params=None):
    items, page = [], 1
    while True:
        params = {**(base_params or {}), "limit": 250, "page": page}
        batch  = amo_get(path, params).get("_embedded", {}).get(key, [])
        items.extend(batch)
        if len(batch) < 250:
            break
        page += 1
    return items


def get_notes(lead_ids):
    """Возвращает {lead_id: текст_последнего_примечания}."""
    best = {}
    for i in range(0, len(lead_ids), 50):
        chunk = lead_ids[i : i + 50]
        page  = 1
        while True:
            params = ([("filter[entity_id][]", lid) for lid in chunk]
                      + [("filter[note_type]", "common"), ("limit", 250), ("page", page)])
            r = requests.get(
                f"https://{AMO_DOMAIN}/api/v4/leads/notes",
                headers=HEADERS, params=params, timeout=30,
            )
            if r.status_code != 200:
                break
            notes = r.json().get("_embedded", {}).get("notes", [])
            for note in notes:
                lid = note.get("entity_id")
                if lid not in best or note.get("id", 0) > best[lid].get("id", 0):
                    best[lid] = note
            if len(notes) < 250:
                break
            page += 1
    return {lid: n.get("params", {}).get("text", "") or "" for lid, n in best.items()}


# ── Analytics ────────────────────────────────────────────────────────────────

def categorize(comment):
    if not isinstance(comment, str):
        return "Другое"
    c = comment.lower()
    for name, pat in CATEGORIES:
        if re.search(pat, c):
            return name
    return "Другое"


def compute(g, manual=None):
    manual   = manual or {}
    total    = len(g)
    pcp      = int((g["КВАЛИФИКАЦИЯ"] == "ПЦП").sum())
    nekv     = int((g["КВАЛИФИКАЦИЯ"] == "НЕКВАЛ").sum())
    zapis    = int((g["Статус:"] == "запись в клинику").sum())
    prishel  = int((g["Явка:"] == "Пришел").sum())
    otmena   = int((g["Явка:"] == "Отмена записи").sum())
    zap_mask = g["Статус:"] == "запись в клинику"
    active   = int((zap_mask & g["Явка:"].isna()).sum())
    otval    = int((zap_mask & g["Явка:"].isin(OTVAL_YAVKA)).sum())

    pcp_contract       = manual.get("pcp_contract")       or 0
    pcp_contract_count = manual.get("pcp_contract_count") or 0
    pcp_no_dep         = manual.get("pcp_no_deposit")     or 0
    ad_spend           = manual.get("ad_spend")

    op_stats = {}
    for op, og in g.groupby("Имя оператора, взявшего в работу", dropna=False):
        op_name = op if pd.notna(op) else "—"
        ozap    = og["Статус:"] == "запись в клинику"
        op_stats[op_name] = {
            "total":   int(len(og)),
            "pcp":     int((og["КВАЛИФИКАЦИЯ"] == "ПЦП").sum()),
            "nekv":    int((og["КВАЛИФИКАЦИЯ"] == "НЕКВАЛ").sum()),
            "zapis":   int(ozap.sum()),
            "prishel": int((og["Явка:"] == "Пришел").sum()),
            "otval":   int((ozap & og["Явка:"].isin(OTVAL_YAVKA)).sum()),
            "active":  int((ozap & og["Явка:"].isna()).sum()),
        }

    rg = g[g["Статус:"] == "ОТКАЗ"]
    rt = int(len(rg))
    refusal_cats = []
    if rt:
        vc = rg["refusal_cat"].value_counts()
        for cat, n in vc.items():
            items = rg[rg["refusal_cat"] == cat][
                ["Имя:", "Статус:", "Имя оператора, взявшего в работу", "Комментарии:"]
            ].to_dict("records")
            refusal_cats.append({
                "cat": cat, "count": int(n),
                "share": round(n / rt * 100, 1),
                "items": items,
            })

    return {
        "kpi": {
            "total": total, "pcp": pcp, "nekv": nekv,
            "zapis": zapis, "prishel": prishel, "otmena": otmena,
            "active_zapis": active, "otval_zapis": otval,
            "conv_pcp":                round(pcp    / total * 100, 1) if total else 0,
            "conv_zapis_from_pcp":     round(zapis  / pcp   * 100, 1) if pcp   else 0,
            "conv_prishel_from_zapis": round(prishel/ zapis * 100, 1) if zapis else 0,
            "refusals_total":  int((g["Статус:"] == "ОТКАЗ").sum()),
            "pcp_contract":       pcp_contract,
            "pcp_contract_count": pcp_contract_count,
            "pcp_no_deposit":     pcp_no_dep,
            "ad_spend":           ad_spend,
        },
        "status_counts": {
            k: int(v) for k, v in
            g["Статус:"].fillna("—").value_counts().items()
        },
        "operator_stats":    op_stats,
        "appointments": g[g["Статус:"] == "запись в клинику"][
            ["Имя:", "Явка:", "Имя оператора, взявшего в работу", "Комментарии:"]
        ].to_dict("records"),
        "refusal_categories": refusal_cats,
    }


def clean(o):
    if isinstance(o, dict):  return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, list):  return [clean(v) for v in o]
    if isinstance(o, np.integer):  return int(o)
    if isinstance(o, np.floating): return None if np.isnan(o) else float(o)
    if isinstance(o, float) and pd.isna(o): return None
    return o


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("⤓ Получаю данные из amoCRM…")

    users_raw = amo_get("users", params={"limit": 250})
    users     = {u["id"]: u["name"] for u in users_raw.get("_embedded", {}).get("users", [])}
    print(f"  Пользователей: {len(users)}")

    leads_raw = paginate("leads", "leads", {"filter[pipeline_id]": PIPELINE_ID})
    print(f"  Лидов в воронке: {len(leads_raw)}")

    lead_ids = [l["id"] for l in leads_raw]
    notes    = get_notes(lead_ids)
    print(f"  Примечаний найдено: {len(notes)}")

    rows, skipped = [], 0
    for lead in leads_raw:
        mapping = STAGE_MAP.get(lead.get("status_id"))
        if mapping is None:
            skipped += 1
            continue
        ts  = lead.get("created_at", 0)
        dt  = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
        comment = notes.get(lead["id"], "")
        row = {
            "Имя:":                              lead.get("name", ""),
            "КВАЛИФИКАЦИЯ":                      mapping["qual"],
            "Статус:":                           mapping["status"],
            "Явка:":                             mapping["yavka"],
            "Комментарии:":                      comment,
            "Имя оператора, взявшего в работу":  users.get(lead.get("responsible_user_id"), "—"),
            "Время:":                            dt.strftime("%Y.%m.%d %H:%M:%S") if dt else None,
        }
        row["refusal_cat"] = categorize(comment) if mapping["status"] == "ОТКАЗ" else None
        rows.append(row)

    print(f"  Пропущено: {skipped} | Учитываем: {len(rows)}")

    df         = pd.DataFrame(rows)
    df["dt"]   = pd.to_datetime(df["Время:"], format="%Y.%m.%d %H:%M:%S", errors="coerce")
    df["month"]= df["dt"].dt.strftime("%Y-%m")

    manual_all: dict = {}
    if MANUAL_PATH.exists():
        with open(MANUAL_PATH, encoding="utf-8") as f:
            manual_all = json.load(f)

    months = sorted(df["month"].dropna().unique())

    def month_label(m):
        y, mo = m.split("-")
        return f"{MONTH_RU[int(mo)]} {y}"

    total_ad = sum(
        v["ad_spend"] for v in manual_all.values()
        if isinstance(v, dict) and v.get("ad_spend")
    ) or None
    total_manual = {
        "pcp_contract":       sum(v.get("pcp_contract",       0) or 0 for v in manual_all.values() if isinstance(v, dict)),
        "pcp_contract_count": sum(v.get("pcp_contract_count", 0) or 0 for v in manual_all.values() if isinstance(v, dict)),
        "pcp_no_deposit":     sum(v.get("pcp_no_deposit",     0) or 0 for v in manual_all.values() if isinstance(v, dict)),
        "ad_spend":           total_ad,
    }

    data = {
        "period": {
            "date_min": str(df["dt"].min().date()) if not df["dt"].isna().all() else "",
            "date_max": str(df["dt"].max().date()) if not df["dt"].isna().all() else "",
        },
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "months": [
            {"key": m, "label": month_label(m), "count": int((df["month"] == m).sum())}
            for m in months
        ],
        "all":      compute(df, manual=total_manual),
        "by_month": {m: compute(df[df["month"] == m], manual=manual_all.get(m, {})) for m in months},
    }

    data = clean(data)
    OUT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    k = data["all"]["kpi"]
    print(f"✓ Готово: {k['total']} лидов · {k['zapis']} записей · {k['prishel']} пришли")
    print(f"  По месяцам: " + ", ".join(f"{m['label']} {m['count']}" for m in data["months"]))
    print(f"  Файл: {OUT_PATH}")


if __name__ == "__main__":
    main()
