"""Тянет лиды из amoCRM (воронка «Колл Центр(data leads)») и строит docs/data.json."""
import os, re, json
from collections import defaultdict
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT        = Path(__file__).parent
OUT_PATH    = ROOT / "docs" / "data.json"
MANUAL_PATH = ROOT / "manual_data.json"

AMO_DOMAIN  = os.environ.get("AMO_DOMAIN", "dmitriiostashov.amocrm.ru")
AMO_TOKEN   = os.environ["AMO_TOKEN"]
PIPELINE_ID = 10786530
CALL_REGULATION = int(os.environ.get("CALL_REGULATION", "9"))
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# Исторические даты событий. Текущая стадия сделки для этих метрик не используется:
# стадия меняется, а эти поля сохраняют факт и дату прохождения шага.
FIELD_PCP_DATE = 960167      # Дата (ПЦП)
FIELD_BOOKING_DATE = 960163  # Дата (Запись)
FIELD_ARRIVAL_DATE = 960165  # Дата (Явка)
FIELD_APPOINTMENT_AT = 959989  # Дата записи на приём (запланированное время)

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
    87523562: dict(qual="ПЦП",    status="запись в клинику", yavka=None),   # Отказ после записи → в «Запись активна»
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


def get_call_tasks(lead_ids):
    """Возвращает задачи amoCRM типа «Звонок», привязанные к нужным лидам."""
    tasks = []
    for i in range(0, len(lead_ids), 50):
        chunk = lead_ids[i : i + 50]
        page = 1
        while True:
            params = (
                [("filter[entity_type]", "leads"), ("filter[task_type][]", 1)]
                + [("filter[entity_id][]", lid) for lid in chunk]
                + [("limit", 250), ("page", page)]
            )
            r = requests.get(
                f"https://{AMO_DOMAIN}/api/v4/tasks",
                headers=HEADERS, params=params, timeout=30,
            )
            if r.status_code == 204:
                break
            r.raise_for_status()
            batch = r.json().get("_embedded", {}).get("tasks", [])
            tasks.extend(batch)
            if len(batch) < 250:
                break
            page += 1
    return tasks


def get_custom_field_value(lead, field_id):
    """Возвращает первое значение допполя сделки."""
    for field in lead.get("custom_fields_values") or []:
        if field.get("field_id") != field_id:
            continue
        values = field.get("values") or []
        return values[0].get("value") if values else None
    return None


def amo_date_to_day(value):
    """Приводит Unix-дату amoCRM к календарному дню по Москве."""
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=MOSCOW_TZ).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return None


# ── Analytics ────────────────────────────────────────────────────────────────

def categorize(comment):
    if not isinstance(comment, str):
        return "Другое"
    c = comment.lower()
    for name, pat in CATEGORIES:
        if re.search(pat, c):
            return name
    return "Другое"


def compute(g, manual=None, include_details=True):
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
    calls_total     = int(g["call_tasks"].sum()) if "call_tasks" in g else 0
    calls_completed = int(g["call_completed"].sum()) if "call_completed" in g else 0
    calls_open      = int(g["call_open"].sum()) if "call_open" in g else 0
    leads_no_calls  = int((g["call_tasks"] == 0).sum()) if "call_tasks" in g else total
    leads_over_norm = int((g["call_tasks"] > CALL_REGULATION).sum()) if "call_tasks" in g else 0

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
            "calls": int(og["call_tasks"].sum()) if "call_tasks" in og else 0,
            "calls_completed": int(og["call_completed"].sum()) if "call_completed" in og else 0,
            "calls_open": int(og["call_open"].sum()) if "call_open" in og else 0,
            "leads_no_calls": int((og["call_tasks"] == 0).sum()) if "call_tasks" in og else int(len(og)),
            "leads_over_norm": int((og["call_tasks"] > CALL_REGULATION).sum()) if "call_tasks" in og else 0,
        }

    rg = g[g["Статус:"] == "ОТКАЗ"]
    rt = int(len(rg))
    refusal_cats = []
    if rt:
        vc = rg["refusal_cat"].value_counts()
        for cat, n in vc.items():
            items = (
                rg[rg["refusal_cat"] == cat][
                    ["Имя:", "Статус:", "Имя оператора, взявшего в работу", "Комментарии:"]
                ].to_dict("records")
                if include_details else []
            )
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
            "calls_total": calls_total,
            "calls_completed": calls_completed,
            "calls_open": calls_open,
            "calls_per_lead": round(calls_total / total, 1) if total else 0,
            "leads_no_calls": leads_no_calls,
            "leads_over_norm": leads_over_norm,
            "pcp_no_deposit":     pcp_no_dep,
            "ad_spend":           ad_spend,
        },
        "status_counts": {
            k: int(v) for k, v in
            g["Статус:"].fillna("—").value_counts().items()
        },
        "operator_stats":    op_stats,
        "appointments": [],
        "refusal_categories": refusal_cats,
    }


def compute_calendar_slice(df, start_day, end_day, manual=None, include_details=True):
    """
    Собирает срез по календарным датам событий:
    - заявки, неквалы, отказы и прозвон — по дате создания лида;
    - ПЦП, записи и явки — по своим сохранённым полям дат в amoCRM.

    Так история не «умирает», когда сделка переходит на следующую стадию.
    """
    created = df[(df["day"] >= start_day) & (df["day"] <= end_day)]
    result = compute(created, manual=manual, include_details=include_details)

    def event_group(column):
        values = df[column].fillna("")
        return df[(values >= start_day) & (values <= end_day)]

    pcp_events = event_group("pcp_day")
    booking_events = event_group("booking_day")
    arrival_events = event_group("arrival_day")

    booking_mask = booking_events["Статус:"] == "запись в клинику"
    active_mask = booking_mask & booking_events["Явка:"].isna()
    cancelled_mask = booking_mask & booking_events["Явка:"].isin(OTVAL_YAVKA)

    kpi = result["kpi"]
    kpi.update({
        "pcp": int(len(pcp_events)),
        "zapis": int(len(booking_events)),
        "prishel": int(len(arrival_events)),
        # Эти две метрики — текущий исход записей, созданных внутри периода.
        "otmena": int(cancelled_mask.sum()),
        "active_zapis": int(active_mask.sum()),
        "otval_zapis": int(cancelled_mask.sum()),
    })
    kpi["conv_pcp"] = round(kpi["pcp"] / kpi["total"] * 100, 1) if kpi["total"] else 0
    kpi["conv_zapis_from_pcp"] = round(kpi["zapis"] / kpi["pcp"] * 100, 1) if kpi["pcp"] else 0
    kpi["conv_prishel_from_zapis"] = round(kpi["prishel"] / kpi["zapis"] * 100, 1) if kpi["zapis"] else 0

    # В таблице операторов этапы тоже должны идти по датам событий, а не по текущей стадии.
    def counts_by_operator(group):
        if group.empty:
            return {}
        return {
            (name if pd.notna(name) else "—"): int(count)
            for name, count in group.groupby("Имя оператора, взявшего в работу", dropna=False).size().items()
        }

    pcp_by_op = counts_by_operator(pcp_events)
    booking_by_op = counts_by_operator(booking_events)
    arrival_by_op = counts_by_operator(arrival_events)
    active_by_op = counts_by_operator(booking_events[active_mask])
    cancelled_by_op = counts_by_operator(booking_events[cancelled_mask])
    operators = set(result["operator_stats"]) | set(pcp_by_op) | set(booking_by_op) | set(arrival_by_op)
    empty_operator = {
        "total": 0, "pcp": 0, "nekv": 0, "zapis": 0, "prishel": 0,
        "otval": 0, "active": 0, "calls": 0, "calls_completed": 0,
        "calls_open": 0, "leads_no_calls": 0, "leads_over_norm": 0,
    }
    for operator in operators:
        stats = result["operator_stats"].setdefault(operator, dict(empty_operator))
        stats["pcp"] = pcp_by_op.get(operator, 0)
        stats["zapis"] = booking_by_op.get(operator, 0)
        stats["prishel"] = arrival_by_op.get(operator, 0)
        stats["active"] = active_by_op.get(operator, 0)
        stats["otval"] = cancelled_by_op.get(operator, 0)

    return result


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
        dt  = datetime.fromtimestamp(ts, tz=MOSCOW_TZ) if ts else None
        comment = notes.get(lead["id"], "")
        row = {
            "_lead_id":                           lead["id"],
            "pcp_day":                            amo_date_to_day(get_custom_field_value(lead, FIELD_PCP_DATE)),
            "booking_day":                        amo_date_to_day(get_custom_field_value(lead, FIELD_BOOKING_DATE)),
            "arrival_day":                        amo_date_to_day(get_custom_field_value(lead, FIELD_ARRIVAL_DATE)),
            "appointment_day":                    amo_date_to_day(get_custom_field_value(lead, FIELD_APPOINTMENT_AT)),
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
    df["month"] = df["dt"].dt.strftime("%Y-%m")
    df["day"]   = df["dt"].dt.strftime("%Y-%m-%d")

    call_tasks = get_call_tasks(df["_lead_id"].astype(int).tolist())
    calls_by_lead = defaultdict(lambda: {"total": 0, "completed": 0, "open": 0})
    for task in call_tasks:
        lid = task.get("entity_id")
        if lid is None:
            continue
        calls_by_lead[lid]["total"] += 1
        if task.get("is_completed"):
            calls_by_lead[lid]["completed"] += 1
        else:
            calls_by_lead[lid]["open"] += 1

    df["call_tasks"] = df["_lead_id"].map(lambda lid: calls_by_lead[int(lid)]["total"])
    df["call_completed"] = df["_lead_id"].map(lambda lid: calls_by_lead[int(lid)]["completed"])
    df["call_open"] = df["_lead_id"].map(lambda lid: calls_by_lead[int(lid)]["open"])
    print(f"  Задач «Звонок»: {len(call_tasks)}")

    manual_all: dict = {}
    if MANUAL_PATH.exists():
        with open(MANUAL_PATH, encoding="utf-8") as f:
            manual_all = json.load(f)

    event_day_columns = ["day", "pcp_day", "booking_day", "arrival_day"]
    days = sorted({
        day
        for column in event_day_columns
        for day in df[column].dropna().astype(str)
        if day
    })
    months = sorted({day[:7] for day in days})

    def month_label(m):
        y, mo = m.split("-")
        return f"{MONTH_RU[int(mo)]} {y}"

    total_ad = sum(
        v["ad_spend"] for v in manual_all.values()
        if isinstance(v, dict) and v.get("ad_spend")
    ) or None
    total_manual = {
        "pcp_no_deposit":     sum(v.get("pcp_no_deposit",     0) or 0 for v in manual_all.values() if isinstance(v, dict)),
        "ad_spend":           total_ad,
    }

    call_daily = defaultdict(lambda: {
        "total": 0, "completed": 0, "open": 0, "operator_stats": defaultdict(int),
    })
    for task in call_tasks:
        is_completed = bool(task.get("is_completed"))
        ts = task.get("updated_at") if is_completed else task.get("complete_till")
        if not ts:
            continue
        day = datetime.fromtimestamp(ts, tz=MOSCOW_TZ).strftime("%Y-%m-%d")
        operator = users.get(task.get("responsible_user_id"), "—")
        call_daily[day]["total"] += 1
        call_daily[day]["completed" if is_completed else "open"] += 1
        call_daily[day]["operator_stats"][operator] += 1

    call_daily = {
        day: {
            **stats,
            "operator_stats": dict(stats["operator_stats"]),
        }
        for day, stats in sorted(call_daily.items())
    }

    period_min = days[0] if days else ""
    period_max = days[-1] if days else ""

    data = {
        "period": {
            "date_min": period_min,
            "date_max": period_max,
        },
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metric_basis": {
            "total": "lead_created_at",
            "pcp": f"lead_custom_field_{FIELD_PCP_DATE}",
            "zapis": f"lead_custom_field_{FIELD_BOOKING_DATE}",
            "prishel": f"lead_custom_field_{FIELD_ARRIVAL_DATE}",
        },
        "metrics_meta": {
            "note": "Заявки считаются по дате создания; ПЦП, записи и явки — по отдельным датам событий amoCRM.",
            "fields": {
                "pcp": {"id": FIELD_PCP_DATE, "label": "Дата (ПЦП)"},
                "zapis": {"id": FIELD_BOOKING_DATE, "label": "Дата (Запись)"},
                "prishel": {"id": FIELD_ARRIVAL_DATE, "label": "Дата (Явка)"},
            },
        },
        "calls_meta": {
            "source": "amo_tasks_type_1",
            "label": "Задачи amoCRM типа «Звонок»",
            "regulation": CALL_REGULATION,
            "note": "Выполненная задача — это попытка звонка, а не подтверждённый разговор.",
        },
        "months": [
            {"key": m, "label": month_label(m), "count": int((df["month"] == m).sum())}
            for m in months
        ],
        "all": compute_calendar_slice(
            df, period_min, period_max, manual=total_manual,
        ),
        "by_month": {
            m: compute_calendar_slice(
                df,
                f"{m}-01",
                f"{m}-{pd.Period(m).days_in_month:02d}",
                manual=manual_all.get(m, {}),
            )
            for m in months
        },
        "by_day": {
            day: compute_calendar_slice(df, day, day, include_details=False)
            for day in days
        },
        "call_daily": call_daily,
    }

    data = clean(data)
    OUT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    k = data["all"]["kpi"]
    print(f"✓ Готово: {k['total']} лидов · {k['zapis']} записей · {k['prishel']} пришли · {k['calls_total']} задач на звонок")
    print(f"  По месяцам: " + ", ".join(f"{m['label']} {m['count']}" for m in data["months"]))
    print(f"  Файл: {OUT_PATH}")


if __name__ == "__main__":
    main()
