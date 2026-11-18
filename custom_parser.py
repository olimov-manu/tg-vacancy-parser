"""
Скрейперы напрямую с сайтов компаний-гигантов, у которых нет публичного ATS
API (Apple, Amazon, Netflix, Zendesk, Shopify, Google) — у каждой свой формат,
поэтому у каждой свой fetch_*, а не общий код как в site_parser.py.

Если ожидаемый контейнер в HTML / ключ в JSON вообще не находится — это
сигнал, что сайт поменял разметку или формат API, а не что вакансий просто
нет. В этом случае в бот шлётся отдельное предупреждение
"HTML скелет сайта N поменялся", а не обычная вакансия (process_company).

ВАЖНО: у Apple и Shopify параметр поиска (?search=/?q=) НЕ фильтрует
результаты на уровне обычного HTTP-запроса — проверено: с параметром и без
него отдаётся один и тот же список (фильтрация у них через JS уже в
браузере, после первой загрузки страницы). Поэтому для них парсится общий
список текущих вакансий, а Go-фильтр применяется только к заголовку — сигнал
слабее, чем у остальных, но это лучшее, что можно получить без браузера с JS.
У Amazon/Netflix/Zendesk/Google поисковый запрос реально обрабатывается на
сервере (хотя у Google сам поиск нестрогий, типа "golang" зацепляет и
"Go-To-Market" — поэтому для Google по возможности матчим по полному тексту
вакансии, а не только по заголовку).
"""

import html as html_lib
import json
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import db
import site_parser   # переиспользуем is_match / _strip_html / _safe_truncate

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

CUSTOM_STATE_FILE = Path("seen_custom_jobs.json")
SEND_ON_FIRST_RUN = False   # True — при первом запуске отправить все текущие открытые вакансии компании
ALERT_COOLDOWN_HOURS = 24   # не долбить в бот об одной и той же поломке чаще раза в сутки


class StructureChangedError(Exception):
    """Контейнер/ключ, которого мы ожидали, вообще не нашёлся — вероятно,
    сайт поменял HTML-разметку или формат API, а не просто у них нет вакансий."""


def fetch_amazon():
    r = requests.get(
        "https://www.amazon.jobs/en/search.json",
        params={"base_query": "golang", "result_limit": 50},
        headers=HEADERS, timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "jobs" not in data:
        raise StructureChangedError("нет ключа 'jobs' в ответе amazon.jobs/en/search.json")
    jobs = []
    for j in data["jobs"]:
        job_id = j.get("id")
        title = j.get("title")
        if not job_id or not title or j.get("is_intern"):
            continue
        desc = site_parser._strip_html(j.get("description") or "")
        quals = site_parser._strip_html((j.get("basic_qualifications") or "") + (j.get("preferred_qualifications") or ""))
        text = f"{html_lib.escape(title, quote=False)}\n\n{desc}\n{quals}".strip()
        path = j.get("job_path", "")
        link = f"https://www.amazon.jobs{path}" if path.startswith("/") else path
        jobs.append((str(job_id), text, link, j.get("posted_date")))
    return jobs


def fetch_netflix():
    r = requests.get(
        "https://explore.jobs.netflix.net/api/apply/v2/jobs",
        params={"domain": "netflix.com", "start": 0, "num": 50, "query": "golang"},
        headers=HEADERS, timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "positions" not in data:
        raise StructureChangedError("нет ключа 'positions' в ответе Netflix jobs API")
    jobs = []
    for p in data["positions"]:
        job_id = p.get("id")
        name = p.get("name")
        if not job_id or not name:
            continue
        posted_at = None
        ts = p.get("t_create")
        if ts:
            try:
                posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except (TypeError, ValueError, OSError):
                pass
        jobs.append((str(job_id), html_lib.escape(name, quote=False), p.get("canonicalPositionUrl", ""), posted_at))
    return jobs


def fetch_zendesk():
    r = requests.post(
        "https://zendesk.wd1.myworkdayjobs.com/wday/cxs/zendesk/zendesk/jobs",
        json={"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "golang"},
        headers={**HEADERS, "Content-Type": "application/json"}, timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if "jobPostings" not in data:
        raise StructureChangedError("нет ключа 'jobPostings' в ответе Zendesk/Workday API")
    jobs = []
    for jp in data["jobPostings"]:
        title = jp.get("title")
        path = jp.get("externalPath")
        if not title or not path:
            continue
        link = path if path.startswith("http") else f"https://zendesk.wd1.myworkdayjobs.com/zendesk{path}"
        bullets = jp.get("bulletFields") or []
        job_id = bullets[0] if bullets else path
        jobs.append((str(job_id), html_lib.escape(title, quote=False), link, None))
    return jobs


def fetch_apple():
    r = requests.get("https://jobs.apple.com/en-us/search", headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    container = soup.select_one("ul#search-job-list")
    if container is None:
        raise StructureChangedError("не нашёл ul#search-job-list на jobs.apple.com")
    jobs = []
    for li in container.select("li"):
        a = li.select_one("h3 a")
        if not a:
            continue
        href = a.get("href", "")
        m = re.search(r"/details/(\d+)", href)
        job_id = m.group(1) if m else href
        if not job_id:
            continue
        title = html_lib.escape(a.get_text(strip=True), quote=False)
        link = f"https://jobs.apple.com{href}" if href.startswith("/") else href
        jobs.append((job_id, title, link, None))
    return jobs


def fetch_shopify():
    r = requests.get("https://www.shopify.com/careers", headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select('a[data-component-name="job-row"]')
    if not rows:
        if soup.select_one("[data-component-name]") is None:
            raise StructureChangedError("не нашёл ни одного data-component-name на shopify.com/careers")
        return []   # контейнер жив, просто нет вакансий под текущий дефолтный список
    jobs = []
    for a in rows:
        h3 = a.select_one("h3")
        title = html_lib.escape(h3.get_text(strip=True), quote=False) if h3 else ""
        if not title:
            continue
        href = a.get("href", "")
        job_id = a.get("data-component-extra-job-posting-id") or href
        link = href if href.startswith("http") else f"https://www.shopify.com{href}"
        jobs.append((str(job_id), title, link, None))
    return jobs


_AF_DS1_RE = re.compile(r"AF_initDataCallback\(\{key:\s*'ds:1'.*?data:(\[.*?\])\s*,\s*sideChannel", re.DOTALL)


def fetch_google():
    r = requests.get("https://careers.google.com/jobs/results/", params={"q": "golang"}, headers=HEADERS, timeout=20)
    r.raise_for_status()
    m = _AF_DS1_RE.search(r.text)
    if not m:
        raise StructureChangedError("не нашёл блок AF_initDataCallback key='ds:1' на careers.google.com")
    try:
        entries = json.loads(m.group(1))[0]
    except (json.JSONDecodeError, IndexError, TypeError):
        raise StructureChangedError("не смог распарсить JSON из AF_initDataCallback (формат изменился)")
    jobs = []
    for entry in entries:
        if not isinstance(entry, list) or len(entry) < 5:
            continue
        job_id, title, link = entry[0], entry[1], entry[2]
        if not job_id or not title:
            continue
        resp = entry[3][1] if isinstance(entry[3], list) and len(entry[3]) > 1 else ""
        qual = entry[4][1] if isinstance(entry[4], list) and len(entry[4]) > 1 else ""
        text = f"{html_lib.escape(title, quote=False)}\n\n{site_parser._strip_html(resp)}\n{site_parser._strip_html(qual)}".strip()
        posted_at = None
        if len(entry) > 12 and isinstance(entry[12], list) and entry[12]:
            try:
                posted_at = datetime.fromtimestamp(entry[12][0], tz=timezone.utc).isoformat()
            except (TypeError, ValueError, OSError, IndexError):
                pass
        jobs.append((str(job_id), text, link, posted_at))
    return jobs


CUSTOM_COMPANIES = [
    {"name": "Amazon", "fetch": fetch_amazon},
    {"name": "Netflix", "fetch": fetch_netflix},
    {"name": "Zendesk", "fetch": fetch_zendesk},
    {"name": "Apple", "fetch": fetch_apple},
    {"name": "Shopify", "fetch": fetch_shopify},
    {"name": "Google", "fetch": fetch_google},
]


def load_state() -> dict:
    if CUSTOM_STATE_FILE.exists():
        try:
            return json.loads(CUSTOM_STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_state(state: dict) -> None:
    CUSTOM_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _handle_structure_changed(name: str, err: Exception, entry: dict, send_to_bot) -> None:
    print(f"[custom:{name}] СТРУКТУРА ПОМЕНЯЛАСЬ: {err}")
    now = datetime.now(timezone.utc)
    last = entry.get("broken_since")
    should_alert = True
    if last:
        try:
            should_alert = (now - datetime.fromisoformat(last)).total_seconds() > ALERT_COOLDOWN_HOURS * 3600
        except ValueError:
            pass
    if should_alert:
        send_to_bot(
            f"⚠️ HTML скелет сайта {html_lib.escape(name, quote=False)} поменялся\n\n"
            f"{html_lib.escape(str(err), quote=False)}"
        )
        entry["broken_since"] = now.isoformat()


def _process_jobs(name: str, jobs: list, entry: dict, first_run: bool, conn, send_to_bot) -> None:
    seen = set(entry.get("seen", []))
    for job_id, text, link, posted_at in jobs:
        if job_id in seen:
            continue
        seen.add(job_id)                              # отмечаем сразу, чтобы при сбое ниже не застрять навсегда
        if first_run and not SEND_ON_FIRST_RUN:
            continue
        try:
            plain = text.replace("<b>", "").replace("</b>", "")
            if site_parser.is_match(plain):
                print(f"[custom:{name}] подходит: {link}")
                db.save_custom_vacancy(conn, name, job_id, link, text, posted_at)
                body = site_parser._safe_truncate(text, 1500)
                send_to_bot(f"🏢 {html_lib.escape(name, quote=False)}\n🔗 {html_lib.escape(link, quote=False)}\n\n{body}")
                time.sleep(1)
        except Exception:
            print(f"[custom:{name}] ошибка на вакансии {job_id}, пропускаю:")
            traceback.print_exc()
    entry["seen"] = sorted(seen)


def run_once(state: dict, conn, send_to_bot, max_workers: int = 6) -> None:
    """Один проход: грузим все сайты параллельно, обрабатываем результаты по очереди."""
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(c["fetch"]): c for c in CUSTOM_COMPANIES}
        for future in as_completed(futures):
            company = futures[future]
            name = company["name"]
            first_run = name not in state
            entry = state.setdefault(name, {"seen": [], "broken_since": None})
            try:
                jobs = future.result()
            except StructureChangedError as e:
                _handle_structure_changed(name, e, entry, send_to_bot)
                save_state(state)
                continue
            except Exception as e:                   # сеть, таймаут — не повод для алерта о поломке разметки
                print(f"[custom:{name}] не удалось загрузить: {e}")
                save_state(state)
                continue
            entry["broken_since"] = None              # снова работает
            try:
                _process_jobs(name, jobs, entry, first_run, conn, send_to_bot)
            except Exception:
                print(f"[custom:{name}] непредвиденная ошибка, пропускаю компанию:")
                traceback.print_exc()
            save_state(state)
