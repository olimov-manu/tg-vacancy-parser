"""
Парсинг вакансий с карьерных страниц IT-компаний через их ATS
(Greenhouse / Lever / Workable / SmartRecruiters / Ashby / Personio).

В отличие от Telegram-каналов, тут нет последовательного post_id — ATS просто
отдаёт текущий список открытых вакансий при каждом запросе. Поэтому дедупликация
идёт по множеству job_id, увиденных для компании (seen_site_jobs.json), а не по
"последнему id" — и дублируется в site_vacancies (db.py, UNIQUE(company, job_id))
как второй, persistent слой защиты от повторной отправки одной и той же вакансии.
"""

import json
import re
import time
import traceback
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import db

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

SITE_STATE_FILE = Path("seen_site_jobs.json")
SEND_ON_FIRST_RUN = True   # True — при первом запуске отправить все текущие открытые вакансии компании

# Компании и их ATS. slug — это и есть "токен борда", видимый в URL карьерной
# страницы (jobs.lever.co/<slug>, boards.greenhouse.io/<slug> и т.д.).
# Заполняется только проверенными записями (slug подтверждён реальным запросом
# к API ATS) — угадывать slug нельзя, иначе компания будет тихо возвращать 404.
COMPANIES = [
    {"name": "Veeam", "ats": "greenhouse", "slug": "veeamsoftware"},
    {"name": "Miro", "ats": "greenhouse", "slug": "realtimeboardglobal"},
    {"name": "Nebius", "ats": "greenhouse", "slug": "nebius"},
    {"name": "PandaDoc", "ats": "greenhouse", "slug": "pandadoc"},
    {"name": "Picsart", "ats": "greenhouse", "slug": "picsart"},
    {"name": "BigID", "ats": "greenhouse", "slug": "bigid"},
    {"name": "Lokalise", "ats": "greenhouse", "slug": "lokalise"},
    {"name": "Avride", "ats": "greenhouse", "slug": "avride"},
    {"name": "Wheely", "ats": "greenhouse", "slug": "wheely"},
    {"name": "Wargaming", "ats": "greenhouse", "slug": "wargamingen"},
    {"name": "Airslate", "ats": "lever", "slug": "airslate"},
    {"name": "Recraft", "ats": "ashby", "slug": "recraft"},
    {"name": "QuickNode", "ats": "ashby", "slug": "quicknode"},
    {"name": "3Commas", "ats": "ashby", "slug": "3commas"},
    {"name": "ManyChat", "ats": "ashby", "slug": "manychat"},
    {"name": "Temporal", "ats": "ashby", "slug": "temporal"},
    {"name": "Teleport", "ats": "ashby", "slug": "goteleport"},
    {"name": "Flipper", "ats": "ashby", "slug": "flipper"},
    {"name": "Bumble", "ats": "ashby", "slug": "bumbleinc"},
    {"name": "Libertex Group", "ats": "workable", "slug": "libertexgroup"},
    {"name": "Libertex Europe", "ats": "workable", "slug": "libertex-europe"},
    {"name": "Freedom24", "ats": "workable", "slug": "freedomfinanceeu"},
    {"name": "Mediacube", "ats": "workable", "slug": "mediacube"},
    {"name": "Joom", "ats": "workable", "slug": "joom"},
]


# Фильтр специально для вакансий с сайтов компаний, а не переиспользованный
# фильтр из tg_vacancy_parser: там посты короткие и обычно явно называют стек,
# тут — длинные англоязычные описания, где слово "Go" без контекста языка
# программирования массово ложно срабатывает на обычных фразах вида
# "Go beyond...", "Go / No-Go decision", "Go-to-market" (частые маркетинговые
# и ценностные формулировки в описаниях вакансий). Поэтому бьём "Go" только
# когда рядом характерный для стека контекст: "golang" явно, "Go" в списке
# языков через запятую/слэш/and/or, "Go developer/engineer", или рядом с
# "experience/knowledge/proficient... in/with Go".
_GOLANG_RE = re.compile(r"\bgolang\b", re.IGNORECASE)
_LANGS = r"(?:Python|Java|Kotlin|C\+\+|Rust|PHP|Ruby|TypeScript|JavaScript|JS|Scala|C#|Node(?:\.js)?|Swift|Elixir)"
_GO_CONTEXT_RE = re.compile(
    r"(?<!\w)Go(?!\w)\s*(?:,|/|&|and|or)\s*" + _LANGS + r"|"
    + _LANGS + r"\s*(?:,|/|&|and|or)\s*(?<!\w)Go(?!\w)|"
    r"(?<!\w)Go(?!\w)\s+(?:developer|engineer|programmer)|"
    r"(?:developer|engineer|programmer)\w*\s+(?:in|with)\s+(?<!\w)Go(?!\w)|"
    r"(?:experience|proficien\w*|fluent|knowledge|skilled|expert\w*)\w*\s+(?:in|with|of)\s+(?<!\w)Go(?!\w)",
    re.IGNORECASE,
)
EXCLUDE_RE = re.compile(r"(?<!\w)(?:junior|intern(?:ship)?|стажер\w*|стажёр\w*)(?!\w)", re.IGNORECASE)


def is_match(text: str) -> bool:
    if not (_GOLANG_RE.search(text) or _GO_CONTEXT_RE.search(text)):
        return False
    if EXCLUDE_RE.search(text):
        return False
    return True


def _strip_html(html: str) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text("\n").strip()


def _local_tag(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def fetch_greenhouse(slug: str):
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json().get("jobs", []):
        title = j.get("title", "")
        content = _strip_html(j.get("content", ""))
        jobs.append((str(j["id"]), f"{title}\n\n{content}", j.get("absolute_url", ""), j.get("updated_at")))
    return jobs


def fetch_lever(slug: str):
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json():
        title = j.get("text", "")
        desc = _strip_html(j.get("description") or j.get("descriptionPlain") or "")
        lists_text = "\n".join(_strip_html(lst.get("content", "")) for lst in j.get("lists", []))
        text = f"{title}\n\n{desc}\n{lists_text}".strip()
        posted_at = None
        ts = j.get("createdAt")
        if ts:
            try:
                posted_at = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
            except (TypeError, ValueError, OSError):
                pass
        jobs.append((str(j["id"]), text, j.get("hostedUrl", ""), posted_at))
    return jobs


def fetch_workable(slug: str):
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json().get("jobs", []):
        job_id = str(j.get("shortcode") or j.get("id"))
        title = j.get("title", "")
        desc = _strip_html(j.get("description") or j.get("full_description") or "")
        text = f"{title}\n\n{desc}".strip()
        posted_at = j.get("published_on") or j.get("created_at")
        jobs.append((job_id, text, j.get("url", ""), posted_at))
    return jobs


def fetch_smartrecruiters(slug: str):
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json().get("content", []):
        job_id = str(j["id"])
        link = j.get("applyUrl") or f"https://jobs.smartrecruiters.com/{slug}/{job_id}"
        jobs.append((job_id, j.get("name", ""), link, j.get("releasedDate")))
    return jobs


def fetch_ashby(slug: str):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json().get("jobs", []):
        title = j.get("title", "")
        desc = _strip_html(j.get("descriptionHtml", ""))
        text = f"{title}\n\n{desc}".strip()
        link = j.get("jobUrl") or j.get("applyUrl", "")
        jobs.append((str(j.get("id")), text, link, j.get("publishedAt")))
    return jobs


def fetch_personio(slug: str):
    url = f"https://{slug}.jobs.personio.de/xml"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    jobs = []
    for pos in root.iter():
        if _local_tag(pos.tag) != "position":
            continue
        fields = {_local_tag(c.tag): (c.text or "").strip() for c in pos}
        job_id = fields.get("id")
        if not job_id:
            continue
        link = fields.get("careerSiteUrl") or f"https://{slug}.jobs.personio.de/job/{job_id}"
        jobs.append((job_id, fields.get("name", ""), link, fields.get("createdAt")))
    return jobs


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "workable": fetch_workable,
    "smartrecruiters": fetch_smartrecruiters,
    "ashby": fetch_ashby,
    "personio": fetch_personio,
}


def fetch_company_jobs(company: dict):
    return FETCHERS[company["ats"]](company["slug"])


def load_site_state() -> dict:
    if SITE_STATE_FILE.exists():
        try:
            return json.loads(SITE_STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_site_state(state: dict) -> None:
    SITE_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def process_company(company: dict, jobs: list, state: dict, conn, send_to_bot) -> None:
    name = company["name"]
    if not jobs:
        print(f"[site:{name}] вакансий нет (пустой борд или сайт не отдал список)")
        return

    first_run = name not in state
    seen = set(state.get(name, []))

    for job_id, text, link, posted_at in jobs:
        if job_id in seen:
            continue
        seen.add(job_id)                              # отмечаем сразу, чтобы при сбое ниже не застрять навсегда
        if first_run and not SEND_ON_FIRST_RUN:
            continue
        try:
            if is_match(text):
                print(f"[site:{name}] подходит: {link}")
                db.save_site_vacancy(conn, name, job_id, link, text, posted_at)
                send_to_bot(f"🏢 {name}\n🔗 {link}\n\n{text[:1500]}")
                time.sleep(1)
        except Exception:
            print(f"[site:{name}] ошибка на вакансии {job_id}, пропускаю:")
            traceback.print_exc()

    state[name] = sorted(seen)


def run_once(state: dict, conn, send_to_bot, max_workers: int = 10) -> None:
    """Один проход по всем компаниям: грузим параллельно, обрабатываем по очереди."""
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_company_jobs, c): c for c in COMPANIES}
        for future in as_completed(futures):
            company = futures[future]
            name = company["name"]
            try:
                jobs = future.result()
            except Exception as e:                   # сеть, таймаут, изменившийся API — что угодно
                print(f"[site:{name}] не удалось загрузить: {e}")
                continue
            try:
                process_company(company, jobs, state, conn, send_to_bot)
            except Exception:
                print(f"[site:{name}] непредвиденная ошибка, пропускаю компанию:")
                traceback.print_exc()
            save_site_state(state)
