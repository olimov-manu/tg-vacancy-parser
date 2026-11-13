"""
Парсер вакансий из публичных Telegram-каналов через веб-версию (t.me/s/<канал>).
Находит посты с нужными ключевыми словами (без учёта регистра) и шлёт их в твой бот.

Делает ОДИН проход по всем каналам и завершается — рассчитан на запуск по расписанию
(GitHub Actions cron), а не на постоянную работу.

Установка:
    pip install -r requirements.txt

Запуск:
    export BOT_TOKEN="123456:ABC..."     # токен от @BotFather
    export CHAT_ID="123456789"           # твой chat_id (узнать у @userinfobot)
    python tg_vacancy_parser.py

Перед запуском напиши своему боту /start, иначе он не сможет тебе писать.
"""

import json
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import db

# ================== НАСТРОЙКИ ==================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Публичные каналы — username без @.
# Собрано агрегацией крупных подборок IT/вакансионных каналов (vc.ru, potok.io,
# partnerkin, geekjob, soprovodai) и ПРОВЕРЕНО вручную запросом к t.me/s/<channel> —
# в пуле только каналы, которые реально существуют, публичны и отдают превью постов.
# Нерелевантный шум (маркетинг, HR, sales, продакт-менеджмент) не включён.
CHANNELS = [
    "cryptoDevJobs",
    "opento_dev",
    "myitspecialist2020",

    # --- Frontend / JS ---
    "JScript_jobs", "angular_jobs_feed", "forfrontend", "forwebdev", "front_end_dev",
    "frontendInterview", "frontend_rabota", "javascript_jobs_feed", "job_react",
    "job_webdev", "nodejsjobsfeed", "runello_rus_frontend", "runello_rus_html",
    "runello_rus_javascript", "runello_rus_typescript", "runello_rus_webdevelopment",
    "typescript_jobs", "backend_frontend_jobs",

    # --- Python ---
    "forpython", "job_python", "job_pythonz", "pythonpythonjobs", "pythonrabota",
    "python_job_board", "ru_pythonjobs", "runello_rus_python",

    # --- PHP ---
    "jobforphp", "joomla_jobs", "laravel_jobs", "rabotaphp", "runello_rus_php",
    "symfony_careers", "wordpress_jobs", "yii_jobs",

    # --- Java / JVM / .NET / C# ---
    "alljvmjobs", "java_c_net_golang_jobs", "job_javadevs", "jvmjobs", "kotlin_jobs",
    "csharpdevjob", "DotNetRuJobsFeed", "runello_rus_c",

    # --- Go / Golang ---
    "forgoandrust", "godevjob", "golangjob", "rabota_golang", "runello_rus_goland",

    # --- Ruby / Rust / C++ / другие языки ---
    "elixir_jobs", "rubyjob", "forruby", "rust_jobs_feed", "scala_jobs_feed",
    "haskell_job", "profunctor_jobs", "ProCxxJobs", "runello_rus_c20", "forcpp",
    "forcsharp", "rabotaembedded",

    # --- Mobile (iOS/Android/Flutter) ---
    "dartlang_jobs", "dartuz_jobs", "flutter_dart_job", "flutterroles",
    "forallmobile", "ios_android_swift_kotlin_jobs", "mobdevjobs", "mobiledevjob",
    "runello_rus_mobileappdevelopment", "swift_jobs", "runello_rus_backend",

    # --- DevOps / SysAdmin ---
    "DevOpsSRE_Jobs", "JobsforDevsandOps", "bykvaadm", "devops_jobs_feed",
    "devopssjob", "fordevops", "sysadmin_jobs",

    # --- QA / Testing ---
    "forallqa", "ingamejob_qa", "qa_chillout_jobs", "qa_rab", "vacancy_qa",

    # --- GameDev ---
    "DevZoneGD", "DevZoneJunior", "GameDev_and_Tech_vacancy", "Gamedevjobs",
    "gamedev_unity_unreal_engine_jobs", "gamedev_vakansii", "gamedevjob",
    "gdjobs", "job_gamedev", "rabota_v_gamedeve", "forgamedev",
    "ue_pub", "uejobs", "unity_jobs", "unityjobs_pub", "Jobs_Unity", "young_gamedev",
    "devjobs",

    # --- Blockchain / Web3 ---
    "cryptoheadhunter", "cryptojobslist", "ethereum_hh", "ibcghr", "opento_crypto",
    "yourweb3jobs", "job_web3", "holder_job_devs",

    # --- Data / AI / ML ---
    "bds_job", "data_science_job", "data_science_place", "datajob", "datajobskz",
    "datasciencejobs", "datascienceml_jobs", "ml_data_science_job", "ml_jobs",
    "ml_jobs_kz", "opento_data",

    # --- InfoSec ---
    "CyberSecurityJobs", "appsec_job", "cisoclub", "cybervacancies_channel",
    "infosec_work",

    # --- CMS / 1С ---
    "Bitrix_1C_Dev_Job", "esres_1c", "FastCodeCareer", "joboneC",

    # --- Архитекторы / тимлиды ---
    "itposition", "jc_it", "jobfortm", "teamlead_job_it",

    # --- Общие IT-агрегаторы / junior / релокация ---
    "Relocats", "Remoteit", "Solvery", "budujobs", "geekjobs", "getitrussia",
    "jobforjunior", "jobs_abroad", "jobscode_infull", "jobskolkovo",
    "junior_vacancies", "juniors_rabota_jobs", "juno_jobs", "over100",
    "partnerkin_job", "remotegeekjob", "studyqa", "vezdeworker",
    "zizu_IT_RU", "habr_career", "digital_hr", "progjob", "careerspace",
    "job_akvelon", "AGIMA_Outsource", "itfreelancers", "webfrl",

    # --- Релокация IT-специалистов / страновые IT-сообщества ---
    "young_relocate", "careernoborders", "RelocationDEV", "digitalnomadsdv",
    "Alexstaffagency", "chursovQA", "youritjob", "hub_cy", "cyithr", "jobsincyprus",
    "montenegro_it_jobs", "montenegro_it_events", "thegravityagency", "young_june",

    # --- Blockchain/crypto (доп.) ---
    "workingincrypto",

    # --- IT-вакансии по странам СНГ/Азии (Казахстан, Узбекистан, Турция) ---
    "uzdev_jobs", "devkz_jobs", "workitkz", "jobkz_1", "youritjobturkey", "click_jobs",
    "ITjobs_Uzbekistan", "it_jobs_kz", "devs_kz", "kz_bi_jobs", "go_kz_vacancy",

    # --- Добавлено из подборки каналов (большой агрегированный список, 2026) ---
    "jobsearchIT", "zapwork", "angular_jobs", "durov_it", "PH_ITjobs_Worldwide",
    "itjobstashkent", "UstozShogird", "devjobs221", "frontend_jobs_channel",
    "cv_it_redcode", "ITjobsUZB1", "Cvflow", "teamwork_uz", "job4hr", "itmarket_uz",
    "remote_ru", "jobfeeds", "south_work", "techskill", "ITworksUz", "way2fresher",
    "TTPUmarketing_hiring", "the_workys", "yojob", "yourcareer", "chooseajob",
    "ya_jobs", "dna325", "self_employment_uz", "Blockchain_I_Job", "ishkop_uz",
    "uz_work", "estrabota_uz", "doda_jobs", "bank_vakansiy", "javascript_jobs",
    "kandidat_uz", "mobile_jobs", "belarusitwant", "remotejun", "remotejobss",
    "forproducts", "workvc", "myresume_ru", "devops_jobs", "nodejs_jobs", "qa_jobs",
    "devs_it", "tophr", "toplevel_job", "top_rel", "cyprusithr", "rff_chat",
    "relocaty_jobs", "georgiaitjobs", "uxui_jobs", "uiux_jb", "top_proit",
    "evacuatejobs", "agile_jobs", "hiddengurus", "workayte", "it_chats",
    "GetFrilans", "ITposting", "polyaluzjob", "mnogovakansiy", "stagirovky",
    "designer_ru", "stablegram", "Vakansi_Rus", "Visionisland", "workzavr",
    "uiux_jobs", "fordesigner", "designodromo", "bbe_jobs", "jobpower",
    "motionhunter", "gamedevparty", "job_in_tashkent", "novosti_vakansii",
    "remote_job_android", "PBH_JOBS", "qajoboffer", "itlenta", "belit_jobs",
    "relocateme", "jobGeeks", "qa_resumes", "univwork", "ingamejob_art",
    "qa_vacancy_pl", "europe_hiring", "jobsineu", "cchellowork", "zhurec",
    "dev_connectablejobs", "fintech_jobs", "zarubezhom_jobs", "huggabletalents",
    "agilefluent", "itjobs_nocode", "User_Acquisition_Job", "freshershiringjobs",
    "vakansiilive", "proffreelancee", "vakansii_it", "runello_rus_datascience",
    "runello_rus_digitalmarketing", "runello_rus_webdesign",
    "runello_rus_contentmanagement", "time2find", "fr_free", "green_light_career",
    "edu_traveler", "heddoworld", "it_vakansii_jobs", "freelancekp", "uzpythonjobs",
    "frontEndJobo", "premiumvacancy", "Uz_Intern", "projavascript",
]

# Обязательные слова: пост должен содержать ХОТЯ БЫ ОДНО из них.
# Регистр не важен: Go, GO, go, Golang, GOLANG — всё ловится.
MUST_MATCH = [r"go", r"golang", r"голанг", r"гоу"]

# Второй фильтр — что это вакансия разработчика (хотя бы одно слово).
# Оставь пустым [], если не нужен.
ROLE_MATCH = [r"разработчик\w*", r"developer", r"backend", r"бэкенд\w*", r"бекенд\w*", r"engineer", r"вакансия"]

# Стоп-слова: если есть хоть одно — пост пропускаем. Можно оставить пустым.
EXCLUDE = [r"junior", r"стажер\w*", r"стажёр\w*"]

SEND_ON_FIRST_RUN = False     # True — при первом запуске отправить подходящие посты с первой страницы
STATE_FILE = Path("seen_posts.json")
MAX_WORKERS = 10              # сколько каналов грузим параллельно (ограничивает время одного прохода)

# ===============================================

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}


def build_regex(words):
    """Целое слово, без учёта регистра. 'Go-разработчик' матчится, 'Google' и 'good' — нет."""
    if not words:
        return None
    return re.compile(r"(?<!\w)(?:" + "|".join(words) + r")(?!\w)", re.IGNORECASE)


MUST_RE = build_regex(MUST_MATCH)
ROLE_RE = build_regex(ROLE_MATCH)
EXCLUDE_RE = build_regex(EXCLUDE)


def is_match(text: str) -> bool:
    if MUST_RE and not MUST_RE.search(text):
        return False
    if ROLE_RE and not ROLE_RE.search(text):
        return False
    if EXCLUDE_RE and EXCLUDE_RE.search(text):
        return False
    return True


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_posts(channel: str, retries: int = 3):
    """Возвращает список (post_id, text, link, posted_at) с первой страницы веб-превью канала."""
    url = f"https://t.me/s/{channel}"
    last_error = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))              # 2с, 4с — обрывы TLS/сети обычно разовые
    else:
        raise last_error

    soup = BeautifulSoup(r.text, "html.parser")

    posts = []
    for msg in soup.select("div.tgme_widget_message[data-post]"):
        data_post = msg["data-post"]                    # "channel/12345"
        try:
            post_id = int(data_post.split("/")[-1])
        except ValueError:
            continue
        text_el = msg.select_one("div.tgme_widget_message_text")
        if not text_el:
            continue
        for br in text_el.find_all("br"):
            br.replace_with("\n")
        text = text_el.get_text().strip()
        time_el = msg.select_one("time[datetime]")
        posted_at = time_el["datetime"] if time_el else None
        posts.append((post_id, text, f"https://t.me/{data_post}", posted_at))
    return posts


def send_to_bot(text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    if len(text) > 4000:                                # лимит Telegram — 4096
        text = text[:4000] + "…"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }, timeout=20)
    if resp.status_code == 429:                         # флуд-контроль
        retry = resp.json().get("parameters", {}).get("retry_after", 5)
        time.sleep(retry)
        send_to_bot(text)
    elif not resp.ok:
        print(f"[bot] ошибка {resp.status_code}: {resp.text}")


def process_channel(channel: str, posts: list, state: dict, conn) -> None:
    if not posts:
        print(f"[{channel}] постов нет (канал приватный или превью отключено?)")
        return

    first_run = channel not in state
    last_seen = state.get(channel, 0)
    new_last = last_seen

    for post_id, text, link, posted_at in sorted(posts):
        if post_id <= last_seen:
            continue
        new_last = max(new_last, post_id)          # отмечаем как обработанный даже при сбое ниже,
        if first_run and not SEND_ON_FIRST_RUN:     # чтобы не застрять на одном и том же посте навсегда
            continue
        try:
            if is_match(text):
                print(f"[{channel}] подходит: {link}")
                db.save_vacancy(conn, channel, post_id, link, text, posted_at)
                send_to_bot(f"📢 @{channel}\n🔗 {link}\n\n{text}")
                time.sleep(1)
        except Exception:
            print(f"[{channel}] ошибка на посте {post_id}, пропускаю:")
            traceback.print_exc()

    state[channel] = new_last


def run_once(state: dict, conn) -> None:
    """Один проход по всем каналам: грузим параллельно, обрабатываем по очереди."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_posts, ch): ch for ch in CHANNELS}
        for future in as_completed(futures):
            ch = futures[future]
            try:
                posts = future.result()
            except Exception as e:                      # сеть, таймаут, изменившаяся разметка — что угодно
                print(f"[{ch}] не удалось загрузить: {e}")
                continue
            try:
                process_channel(ch, posts, state, conn)
            except Exception:
                print(f"[{ch}] непредвиденная ошибка, пропускаю канал:")
                traceback.print_exc()
            save_state(state)                            # сразу на диск — чтобы при падении не разослать дубли повторно


def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise SystemExit("Задай BOT_TOKEN и CHAT_ID (переменные окружения)")

    state = load_state()
    conn = db.connect()
    print(f"Старт. Каналов: {len(CHANNELS)}")
    try:
        run_once(state, conn)
    finally:
        conn.close()
    print("Готово.")


if __name__ == "__main__":
    main()
