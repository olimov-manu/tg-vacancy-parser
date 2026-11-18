"""Хранилище найденных вакансий (SQLite)."""

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path("vacancies.db")


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            channel    TEXT    NOT NULL,
            post_id    INTEGER NOT NULL,
            link       TEXT    NOT NULL,
            text       TEXT    NOT NULL,
            posted_at  TEXT,               -- дата публикации поста (UTC, ISO)
            parsed_at  TEXT    NOT NULL,   -- когда спарсили (локальное время, ISO)
            UNIQUE(channel, post_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_at ON vacancies(parsed_at)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS site_vacancies (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            company    TEXT    NOT NULL,
            job_id     TEXT    NOT NULL,   -- id вакансии в ATS компании (строка — у разных ATS разный формат)
            link       TEXT    NOT NULL,
            text       TEXT    NOT NULL,
            posted_at  TEXT,               -- дата публикации вакансии, если ATS её отдаёт
            parsed_at  TEXT    NOT NULL,   -- когда спарсили (локальное время, ISO)
            UNIQUE(company, job_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_site_parsed_at ON site_vacancies(parsed_at)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_vacancies (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            company    TEXT    NOT NULL,
            job_id     TEXT    NOT NULL,
            link       TEXT    NOT NULL,
            text       TEXT    NOT NULL,
            posted_at  TEXT,
            parsed_at  TEXT    NOT NULL,
            UNIQUE(company, job_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_parsed_at ON custom_vacancies(parsed_at)")
    return conn


def save_vacancy(conn, channel, post_id, link, text, posted_at):
    conn.execute(
        "INSERT OR IGNORE INTO vacancies (channel, post_id, link, text, posted_at, parsed_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (channel, post_id, link, text, posted_at, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()


def save_site_vacancy(conn, company, job_id, link, text, posted_at):
    conn.execute(
        "INSERT OR IGNORE INTO site_vacancies (company, job_id, link, text, posted_at, parsed_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (company, job_id, link, text, posted_at, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()


def save_custom_vacancy(conn, company, job_id, link, text, posted_at):
    conn.execute(
        "INSERT OR IGNORE INTO custom_vacancies (company, job_id, link, text, posted_at, parsed_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (company, job_id, link, text, posted_at, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()
