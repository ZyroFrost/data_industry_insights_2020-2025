#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crawl_vn_full_requests_only.py
Single-file crawler (requests + BeautifulSoup) for VN job sites (stable ones only)
Crawls: Joboko, JobsGO, TimViecNhanh, MyWork, Vieclam24h, CareerLink, ChoTot (API)
Outputs:
  ./output/<source>_jobs.csv
  ./output/merged_vn_jobs_full.csv
Fields:
  source, job_id, title, company, location, city, country, posted_date, expired_date,
  min_salary, max_salary, currency, employment_type, seniority, description, skills, url, scraped_at
"""

import re
import time
import random
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import parser as dateparser
from tqdm import tqdm

# ---------------- CONFIG ----------------
CONFIG = {
    "OUTPUT_DIR": "output",
    "JOBOKO_PAGES": 20,
    "JOBS_GO_PAGES": 20,
    "TIMVIECNHANH_PAGES": 20,
    "MYWORK_PAGES": 20,
    "VIECLAM24H_PAGES": 20,
    "CAREERLINK_PAGES": 20,
    "CHOTOT_PAGES": 10,
    "DELAY": (0.3, 1.0),
    "RETRY": 3,
    "TIMEOUT": 12,
    "USER_AGENTS": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)",
    ],
    "VERBOSE": True,
    "COUNTRY": "VN"
}

# Logging
logging.basicConfig(level=logging.INFO if CONFIG["VERBOSE"] else logging.WARNING,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("vn_requests_crawler")

# Ensure output dir
Path(CONFIG["OUTPUT_DIR"]).mkdir(parents=True, exist_ok=True)

# Canonical fields
FIELDS = [
    "source", "job_id", "title", "company", "location", "city", "country",
    "posted_date", "expired_date", "min_salary", "max_salary", "currency",
    "employment_type", "seniority", "description", "skills", "url", "scraped_at"
]

# ---------- Utilities ----------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def rand_sleep():
    time.sleep(random.uniform(*CONFIG["DELAY"]))

def headers():
    return {"User-Agent": random.choice(CONFIG["USER_AGENTS"])}

def safe_get(url: str, params: dict = None, verify: bool = True) -> Optional[requests.Response]:
    for attempt in range(CONFIG["RETRY"]):
        try:
            r = requests.get(url, params=params, headers=headers(), timeout=CONFIG["TIMEOUT"], verify=verify)
            if r.status_code == 200:
                return r
            else:
                logger.debug("GET %s -> status %s", url, r.status_code)
                # short backoff on 429/403
                if r.status_code in (403, 429):
                    time.sleep(1 + attempt*1.5)
        except Exception as e:
            logger.debug("Request error %s -> %s", url, e)
            time.sleep(0.5 + attempt)
    return None

def safe_get_text(url: str, params: dict = None, verify: bool = True) -> Optional[str]:
    r = safe_get(url, params=params, verify=verify)
    return r.text if r is not None else None

def text_clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    # remove excess whitespace and newlines, keep plain text
    txt = re.sub(r'\s+', ' ', s).strip()
    return txt if txt else None

def guess_city(location: Optional[str], description: Optional[str]) -> Optional[str]:
    if location:
        txt = location.lower()
        for city in COMMON_CITIES:
            if city.lower() in txt:
                return city
    if description:
        txt = description.lower()
        for city in COMMON_CITIES:
            if city.lower() in txt:
                return city
    return None

def extract_salary(text: Optional[str]) -> (Optional[float], Optional[float], Optional[str]):
    """
    Return (min_salary, max_salary, currency)
    Salary parsing heuristic: supports VND, VNĐ, USD, $; numbers with separators.
    Returns amounts in original currency units (e.g. VND).
    """
    if not text:
        return None, None, None
    t = text.replace('\xa0',' ').replace(',', '').replace('.', '')
    # common currency
    cur = None
    if re.search(r'\b(vnd|vnđ|vnđ|đ|dong)\b', text.lower()):
        cur = "VND"
    elif re.search(r'\b(us\$|\$|usd)\b', text.lower()):
        cur = "USD"
    # find number ranges like 10-20 triệu, 10 đến 20 triệu, từ 10 triệu
    # handle "triệu" -> multiply
    million = 1
    if re.search(r'\btriệ(u|o)?\b', text.lower()):
        million = 1_000_000
    # find numbers
    nums = re.findall(r'(\d{1,3}(?:[.,]\d{1,3})?)', text)
    # also find patterns like "10-20" or "10 - 20"
    range_match = re.search(r'(\d{1,3}(?:[.,]\d{1,3})?)\s*[-tođến–]\s*(\d{1,3}(?:[.,]\d{1,3})?)', text.replace('–','-'))
    if range_match:
        a = float(range_match.group(1).replace(',', '.'))
        b = float(range_match.group(2).replace(',', '.'))
        # if 'triệu' detected, scale by 1e6
        if 'triệu' in text.lower() or 'trieu' in text.lower():
            a *= 1_000_000
            b *= 1_000_000
            cur = cur or "VND"
        return int(a), int(b), cur
    # if single number with 'triệu' or 'tháng' present
    if 'triệu' in text.lower() or 'trieu' in text.lower():
        m = re.search(r'(\d+(?:[.,]\d+)?)', text)
        if m:
            v = float(m.group(1).replace(',', '.')) * 1_000_000
            return int(v), int(v), cur or "VND"
    # fallback: take first two numbers
    if nums:
        try:
            vals = [float(n.replace(',','').replace('.','')) for n in nums]
            if len(vals) >= 2:
                return int(vals[0]), int(vals[1]), cur
            else:
                return int(vals[0]), int(vals[0]), cur
        except:
            pass
    return None, None, cur

# Skills list (common in data job postings)
COMMON_SKILLS = [
    "python","sql","r","pandas","numpy","spark","hadoop","aws","gcp","azure",
    "tensorflow","pytorch","scikit-learn","docker","kubernetes","airflow",
    "powerbi","tableau","lookml","excel","matplotlib","seaborn","nlp",
    "deep learning","machine learning","data visualization","etl","bi",
    "data engineering","big data","statistics","probability","linear regression"
]

# Common Vietnamese cities for city extraction
COMMON_CITIES = [
    "Hà Nội","Ha Noi","Hanoi","Hồ Chí Minh","Ho Chi Minh","HCMC","HCM",
    "Đà Nẵng","Da Nang","Hai Phong","HaiPhong","Can Tho","Cần Thơ","CanTho",
    "Bắc Ninh","Binh Duong","Bình Dương","Hai Duong","Đà Lạt"
]

def extract_skills(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    desc = description.lower()
    found = set()
    for skill in COMMON_SKILLS:
        if skill.lower() in desc:
            found.add(skill)
    return ",".join(sorted(found)) if found else None

def infer_seniority(description: Optional[str], title: Optional[str]) -> Optional[str]:
    txt = (title or '') + ' ' + (description or '')
    txt = txt.lower()
    if any(k in txt for k in ['senior', 'sr.', 'sr ', 'lead', 'principal', 'trưởng', 'chuyên gia']):
        return "Senior"
    if any(k in txt for k in ['mid', 'experienced', 'kỹ sư cấp trung', 'intermediate']):
        return "Mid"
    if any(k in txt for k in ['junior', 'jr.', 'jr ', 'mới tốt nghiệp', 'intern', 'thực tập']):
        return "Junior"
    return None

def infer_employment_type(description: Optional[str], title: Optional[str]) -> Optional[str]:
    txt = (title or '') + ' ' + (description or '')
    txt = txt.lower()
    if 'part-time' in txt or 'bán thời gian' in txt or 'part time' in txt:
        return "Part-time"
    if 'contract' in txt or 'hợp đồng' in txt:
        return "Contract"
    return "Full-time"

# ---------- Scrapers for each site (requests only) ----------
# Each returns list[dict] with canonical fields

# Joboko
def crawl_joboko(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base = "https://www.joboko.com"
    logger.info("Starting Joboko (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        url = f"{base}/viec-lam-data-trang-{p}.html"
        html = safe_get_text(url)
        if not html:
            logger.info("Joboko: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".job-item, .job-card, .job")
        if not cards:
            logger.info("Joboko: no cards on page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                company = (c.select_one(".company") or c.select_one(".job-company"))
                company = text_clean(company.get_text()) if company else None
                # fetch detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, .detail-content, .info-job")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        # try salary selectors
                        sal_el = dsoup.find(text=re.compile(r'lương|salary', re.I))
                        if sal_el:
                            SAL = sal_el.parent.get_text(" ")
                            salary_min, salary_max, currency = extract_salary(SAL)
                        # posted date
                        date_el = dsoup.select_one(".post-date, .ngay-dang, time")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                        rand_sleep()
                loc = c.select_one(".location, .job-location")
                location = text_clean(loc.get_text()) if loc else None
                city = guess_city(location, description)
                skills = extract_skills(description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"joboko",
                    "job_id": f"joboko_{abs(hash(href))}" if href else f"joboko_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("Joboko card parse error: %s", e)
        rand_sleep()
    logger.info("Joboko done: %s rows", len(rows))
    return rows

# JobsGO
def crawl_jobsgo(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base="https://jobsgo.vn"
    logger.info("Starting JobsGO (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        if p==1:
            url = f"{base}/viec-lam/data.html"
        else:
            url = f"{base}/viec-lam/data-p{p}.html"
        html = safe_get_text(url)
        if not html:
            logger.info("JobsGO: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".job-item, .list-job-item, .card")
        if not cards:
            logger.info("JobsGO: no cards page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                company = c.select_one(".company, .job-company")
                company = text_clean(company.get_text()) if company else None
                # detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, .detail-content")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        # salary find
                        cont_text = dsoup.get_text(" ")
                        salary_min, salary_max, currency = extract_salary(cont_text)
                        # posted date
                        date_el = dsoup.select_one("time, .posted, .date")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                    rand_sleep()
                location = text_clean((c.select_one(".location") or c.select_one(".job-location") or c.select_one(".job-city")).get_text()) if c.select_one(".location") else None
                city = guess_city(location, description)
                skills = extract_skills(description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"jobsgo",
                    "job_id": f"jobsgo_{abs(hash(href))}" if href else f"jobsgo_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("JobsGO card parse error: %s", e)
        rand_sleep()
    logger.info("JobsGO done: %s rows", len(rows))
    return rows

# TimViecNhanh
def crawl_timviecnhanh(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base="https://www.timviecnhanh.com"
    logger.info("Starting TimViecNhanh (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        url = f"{base}/tim-kiem-viec-lam?keyword=data&page={p}"
        html = safe_get_text(url)
        if not html:
            logger.info("TimViecNhanh: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".list_jobbox .jobbox, .job-item, .jobBox")
        if not cards:
            logger.info("TimViecNhanh: no cards page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                comp = c.select_one(".companyName, .company")
                company = text_clean(comp.get_text()) if comp else None
                # detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, .content")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        salary_min, salary_max, currency = extract_salary(dsoup.get_text(" "))
                        date_el = dsoup.select_one(".date, .post-date, time")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                    rand_sleep()
                loc = c.select_one(".jobCity, .location")
                location = text_clean(loc.get_text()) if loc else None
                skills = extract_skills(description)
                city = guess_city(location, description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"timviecnhanh",
                    "job_id": f"tvn_{abs(hash(href))}" if href else f"tvn_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("TimViecNhanh card error: %s", e)
        rand_sleep()
    logger.info("TimViecNhanh done: %s rows", len(rows))
    return rows

# MyWork
def crawl_mywork(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base="https://www.mywork.com.vn"
    logger.info("Starting MyWork (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        url = f"{base}/tim-viec-lam?q=data&page={p}"
        html = safe_get_text(url)
        if not html:
            logger.info("MyWork: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".job-item, .joblist .item")
        if not cards:
            logger.info("MyWork: no cards page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a.job-link, a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                comp = c.select_one(".company, .company-name")
                company = text_clean(comp.get_text()) if comp else None
                # detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, #job-desc")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        salary_min, salary_max, currency = extract_salary(dsoup.get_text(" "))
                        date_el = dsoup.select_one(".date, .posted, time")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                    rand_sleep()
                loc = c.select_one(".location")
                location = text_clean(loc.get_text()) if loc else None
                city = guess_city(location, description)
                skills = extract_skills(description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"mywork",
                    "job_id": f"mywork_{abs(hash(href))}" if href else f"mywork_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("MyWork card error: %s", e)
        rand_sleep()
    logger.info("MyWork done: %s rows", len(rows))
    return rows

# Vieclam24h
def crawl_vieclam24h(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base="https://vieclam24h.vn"
    logger.info("Starting Vieclam24h (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        url = f"{base}/tim-kiem/viec-lam?page={p}&keyword=data"
        html = safe_get_text(url)
        if not html:
            logger.info("Vieclam24h: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".jobList li, .box-job, .list-job .item")
        if not cards:
            logger.info("Vieclam24h: no cards page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                comp = c.select_one(".company")
                company = text_clean(comp.get_text()) if comp else None
                # detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, .jobContent")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        salary_min, salary_max, currency = extract_salary(dsoup.get_text(" "))
                        date_el = dsoup.select_one("time, .date, .posted")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                    rand_sleep()
                loc = c.select_one(".local")
                location = text_clean(loc.get_text()) if loc else None
                city = guess_city(location, description)
                skills = extract_skills(description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"vieclam24h",
                    "job_id": f"vieclam24h_{abs(hash(href))}" if href else f"vieclam24h_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("Vieclam24h card error: %s", e)
        rand_sleep()
    logger.info("Vieclam24h done: %s rows", len(rows))
    return rows

# CareerLink
def crawl_careerlink(max_pages:int=10)->List[Dict[str,Any]]:
    rows=[]
    base="https://www.careerlink.vn"
    logger.info("Starting CareerLink (pages=%s)", max_pages)
    for p in range(1, max_pages+1):
        url = f"{base}/vieclam/list?keyword=data&page={p}"
        html = safe_get_text(url)
        if not html:
            logger.info("CareerLink: no response page %s -> stop", p)
            break
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(".job-item, .row-job")
        if not cards:
            logger.info("CareerLink: no cards page %s -> stop", p)
            break
        for c in cards:
            try:
                a = c.select_one("a")
                href = a.get("href") if a else None
                if href and href.startswith("/"):
                    href = base + href
                title = text_clean(a.get_text()) if a else None
                comp = c.select_one(".company")
                company = text_clean(comp.get_text()) if comp else None
                # detail
                description=None; posted=None; salary_min=None; salary_max=None; currency=None
                if href:
                    det = safe_get_text(href)
                    if det:
                        dsoup = BeautifulSoup(det, "lxml")
                        desc_el = dsoup.select_one(".job-description, .description, .detail")
                        description = text_clean(desc_el.get_text(" ")) if desc_el else None
                        salary_min, salary_max, currency = extract_salary(dsoup.get_text(" "))
                        date_el = dsoup.select_one(".date, time, .posted")
                        if date_el:
                            try:
                                posted = dateparser.parse(date_el.get_text(), fuzzy=True).isoformat()
                            except:
                                posted = None
                    rand_sleep()
                loc = c.select_one(".address, .location")
                location = text_clean(loc.get_text()) if loc else None
                city = guess_city(location, description)
                skills = extract_skills(description)
                seniority = infer_seniority(description, title)
                employment_type = infer_employment_type(description, title)
                rows.append({
                    "source":"careerlink",
                    "job_id": f"careerlink_{abs(hash(href))}" if href else f"careerlink_{p}_{random.randint(1,10**6)}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "city": city,
                    "country": CONFIG["COUNTRY"],
                    "posted_date": posted,
                    "expired_date": None,
                    "min_salary": salary_min,
                    "max_salary": salary_max,
                    "currency": currency or "VND",
                    "employment_type": employment_type,
                    "seniority": seniority,
                    "description": description,
                    "skills": skills,
                    "url": href,
                    "scraped_at": now_iso()
                })
            except Exception as e:
                logger.debug("CareerLink card error: %s", e)
        rand_sleep()
    logger.info("CareerLink done: %s rows", len(rows))
    return rows

# ChoTot API
def crawl_chotot(max_pages:int=5)->List[Dict[str,Any]]:
    rows=[]
    logger.info("Starting ChoTot (gateway API) pages=%s", max_pages)
    base = "https://gateway.chotot.com/v1/public/job-listing"
    for p in range(0, max_pages):
        params = {"keyword":"data", "page": p, "size": 30}
        try:
            r = requests.get(base, params=params, headers=headers(), timeout=CONFIG["TIMEOUT"])
            if r.status_code != 200:
                logger.warning("ChoTot API page %s -> status %s", p, r.status_code)
                break
            data = r.json()
            ads = data.get("ads") or data.get("items") or []
            if not ads:
                break
            for ad in ads:
                try:
                    title = ad.get("subject") or ad.get("title")
                    description = text_clean(ad.get("description") or "")
                    location = ad.get("area_name") or ad.get("province_name")
                    salary_min, salary_max, currency = extract_salary(ad.get("salary") or description or "")
                    skills = extract_skills(description)
                    city = guess_city(location, description)
                    rows.append({
                        "source":"chotot",
                        "job_id": f"chotot_{ad.get('list_id') or random.randint(1,10**9)}",
                        "title": text_clean(title),
                        "company": None,
                        "location": location,
                        "city": city,
                        "country": CONFIG["COUNTRY"],
                        "posted_date": None,
                        "expired_date": None,
                        "min_salary": salary_min,
                        "max_salary": salary_max,
                        "currency": currency or "VND",
                        "employment_type": infer_employment_type(description, title),
                        "seniority": infer_seniority(description, title),
                        "description": description,
                        "skills": skills,
                        "url": f"https://www.chotot.com/{ad.get('list_id')}" if ad.get('list_id') else None,
                        "scraped_at": now_iso()
                    })
                except Exception as e:
                    logger.debug("ChoTot ad parse error: %s", e)
        except Exception as e:
            logger.warning("ChoTot request error page %s: %s", p, e)
            break
        rand_sleep()
    logger.info("ChoTot done: %s rows", len(rows))
    return rows

# ---------------- Merge & Save ----------------
def save_csv(rows: List[Dict[str,Any]], filename: str):
    df = pd.DataFrame(rows)
    # ensure columns
    for c in FIELDS:
        if c not in df.columns:
            df[c] = None
    df = df[FIELDS]
    path = Path(CONFIG["OUTPUT_DIR"]) / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("Saved %s rows to %s", len(df), path)

def dedupe_merge(all_rows: List[Dict[str,Any]]) -> pd.DataFrame:
    if not all_rows:
        return pd.DataFrame(columns=FIELDS)
    df = pd.DataFrame(all_rows)
    for c in FIELDS:
        if c not in df.columns:
            df[c] = None
    df["url_norm"] = df["url"].fillna("").astype(str)
    df["key"] = df.apply(lambda r: r["url_norm"] if r["url_norm"] else f'{(r.get("title") or "").strip().lower()}|{(r.get("company") or "").strip().lower()}|{(r.get("location") or "").strip().lower()}', axis=1)
    df = df.drop_duplicates(subset=["key"]).drop(columns=["url_norm","key"])
    df = df[FIELDS].reset_index(drop=True)
    return df

# ---------------- Main orchestration ----------------
def main():
    all_rows: List[Dict[str,Any]] = []

    try:
        jbk = crawl_joboko(CONFIG["JOBOKO_PAGES"])
        save_csv(jbk, "joboko_jobs.csv")
        all_rows.extend(jbk)
    except Exception as e:
        logger.exception("Joboko failed: %s", e)

    try:
        jsg = crawl_jobsgo(CONFIG["JOBS_GO_PAGES"])
        save_csv(jsg, "jobsgo_jobs.csv")
        all_rows.extend(jsg)
    except Exception as e:
        logger.exception("JobsGO failed: %s", e)

    try:
        tvn = crawl_timviecnhanh(CONFIG["TIMVIECNHANH_PAGES"])
        save_csv(tvn, "timviecnhanh_jobs.csv")
        all_rows.extend(tvn)
    except Exception as e:
        logger.exception("TimViecNhanh failed: %s", e)

    try:
        myw = crawl_mywork(CONFIG["MYWORK_PAGES"])
        save_csv(myw, "mywork_jobs.csv")
        all_rows.extend(myw)
    except Exception as e:
        logger.exception("MyWork failed: %s", e)

    try:
        v24 = crawl_vieclam24h(CONFIG["VIECLAM24H_PAGES"])
        save_csv(v24, "vieclam24h_jobs.csv")
        all_rows.extend(v24)
    except Exception as e:
        logger.exception("Vieclam24h failed: %s", e)

    try:
        cl = crawl_careerlink(CONFIG["CAREERLINK_PAGES"])
        save_csv(cl, "careerlink_jobs.csv")
        all_rows.extend(cl)
    except Exception as e:
        logger.exception("CareerLink failed: %s", e)

    try:
        ct = crawl_chotot(CONFIG["CHOTOT_PAGES"])
        save_csv(ct, "chotot_jobs.csv")
        all_rows.extend(ct)
    except Exception as e:
        logger.exception("ChoTot failed: %s", e)

    # merge & dedupe
    merged = dedupe_merge(all_rows)
    save_csv(merged.to_dict("records"), "merged_vn_jobs_full.csv")
    logger.info("DONE. Total merged rows: %s", len(merged))

if __name__ == "__main__":
    main()
