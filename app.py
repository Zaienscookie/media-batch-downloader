#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
媒体批量下载 WebUI
- 输入用户首页(Bsky/Twitter/YouTube 频道)或单条链接，扫描其中的图片/视频/GIF
- 前端勾选需要下载的资源，批量下载到本机
- 代理可手动配置（http/https/socks5）
"""
import os
import re
import io
import sys
import json
import uuid
import time
import shutil
import zipfile
import threading
import asyncio
import subprocess
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import aiohttp
from flask import Flask, request, jsonify, send_file, render_template
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
DL_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DL_DIR, exist_ok=True)

BLUE_API = "https://public.api.bsky.app/xrpc"
BLUE_CDN = "https://cdn.bsky.app/img"
FX_API = "https://api.fxtwitter.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

YTDLP = shutil.which("yt-dlp")
FFMPEG = shutil.which("ffmpeg")

try:
    from aiohttp_socks import ProxyConnector
    HAVE_SOCKS = True
except ImportError:
    HAVE_SOCKS = False

DEFAULT_CONFIG = {
    "proxy_enabled": False,
    "proxy": "http://127.0.0.1:7890",
    "max_mb": 200,
    "max_items": 300,
    "nitter_instance": "",
    "nitter_instances": [
        "http://192.168.2.126:8088",
        "https://xcancel.com",
        "https://nitter.tiekoetter.com",
        "https://nitter.poast.org",
        "https://nitter.privacydev.net",
        "https://nitter.space",
        "https://lightbrd.com",
        "https://nitter.kavin.rocks",
        "https://nitter.1d4.us",
        "https://nitter.net",
    ],
    "twitter_bearer": "",
}

_CONFIG_LOCK = threading.Lock()


class ScanError(Exception):
    pass


class NitterUnusable(ScanError):
    """实例本身不可用（反爬/连接失败等），自动探测时应跳过换下一个"""
    pass


class DownloadError(Exception):
    pass


# ---------------------------------------------------------------- config
def get_config():
    with _CONFIG_LOCK:
        if not os.path.exists(CONFIG_FILE):
            cfg = dict(DEFAULT_CONFIG)
        else:
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
            except Exception:
                cfg = {}
        merged = dict(DEFAULT_CONFIG)
        merged.update(cfg)
        return merged


def save_config(cfg):
    with _CONFIG_LOCK:
        tmp = CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_FILE)


def active_proxy(cfg):
    proxy = (cfg.get("proxy") or "").strip()
    if cfg.get("proxy_enabled") and proxy:
        return proxy
    return None


def is_socks(proxy):
    return proxy.startswith(("socks4", "socks5"))


async def make_session(proxy):
    timeout = aiohttp.ClientTimeout(total=600, sock_read=180)
    if proxy and is_socks(proxy):
        if not HAVE_SOCKS:
            raise RuntimeError("socks 代理需要 aiohttp-socks，请 pip install aiohttp-socks 或改用 http 代理")
        conn = ProxyConnector.from_url(proxy)
        return aiohttp.ClientSession(connector=conn, timeout=timeout)
    return aiohttp.ClientSession(timeout=timeout)


def req_proxy(proxy):
    """http/https 代理直接传给 aiohttp；socks 已由连接器处理，这里返回 None"""
    if proxy and is_socks(proxy):
        return None
    return proxy


# ---------------------------------------------------------------- helpers
def mk_item(platform, mtype, title, source, thumb="", page="", date="", width=0, height=0):
    return {
        "id": uuid.uuid4().hex[:12],
        "platform": platform,
        "type": mtype,
        "title": title,
        "source": source,
        "thumb": thumb,
        "page": page,
        "date": date,
        "width": width,
        "height": height,
    }


def clip(text, n=80):
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:n]


def http_ext(content_type, url, type_hint=""):
    ct = (content_type or "").lower()
    if "mp4" in ct or "video" in ct:
        return ".mp4"
    if "webm" in ct:
        return ".webm"
    if "gif" in ct:
        return ".gif"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if type_hint == "gif":
        return ".gif"
    if type_hint == "video":
        return ".mp4"
    m = re.search(r"\.(jpe?g|png|gif|webp|mp4|webm|mov|mkv)(?:$|[?#])", url, re.I)
    if m:
        return "." + m.group(1).lower()
    return ".bin"


def safe_dl_path(name):
    base = os.path.basename(name)
    return os.path.join(DL_DIR, base)


# ---------------------------------------------------------------- downloaders
async def dl_http(session, url, type_hint, proxy, max_bytes):
    async with session.get(url, proxy=req_proxy(proxy), headers=HEADERS, allow_redirects=True) as r:
        if r.status != 200:
            raise DownloadError(f"HTTP {r.status}")
        data = await r.read()
        if max_bytes and len(data) > max_bytes:
            raise DownloadError("超过大小限制")
        ext = http_ext(r.headers.get("Content-Type", ""), url, type_hint)
        fname = f"{uuid.uuid4().hex}{ext}"
        with open(os.path.join(DL_DIR, fname), "wb") as f:
            f.write(data)
        return fname, len(data)


async def dl_hls(session, master_url, proxy, max_bytes):
    async with session.get(master_url, proxy=req_proxy(proxy), headers=HEADERS) as r:
        if r.status != 200:
            raise DownloadError(f"播放列表 HTTP {r.status}")
        master = await r.text()

    variants = []
    lines = master.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF"):
            nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
            bw = re.search(r"AVERAGE-BANDWIDTH=(\d+)", line)
            res = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
            variants.append((int(bw.group(1)) if bw else 0, res.groups() if res else (0, 0), nxt))
            i += 2
        else:
            i += 1
    if not variants:
        segs_ok = any(l for l in lines if l.strip() and not l.strip().startswith("#"))
        if segs_ok:
            variants = [(0, (0, 0), master_url)]
        else:
            raise DownloadError("无法解析视频播放列表")

    variant_url = max(variants, key=lambda v: v[0])[2]
    if not variant_url.startswith("http"):
        variant_url = urllib.parse.urljoin(master_url, variant_url)
    base = variant_url.rsplit("/", 1)[0] + "/"

    async with session.get(variant_url, proxy=req_proxy(proxy), headers=HEADERS) as r:
        if r.status != 200:
            raise DownloadError(f"视频流 HTTP {r.status}")
        vplay = await r.text()

    segs = []
    for l in vplay.splitlines():
        l = l.strip()
        if l and not l.startswith("#"):
            if l.startswith("http"):
                segs.append(l)
            else:
                segs.append(base + l.lstrip("/"))
    if not segs:
        raise DownloadError("视频流中没有分片")
    if len(segs) > 3000:
        raise DownloadError("分片过多，跳过")

    parts = [None] * len(segs)
    sem = asyncio.Semaphore(4)
    total = 0

    async def get_seg(idx, u):
        nonlocal total
        async with sem:
            async with session.get(u, proxy=req_proxy(proxy), headers=HEADERS) as r:
                if r.status != 200:
                    raise DownloadError(f"分片 {idx} HTTP {r.status}")
                b = await r.read()
                if max_bytes and total + len(b) > max_bytes * 1.1:
                    raise DownloadError("超过大小限制")
                parts[idx] = b
                total += len(b)

    await asyncio.gather(*(get_seg(i, u) for i, u in enumerate(segs)))
    data = b"".join(p for p in parts if p is not None)
    ts_name = f"{uuid.uuid4().hex}.ts"
    with open(os.path.join(DL_DIR, ts_name), "wb") as f:
        f.write(data)
    if FFMPEG:
        mp4 = ts_name[:-3] + ".mp4"
        proc = await asyncio.create_subprocess_exec(
            FFMPEG, "-y", "-i", os.path.join(DL_DIR, ts_name), "-c", "copy",
            os.path.join(DL_DIR, mp4),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        rc = await proc.wait()
        if rc == 0:
            os.remove(os.path.join(DL_DIR, ts_name))
            return mp4, len(data)
    return ts_name, len(data)


async def dl_youtube(url, proxy, max_bytes):
    if not YTDLP:
        raise DownloadError("未安装 yt-dlp，无法下载 YouTube")
    max_mb = int(max_bytes // (1024 * 1024)) if max_bytes else 0
    out_tmpl = os.path.join(DL_DIR, f"yt_{uuid.uuid4().hex}.%(ext)s")
    for attempt in (1, 2):
        fmt = ["bv*+ba/b", "b[ext=mp4]/b"][attempt - 1]
        cmd = [YTDLP, "-f", fmt, "--merge-output-format", "mp4", "-o", out_tmpl,
               "--no-playlist", "--no-warnings", "--no-part"]
        if max_mb:
            cmd += ["--max-filesize", f"{max_mb}M"]
        if proxy:
            cmd += ["--proxy", proxy]
        cmd += [url]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        try:
            _, err = await asyncio.wait_for(proc.communicate(), timeout=1800)
        except asyncio.TimeoutError:
            proc.kill()
            raise DownloadError("下载超时")
        for f in os.listdir(DL_DIR):
            if f.startswith("yt_"):
                return f, os.path.getsize(os.path.join(DL_DIR, f))
        if attempt == 1:
            # 第一个格式失败(如缺 ffmpeg 无法合并)，重试单文件格式
            out_tmpl = os.path.join(DL_DIR, f"yt_{uuid.uuid4().hex}.%(ext)s")
            continue
        msg = err.decode("utf-8", errors="replace")[-300:] if err else "未知错误"
        raise DownloadError(f"yt-dlp 下载失败: {msg}")


async def dl_item(session, item, proxy, max_bytes):
    platform = item.get("platform")
    source = item.get("source") or ""
    mtype = item.get("type") or ""
    if platform == "youtube":
        return await dl_youtube(source, proxy, max_bytes)
    if mtype == "video" and "video.bsky.app/watch" in source and ".m3u8" in source:
        return await dl_hls(session, source, proxy, max_bytes)
    return await dl_http(session, source, mtype, proxy, max_bytes)


# ---------------------------------------------------------------- scanners
async def scan_bluesky(session, handle, proxy, max_items):
    handle = handle.strip().lstrip("@")
    if not handle:
        raise ScanError("无效的 Bluesky 用户名")
    items = []
    cursor = None
    pages = 0
    while pages < 8 and len(items) < max_items:
        params = {"actor": handle, "limit": 100,
                  "includeReposts": "false", "filter": "posts_no_replies"}
        if cursor:
            params["cursor"] = cursor
        try:
            async with session.get(BLUE_API + "/app.bsky.feed.getAuthorFeed",
                                   params=params, proxy=req_proxy(proxy), headers=HEADERS) as r:
                if r.status != 200:
                    raise ScanError(f"Bluesky API 返回 HTTP {r.status}")
                d = await r.json()
        except aiohttp.ClientError as e:
            raise ScanError(f"请求 Bluesky 失败: {e}")
        feed = d.get("feed") or []
        for f in feed:
            post = f.get("post") or {}
            author = (post.get("author") or {}).get("handle", "")
            if author.lower() != handle.lower():
                continue
            for it in extract_bsky_media(post, handle):
                items.append(it)
                if len(items) >= max_items:
                    break
            if len(items) >= max_items:
                break
        cursor = d.get("cursor")
        if not cursor or not feed:
            break
        pages += 1
    if not items:
        raise ScanError("该用户没有可下载的媒体，或用户名无效")
    return items


def extract_bsky_media(post, handle):
    out = []
    rec = post.get("record") or {}
    text = clip(rec.get("text", ""))
    uri = post.get("uri") or ""
    rkey = uri.rsplit("/", 1)[-1]
    page = f"https://bsky.app/profile/{handle}/post/{rkey}" if rkey and rkey != uri else ""
    date = rec.get("createdAt", "")
    did = (post.get("author") or {}).get("did", "")

    embed = post.get("embed") or {}
    etype = embed.get("$type", "")

    if etype == "app.bsky.embed.images#view":
        for img in embed.get("images") or []:
            full = img.get("fullsize") or ""
            thumb = img.get("thumb") or full
            ar = img.get("aspectRatio") or {}
            if full:
                out.append(mk_item("bluesky", "image", text or "图片",
                                   full, thumb, page, date,
                                   int(ar.get("width", 0) or 0), int(ar.get("height", 0) or 0)))
    elif etype == "app.bsky.embed.video#view":
        playlist = embed.get("playlist") or ""
        thumb = embed.get("thumbnail") or ""
        ar = embed.get("aspectRatio") or {}
        if playlist:
            out.append(mk_item("bluesky", "video", text or "视频",
                               playlist, thumb, page, date,
                               int(ar.get("width", 0) or 0), int(ar.get("height", 0) or 0)))

    # 回退：从 record.embed 原始 blob 构建 CDN 地址
    if not out:
        rem = rec.get("embed") or {}
        rtype = rem.get("$type", "")
        if rtype == "app.bsky.embed.images":
            for img in rem.get("images") or []:
                blob = img.get("image") or {}
                cid = (blob.get("ref") or {}).get("$link", "")
                if not cid:
                    continue
                ext = (blob.get("mimeType") or "image/jpeg").split("/")[-1] or "jpeg"
                if ext not in ("jpg", "jpeg", "png", "gif", "webp"):
                    ext = "jpeg"
                full = f"{BLUE_CDN}/feed_fullsize/plain/{did}/{cid}@{ext}"
                thumb = f"{BLUE_CDN}/feed_thumbnail/plain/{did}/{cid}@{ext}"
                ar = img.get("aspectRatio") or {}
                out.append(mk_item("bluesky", "image", text or "图片", full, thumb, page, date,
                                   int(ar.get("width", 0) or 0), int(ar.get("height", 0) or 0)))
        elif rtype == "app.bsky.embed.video":
            blob = rem.get("video") or {}
            cid = (blob.get("ref") or {}).get("$link", "")
            if cid:
                playlist = f"https://video.bsky.app/watch/{urllib.parse.quote(did)}/{cid}/playlist.m3u8"
                thumb = f"https://video.bsky.app/watch/{urllib.parse.quote(did)}/{cid}/thumbnail.jpg"
                out.append(mk_item("bluesky", "video", text or "视频", playlist, thumb, page, date))
    return out


async def scan_twitter_status(session, url, proxy):
    m = re.search(r"(?:twitter\.com|x\.com)/([^/?#]+)/status/(\d+)", url)
    if not m:
        raise ScanError("无法识别 Twitter 推文链接")
    user, sid = m.group(1), m.group(2)
    try:
        async with session.get(f"{FX_API}/{user}/status/{sid}",
                               proxy=req_proxy(proxy), headers=HEADERS) as r:
            if r.status != 200:
                raise ScanError(f"fxtwitter 返回 HTTP {r.status}")
            d = await r.json()
    except aiohttp.ClientError as e:
        raise ScanError(f"请求 fxtwitter 失败(可能需要代理): {e}")
    t = d.get("tweet") or {}
    if not t:
        raise ScanError("推文不存在或已被删除")
    items = []
    medias = list(t.get("media", {}).get("all", []))
    q = t.get("quote")
    if isinstance(q, dict):
        medias += list((q.get("media", {}) or {}).get("all", []))
    text = clip(t.get("text", ""))
    date = t.get("date") or t.get("createdAt") or ""
    page = f"https://x.com/{user}/status/{sid}"
    for mm in medias:
        mu = mm.get("url") or ""
        if not mu:
            continue
        typ = (mm.get("type") or "").lower()
        thumb = mm.get("thumbnail_url") or ""
        w = int(mm.get("width", 0) or 0)
        h = int(mm.get("height", 0) or 0)
        if typ in ("photo", "image"):
            mu2 = mu
            for a, b in (("name=small", "name=large"), ("name=medium", "name=large"),
                         ("name=orig", "name=large"), ("?format=jpg&name=360x360", "?format=jpg&name=large")):
                mu2 = mu2.replace(a, b)
            items.append(mk_item("twitter", "image", text or "图片", mu2, thumb or mu, page, date, w, h))
        elif typ in ("video", "gif", "animated_gif"):
            g = "gif" if typ in ("gif", "animated_gif") else "video"
            items.append(mk_item("twitter", g, text or "视频", mu, thumb, page, date, w, h))
    if not items:
        raise ScanError("该推文没有可下载的媒体")
    return items


async def scan_twitter_nitter(session, handle, base, proxy, max_items):
    base = (base or "").rstrip("/")
    if not base:
        raise NitterUnusable("未配置 Nitter 实例")
    try:
        async with session.get(f"{base}/{handle}/media", proxy=req_proxy(proxy), headers=HEADERS) as r:
            if r.status != 200:
                raise NitterUnusable(f"HTTP {r.status}")
            html = await r.text()
    except aiohttp.ClientError as e:
        raise NitterUnusable(f"连接失败: {e}")

    low = html.lower()
    if "anubis" in low or "proof of work" in low or "not a bot" in low or "making sure you" in low:
        raise NitterUnusable("开启了 Anubis 反爬验证")
    if ("just a moment" in low) or ("cloudflare" in low and "challenge" in low):
        raise NitterUnusable("被 Cloudflare 验证拦截")
    if "for sale" in low or "domain may be for sale" in low:
        raise NitterUnusable("域名已停用/待售")
    if re.search(r"<title>\s*404\s*</title>", low):
        raise ScanError(f"在 {base} 上找不到用户 @{handle}（账号不存在，或该实例数据较旧）")

    items, cursor = parse_nitter(html, base, handle)
    if not items:
        raise ScanError(f"账号 @{handle} 暂无媒体（在该实例 {base} 上）")

    for _ in range(3):
        if not cursor or len(items) >= max_items:
            break
        try:
            async with session.get(f"{base}/{handle}/media?cursor={cursor}",
                                   proxy=req_proxy(proxy), headers=HEADERS) as r:
                if r.status != 200:
                    break
                html = await r.text()
            page_items, cursor = parse_nitter(html, base, handle)
            items += page_items
        except aiohttp.ClientError:
            break
    return items[:max_items]


def parse_nitter(html, base, handle):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for tl in soup.select(".timeline-item"):
        text_el = tl.select_one(".tweet-content")
        title = clip(" ".join(text_el.get_text(" ", strip=True).split())) if text_el else f"@{handle} 的推文"
        page = ""
        link_el = tl.select_one("a.tweet-link")
        if link_el and link_el.get("href"):
            page = base + link_el["href"] if link_el["href"].startswith("/") else link_el["href"]
        date = ""
        dt_el = tl.select_one(".tweet-date a, .tweet-date")
        if dt_el:
            date = (dt_el.get("title") or dt_el.get_text(strip=True))[:40]
        for a in tl.select("a.still-image, a.media-image"):
            img = a.select_one("img")
            src = (img.get("src") or "") if img else ""
            if not src:
                src = a.get("data-url") or a.get("href") or ""
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = base + src
            if src:
                items.append(mk_item("twitter", "image", title, src, src, page, date))
        for v in tl.select("video"):
            poster = v.get("poster") or ""
            if poster.startswith("/"):
                poster = base + poster
            src = ""
            s = v.select_one("source")
            if s:
                src = s.get("src") or ""
            if not src:
                src = v.get("data-url") or v.get("src") or ""
            if src.startswith("/"):
                src = base + src
            if src:
                items.append(mk_item("twitter", "video", title, src, poster, page, date))
    cursor = ""
    more = soup.select_one("a.show-more, a.more")
    if more and more.get("href"):
        m = re.search(r"cursor=([^&]+)", more["href"])
        if m:
            cursor = m.group(1)
    return items, cursor


async def scan_twitter_user(session, handle, proxy, cfg, max_items):
    handle = handle.strip().lstrip("@")
    if not handle:
        raise ScanError("无效的 Twitter 用户名")

    # 先用 fxtwitter 校验账号是否存在、是否受保护（尽力而为，不影响扫描）
    try:
        async with session.get(f"{FX_API}/{handle}", proxy=req_proxy(proxy),
                               headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                ud = await r.json()
                u = ud.get("user") or {}
                if u.get("protected"):
                    raise ScanError(f"@{handle} 是受保护账号（仅粉丝可见），无法公开扫描媒体")
    except ScanError:
        raise
    except Exception:
        pass

    candidates = []
    override = (cfg.get("nitter_instance") or "").strip().rstrip("/")
    if override:
        candidates.append(override)
    candidates += [x.strip().rstrip("/") for x in (cfg.get("nitter_instances") or []) if x.strip()]

    errs = []
    for base in candidates:
        try:
            return await scan_twitter_nitter(session, handle, base, proxy, max_items)
        except NitterUnusable as e:
            errs.append(f"{base} → {e}")
        except ScanError as e:
            raise ScanError(f"Twitter 用户首页扫描失败：{e}")
    if not errs:
        errs.append("未配置任何 Nitter 实例")
    raise ScanError(
        "Twitter 用户首页扫描失败：以下 Nitter 实例均不可用——\n"
        + "\n".join("  · " + e for e in errs)
        + "\n\n解决办法：\n"
        "① 在页面 ⚙️ 设置里填一个可用的 Nitter 实例地址（每行一个，越靠前越优先）\n"
        "② 用 Docker 自建一个（推荐，你已有服务器）：\n"
        "      docker run -d -p 8080:8080 zedeus/nitter\n"
        "      然后在设置里填 http://127.0.0.1:8080\n"
        "③ 或者直接粘贴单条推文链接 https://x.com/xxx/status/123（走 fxtwitter，无需 Nitter）")


async def scan_youtube(session, url, proxy, max_items):
    if not YTDLP:
        raise ScanError("未安装 yt-dlp，无法扫描 YouTube。请先安装 yt-dlp。")
    cmd = [YTDLP, "--flat-playlist", "--no-warnings", "--no-download",
           "--print", "%(id)s\x1f%(title)s\x1f%(duration)s\x1f%(channel)s",
           "--playlist-end", str(max_items), url]
    if proxy:
        cmd += ["--proxy", proxy]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        proc.kill()
        raise ScanError("yt-dlp 扫描超时")
    if proc.returncode != 0:
        msg = err.decode("utf-8", errors="replace")[-300:] if err else "yt-dlp 退出码非 0"
        raise ScanError(f"yt-dlp 扫描失败: {msg}")
    items = []
    for line in out.decode("utf-8", errors="replace").splitlines():
        parts = line.split("\x1f")
        if len(parts) < 2 or not parts[0]:
            continue
        vid, title, dur, channel = (parts + [""] * (4 - len(parts)))[:4]
        if not vid:
            continue
        items.append(mk_item(
            "youtube", "video", clip(title, 90),
            f"https://www.youtube.com/watch?v={vid}",
            f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            f"https://www.youtube.com/watch?v={vid}",
            channel or "", 0, 0))
        if len(items) >= max_items:
            break
    if not items:
        raise ScanError("未解析到视频（链接无效或需要代理）")
    return items


async def scan_direct(session, url, proxy):
    mtype = "gif" if re.search(r"\.gif(?:$|[?#])|@gif(?:$|[?#])", url, re.I) else "image"
    return [mk_item("url", mtype, os.path.basename(url), url, url, url)]


async def scan_url(session, url, proxy, cfg):
    max_items = int(cfg.get("max_items", 300) or 300)
    host = (urllib.parse.urlsplit(url).netloc or "").lower()
    if host in ("bsky.app", "www.bsky.app", "bsky.social"):
        m = re.search(r"/profile/([^/?#]+)", url)
        if not m:
            raise ScanError("无法识别 Bluesky 用户主页链接，示例：https://bsky.app/profile/用户名")
        return await scan_bluesky(session, m.group(1), proxy, max_items)
    if re.search(r"(?:twitter\.com|x\.com)", host):
        if re.search(r"/status/\d+", url):
            return await scan_twitter_status(session, url, proxy)
        m = re.search(r"(?:twitter\.com|x\.com)/([^/?#]+)", url)
        if m and m.group(1) not in ("home", "explore", "search", "i"):
            return await scan_twitter_user(session, m.group(1), proxy, cfg, max_items)
        raise ScanError("无法识别的 Twitter 链接")
    if re.search(r"(?:youtube\.com|youtu\.be)", host):
        return await scan_youtube(session, url, proxy, max_items)
    if re.search(r"(?:\.|@)(jpe?g|png|gif|webp)(?:$|[?#])", url, re.I):
        return await scan_direct(session, url, proxy)
    raise ScanError("暂不支持该链接。支持：Bluesky 用户主页 / Twitter 推文或用户首页(Nitter) / YouTube 频道 / 图片GIF直链")


# ---------------------------------------------------------------- flask app
app = Flask(__name__)
JOBS = {}
JOBS_LOCK = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "GET":
        cfg = dict(get_config())
        cfg.pop("twitter_bearer", None)
        return jsonify({"ok": True, "config": cfg})
    patch = request.get_json(silent=True) or {}
    cfg = get_config()
    keys = ("proxy_enabled", "proxy", "max_mb", "max_items",
            "nitter_instance", "nitter_instances", "twitter_bearer")
    for k in keys:
        if k in patch:
            cfg[k] = patch[k]
    save_config(cfg)
    out = dict(cfg)
    out.pop("twitter_bearer", None)
    return jsonify({"ok": True, "config": out})


@app.route("/api/scan", methods=["POST"])
def api_scan():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "请先输入链接"})
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    cfg = get_config()
    proxy = active_proxy(cfg)
    try:
        items = asyncio.run(_scan_internal(url, proxy, cfg))
    except ScanError as e:
        return jsonify({"ok": False, "error": str(e)})
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)})
    except Exception as e:
        return jsonify({"ok": False, "error": f"扫描失败: {e}"})
    return jsonify({"ok": True, "platform": items[0]["platform"] if items else "",
                    "count": len(items), "items": items})


async def _scan_internal(url, proxy, cfg):
    async with await make_session(proxy) as session:
        return await scan_url(session, url, proxy, cfg)


def _run_download_job(job_id, items):
    job = JOBS[job_id]
    try:
        asyncio.run(_process_job(job, items))
    except Exception as e:
        job["status"] = "error"
        job["current"] = f"下载失败: {e}"
    if job["status"] != "error" and job["done"] >= job["total"] and not job.get("results"):
        job["status"] = "error"


async def _process_job(job, items):
    cfg = get_config()
    proxy = active_proxy(cfg)
    max_bytes = int(cfg.get("max_mb", 200) or 200) * 1024 * 1024
    try:
        async with await make_session(proxy) as session:
            for it in items:
                job["current"] = clip(it.get("title") or it.get("source") or "", 60)
                try:
                    fname, size = await dl_item(session, it, proxy, max_bytes)
                    job["results"].append({
                        "id": it.get("id"), "title": it.get("title") or fname,
                        "type": it.get("type") or "", "file": fname, "size": size,
                        "view": f"/media/{fname}", "download": f"/media/{fname}?dl=1",
                    })
                except Exception as e:
                    job["errors"].append({
                        "id": it.get("id"), "title": clip(it.get("title") or it.get("source"), 50),
                        "error": str(e),
                    })
                job["done"] += 1
    except Exception as e:
        job["status"] = "error"
        job["current"] = f"会话错误: {e}"
        return
    job["status"] = "done"
    job["current"] = ""


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json(silent=True) or {}
    items = data.get("items") or []
    items = [it for it in items if (it.get("source") or "").startswith(("http://", "https://"))]
    if not items:
        return jsonify({"ok": False, "error": "请先勾选要下载的资源"})
    job_id = uuid.uuid4().hex
    job = {
        "id": job_id, "status": "running", "total": len(items), "done": 0,
        "current": "", "results": [], "errors": [], "created": time.time(),
    }
    with JOBS_LOCK:
        JOBS[job_id] = job
    threading.Thread(target=_run_download_job, args=(job_id, items), daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/api/progress/<job_id>")
def api_progress(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "任务不存在"})
    return jsonify({
        "ok": True,
        "status": job["status"],
        "total": job["total"],
        "done": job["done"],
        "current": job["current"],
        "results": job["results"],
        "errors": job["errors"],
    })


@app.route("/api/zip", methods=["POST"])
def api_zip():
    data = request.get_json(silent=True) or {}
    files = data.get("files") or []
    existing = []
    for f in files:
        if not f or os.path.basename(f) != f:
            continue
        p = os.path.join(DL_DIR, f)
        if os.path.isfile(p):
            existing.append(f)
    if not existing:
        return jsonify({"ok": False, "error": "没有可打包的文件"})
    zname = f"batch_{uuid.uuid4().hex[:8]}.zip"
    zpath = os.path.join(DL_DIR, zname)
    try:
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            for f in existing:
                z.write(os.path.join(DL_DIR, f), arcname=f)
    except Exception as e:
        return jsonify({"ok": False, "error": f"打包失败: {e}"})
    return jsonify({"ok": True, "file": zname, "size": os.path.getsize(zpath),
                    "download": f"/media/{zname}?dl=1"})


@app.route("/media/<path:filename>")
def media(filename):
    if os.path.basename(filename) != filename:
        return "bad request", 400
    path = os.path.join(DL_DIR, filename)
    if not os.path.isfile(path):
        return "not found", 404
    as_attachment = request.args.get("dl") == "1"
    return send_file(path, as_attachment=as_attachment)


@app.route("/api/server")
def api_server():
    return jsonify({
        "ok": True,
        "ytdlp": bool(YTDLP),
        "ffmpeg": bool(FFMPEG),
        "dl_dir": DL_DIR,
    })


if __name__ == "__main__":
    host = os.environ.get("MEDIA_HOST", "0.0.0.0")
    port = int(os.environ.get("MEDIA_PORT", "8891"))
    print(f"媒体下载 WebUI 已启动: http://127.0.0.1:{port}")
    if YTDLP:
        print(f"yt-dlp: 已找到")
    else:
        print("yt-dlp: 未安装（YouTube 功能不可用）")
    app.run(host=host, port=port, threaded=True)
