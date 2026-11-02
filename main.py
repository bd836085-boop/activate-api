"""
Free Fire Guest Account Activator API
Run:  pip install fastapi uvicorn httpx pycryptodome protobuf-decoder google-play-scraper
      python api.py
Then: POST http://0.0.0.0:8000/activate  {"uid":"...","password":"..."}
"""

# ==================== STANDARD IMPORTS ====================
import sys, asyncio, httpx, json, socket, time, os, uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from protobuf_decoder.protobuf_decoder import Parser
import MASTERTeam_pb2  # your generated protobuf module


# ==================== CONFIG ====================
TOKEN_CACHE_FILE  = "token_cache.json"
DEVICES_FILE      = "devices.json"
TOKEN_CACHE_TTL   = 3600

LOGIN_CONCURRENCY = 60
MAX_CONNECTIONS   = 1000
MAX_KEEPALIVE     = 500
HOST_RACE_TIMEOUT = 6.0

# ─── UPDATED REGIONAL HOSTS (your 9 servers + originals) ───
REGIONAL_LOGIN_HOSTS = {
    # ── user's new list ──
    "BD":     "https://clientbp.ppmainecoonghj.com",
    "BR":     "https://client.us.freefiremobile.com",
    "EUROPE": "https://clientbp.ppmainecoonghj.com",
    "ID":     "https://clientbp.ppmainecoonghj.com",
    "ME":     "https://clientbp.ppmainecoonghj.com",
    "NA":     "https://client.us.freefiremobile.com",
    "PK":     "https://clientbp.ppmainecoonghj.com",
    "TW":     "https://clientbp.ppmainecoonghj.com",
    "VN":     "https://clientbp.ppmainecoonghj.com",
    # ── original (kept for compatibility) ──
    "IND":    "https://client.ind.freefiremobile.com",
    "SG":     "https://client.sg.freefiremobile.com",
    "EU":     "https://client.eu.freefiremobile.com",
    "US":     "https://client.us.freefiremobile.com",
}

MAJORLOGIN_FALLBACK_HOSTS = [
    "https://client.ind.freefiremobile.com/",
    "https://client.bd.freefiremobile.com/",
    "https://clientbp.ppmainecoonghj.com/",
    "https://client.us.freefiremobile.com/",
    "https://client.sg.freefiremobile.com/",
    "https://client.eu.freefiremobile.com/",
    "https://client.me.freefiremobile.com/",
    "https://loginbp.ppmainecoonghj.com/",
    "https://loginbp.ggmainecoonghj.com/",
    "https://loginbp.ggblueshark.com/",
    "https://loginbp.commonmainecoonghj.com/",
    "https://indnetwork.ggblueshark.com/",
    "https://indgigateway.ggblueshark.com/",
]

AES_KEY = b'Yg&tc%DEuh6%Zc^8'
AES_IV  = b'6oyZDr22E3ychjM%'

DEV_INFO = {"made_by": "@TitanCodex", "channel": "@TitanBossCodex"}


# ==================== LOG HELPERS ====================
def log_info(msg):   print(f"[INFO]  {msg}")
def log_ok(msg):     print(f"[OK]    {msg}")
def log_err(msg):    print(f"[ERR]   {msg}")


# ==================== DEVICE RANDOMIZER ====================
_DEVICE_POOL = [
    ("Samsung", "SM-G998B",  "Adreno (TM) 660", "Android OS 12 / API-31"),
    ("Xiaomi",  "2201122G",  "Adreno (TM) 730", "Android OS 13 / API-33"),
    ("Realme",  "RMX3700",   "Mali-G710",       "Android OS 14 / API-34"),
    ("OnePlus", "CPH2451",   "Adreno (TM) 740", "Android OS 13 / API-33"),
    ("OPPO",    "CPH2611",   "Adreno (TM) 720", "Android OS 14 / API-34"),
    ("Vivo",    "V2203",     "Mali-G710",       "Android OS 12 / API-31"),
    ("Poco",    "M2102J20SG","Adreno (TM) 660", "Android OS 13 / API-33"),
]

_DEVICES_LOCK: Optional[asyncio.Lock] = None
_DEVICES_CACHE: Optional[dict] = None


def _generate_new_device() -> dict:
    brand, model, gpu, os_ver = __import__("random").choice(_DEVICE_POOL)
    rnd = __import__("random")
    return {
        "unique_device_id":  f"Google|{uuid.uuid4()}",
        "brand":             brand,
        "model":             model,
        "gpu_renderer":      gpu,
        "system_software":   os_ver,
        "screen_width":      rnd.choice([1080, 1440, 720, 1280]),
        "screen_height":     rnd.choice([2400, 3200, 1600, 2400]),
        "screen_dpi":        str(rnd.randint(300, 420)),
        "memory":            rnd.randint(2800, 6500),
        "processor_details": f"ARM64 FP ASIMD AES VMH | {rnd.randint(2200, 3200)} | {rnd.randint(6, 12)}",
        "client_ip":         f"{rnd.randint(103, 223)}.{rnd.randint(10, 250)}.{rnd.randint(10, 250)}.{rnd.randint(10, 250)}",
    }


def _load_devices_sync() -> dict:
    if not os.path.exists(DEVICES_FILE):
        return {}
    try:
        with open(DEVICES_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_devices_sync(devices: dict):
    try:
        tmp = DEVICES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(devices, f, indent=4)
        os.replace(tmp, DEVICES_FILE)
    except Exception:
        pass


async def get_device_for_account(account_identifier: str) -> dict:
    global _DEVICES_CACHE, _DEVICES_LOCK
    if _DEVICES_LOCK is None:
        _DEVICES_LOCK = asyncio.Lock()
    async with _DEVICES_LOCK:
        if _DEVICES_CACHE is None:
            loop = asyncio.get_running_loop()
            _DEVICES_CACHE = await loop.run_in_executor(None, _load_devices_sync)
        acc_key = str(account_identifier).strip()
        if acc_key in _DEVICES_CACHE:
            return _DEVICES_CACHE[acc_key]
        new_device = _generate_new_device()
        _DEVICES_CACHE[acc_key] = new_device
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_devices_sync, dict(_DEVICES_CACHE))
        return new_device


# ==================== DNS + SOCKET ====================
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_DNS_CACHE_TTL = 600.0


async def resolve_host(hostname: str) -> str:
    if not hostname:
        return hostname
    parts = hostname.split('.')
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        return hostname
    now = time.time()
    cached = _DNS_CACHE.get(hostname)
    if cached and now < cached[1]:
        return cached[0]
    try:
        loop = asyncio.get_running_loop()
        ip_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
        if ip_info:
            ip = ip_info[0][4][0]
            _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL)
            return ip
    except Exception:
        pass
    return hostname


def optimize_tcp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)
    except Exception:
        pass


# ==================== HTTP CLIENT (lifespan) ====================
client: Optional[httpx.AsyncClient] = None
_LOGIN_SEMAPHORE: Optional[asyncio.Semaphore] = None

_BASE_HEADERS = {
    'User-Agent': 'UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'Content-Type': 'application/octet-stream',
    'X-Unity-Version': '2018.4.12f1',
    'X-GA-SV': '1789535859',
    'X-GA': 'v1 1',
    'ReleaseVersion': 'OB55',
}


# ==================== TOKEN CACHE ====================
def _json_serializer(obj):
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_hex__": bytes(obj).hex()}
    raise TypeError(f"Type {type(obj)} not serializable")


def _json_deserializer(obj):
    if isinstance(obj, dict):
        if "__bytes_hex__" in obj and len(obj) == 1:
            try:
                return bytes.fromhex(obj["__bytes_hex__"])
            except Exception:
                return b""
        return {k: _json_deserializer(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_deserializer(x) for x in obj]
    return obj


_TOKEN_CACHE: Optional[Dict[str, Any]] = None
_TOKEN_CACHE_LOCK: Optional[asyncio.Lock] = None


def _load_token_cache_sync() -> Dict[str, Any]:
    if not os.path.exists(TOKEN_CACHE_FILE):
        return {}
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return {}
        data = json.loads(content)
        return _json_deserializer(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_token_cache_sync(cache: Dict[str, Any]):
    try:
        tmp = TOKEN_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, default=_json_serializer)
        os.replace(tmp, TOKEN_CACHE_FILE)
    except Exception:
        pass


async def cache_get(uid: str) -> Optional[Dict]:
    global _TOKEN_CACHE, _TOKEN_CACHE_LOCK
    if _TOKEN_CACHE_LOCK is None:
        _TOKEN_CACHE_LOCK = asyncio.Lock()
    async with _TOKEN_CACHE_LOCK:
        if _TOKEN_CACHE is None:
            loop = asyncio.get_running_loop()
            _TOKEN_CACHE = await loop.run_in_executor(None, _load_token_cache_sync)
        entry = _TOKEN_CACHE.get(str(uid))
        if not entry:
            return None
        if time.time() - entry.get("cached_at", 0) > TOKEN_CACHE_TTL:
            _TOKEN_CACHE.pop(str(uid), None)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _save_token_cache_sync, dict(_TOKEN_CACHE))
            return None
        if str(entry.get("account_id", "")).isdigit():
            entry["account_id"] = int(entry["account_id"])
        return entry


async def cache_set(uid: str, account_data: Dict):
    global _TOKEN_CACHE, _TOKEN_CACHE_LOCK
    if _TOKEN_CACHE_LOCK is None:
        _TOKEN_CACHE_LOCK = asyncio.Lock()
    async with _TOKEN_CACHE_LOCK:
        if _TOKEN_CACHE is None:
            _TOKEN_CACHE = {}
        entry = dict(account_data)
        entry["cached_at"] = time.time()
        _TOKEN_CACHE[str(uid)] = entry
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_token_cache_sync, dict(_TOKEN_CACHE))


# ==================== AES ====================
async def aes_encrypt(payload: bytes, key: bytes, iv: bytes) -> bytes:
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(payload, AES.block_size))


_VERSION_CONFIG_CACHE = None
_VERSION_CONFIG_CACHE_TIME = 0.0
_VERSION_CONFIG_TTL = 1800.0
_VERSION_LOCK: Optional[asyncio.Lock] = None


async def get_playstore_version():
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: play_scraper('com.dts.freefireth', lang='hi', country='id')
        )
        return result.get("version")
    except Exception:
        return "1.132.9"


async def version_config(region: str = "IND"):
    global _VERSION_CONFIG_CACHE, _VERSION_CONFIG_CACHE_TIME, _VERSION_LOCK
    if _VERSION_LOCK is None:
        _VERSION_LOCK = asyncio.Lock()
    now = time.time()
    if _VERSION_CONFIG_CACHE and (now - _VERSION_CONFIG_CACHE_TIME) < _VERSION_CONFIG_TTL:
        return _VERSION_CONFIG_CACHE
    async with _VERSION_LOCK:
        now = time.time()
        if _VERSION_CONFIG_CACHE and (now - _VERSION_CONFIG_CACHE_TIME) < _VERSION_CONFIG_TTL:
            return _VERSION_CONFIG_CACHE
        reg = str(region).upper() if region else "IND"
        regional_url = REGIONAL_LOGIN_HOSTS.get(reg, REGIONAL_LOGIN_HOSTS["IND"])
        FALLBACK = ("OB55", "1.132.9", regional_url)
        try:
            app_version = await get_playstore_version() or "1.132.9"
            api_url = (
                "https://version.ggwhitehawk.com/live/ver.php"
                f"?version={app_version}"
                "&lang=hi&device=android&channel=android"
                "&appstore=googleplay&region=IND"
                "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
            )
            response = await client.get(api_url, timeout=6.0)
            if response.status_code == 200:
                data = response.json()
                server_url = data.get("server_url") or regional_url
                remote_version = data.get("remote_version")
                latest_release_version = data.get("latest_release_version")
                if server_url and remote_version and latest_release_version:
                    _VERSION_CONFIG_CACHE = (latest_release_version, remote_version, server_url)
                    _VERSION_CONFIG_CACHE_TIME = now
                    return _VERSION_CONFIG_CACHE
        except Exception:
            pass
        _VERSION_CONFIG_CACHE = FALLBACK
        _VERSION_CONFIG_CACHE_TIME = now
        return _VERSION_CONFIG_CACHE


async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    hdrs = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B Build/SP1A.210812.016)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
    }
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067",
    }
    for _ in range(3):
        try:
            response = await client.post(url, headers=hdrs, data=data)
            if response.status_code == 200:
                rd = response.json()
                open_id = rd.get("open_id")
                access_token = rd.get("access_token")
                platform = rd.get("platform", 4)
                if open_id and access_token:
                    return open_id, access_token, platform
            if response.status_code == 429:
                await asyncio.sleep(0.8)
                continue
        except Exception:
            pass
        await asyncio.sleep(0.3)
    return None


# ==================== PROTOBUF ====================
def encode_varint(n: int) -> bytes:
    if n < 0:
        return b''
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


def build_proto(fields: dict) -> bytes:
    parts = []
    for k, v in fields.items():
        if isinstance(v, dict):
            nested = build_proto(v)
            parts.append(encode_varint((k << 3) | 2) + encode_varint(len(nested)) + nested)
        elif isinstance(v, int):
            parts.append(encode_varint((k << 3) | 0) + encode_varint(v))
        elif isinstance(v, (str, bytes)):
            ev = v.encode() if isinstance(v, str) else v
            parts.append(encode_varint((k << 3) | 2) + encode_varint(len(ev)) + ev)
    return b''.join(parts)


def encrypt_api_bytes(plain_bytes: bytes) -> bytes:
    return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(
        pad(plain_bytes, AES.block_size)
    )


async def build_majorlogin_payload(open_id, access_token, platform, client_version, device_info, region="IND"):
    try:
        payload_dict = {
            3:  str(datetime.now())[:-7],
            4:  "free fire",
            5:  "1",
            7:  str(client_version) if client_version else "1.132.8",
            8:  str(device_info.get("system_software", "Android OS 15 / API-35")),
            9:  "Handheld",
            10: "Telkomsel",
            11: "CarrierDataNetwork",
            12: str(device_info.get("screen_width", 1639)),
            13: str(device_info.get("screen_height", 720)),
            14: str(device_info.get("screen_dpi", 320)),
            15: str(device_info.get("processor_details", "ARM64 FP ASIMD AES | 2000 | 8")),
            16: str(device_info.get("memory", 5653)),
            17: str(device_info.get("gpu_renderer", "Mali-G52 MC2")),
            18: "OpenGL ES 3.2 v1.r49p1-03bet0.19498e0ae1d5dac223383c39a2e58f04",
            19: str(device_info.get("unique_device_id", f"Google|{uuid.uuid4()}")),
            20: str(device_info.get("client_ip", "182.5.135.225")),
            21: "en",
            22: str(open_id),
            23: str(platform),
            24: "Handheld",
            25: str(device_info.get("model", "Xiaomi 2409BRN2CY")),
            26: region.upper(),
            29: str(access_token),
            30: "1",
            41: "Telkomsel",
            42: "4G",
            57: "7428b253defc164018c604a1ebbfebdf",
            60: "109278", 61: "55402", 62: "868", 64: "55927",
            65: "109278", 66: "55927", 67: "109278", 73: "2",
            74: "/data/app/~~6Cw028g_ld-8Z0z2qhrOLA==/com.dts.freefireth-i3wiaEmpx8CbiRVhUMx_ag==/lib/arm64",
            76: "1",
            77: "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/~~6Cw028g_ld-8Z0z2qhrOLA==/com.dts.freefireth-i3wiaEmpx8CbiRVhUMx_ag==/base.apk",
            78: "3", 79: "2", 81: "64", 83: "2019121229",
            85: "3", 86: "OpenGLES2", 87: "8191", 88: str(platform),
            90: "Cirebon", 91: "JB", 92: "14466", 93: "android",
            94: "KqsHTyb7yHjsAiyNWUUZa0DSOD6U0AhDQ76smAo1WeHHHpSmJ8KMRyRrxb2ttgJqO9YOqO/K6AORAvDMAX/qBS8+JahIkrM7kHfzlfULbgRAoZ9r",
            95: "111207",
            96: "{\"cur_rate\":[60,90,120],\"support_etc2\":false}",
            97: "1", 98: "1", 99: str(platform), 100: str(platform),
            102: "D]C\x15\x05T\x0eU3", 103: "1", 104: "52837", 105: "1",
            106: "https://dl.gmc.freefiremobile.com/live/ABHotUpdates/|https://core-gmc.freefiremobile.com/live/ABHotUpdates/|211c933168f55902c7dfbfd8c4e2957d",
            107: "1.3fb8adbf45f44ce9",
        }
        return encrypt_api_bytes(build_proto(payload_dict))
    except Exception:
        return None


# ==================== MAJORLOGIN ====================
async def _try_majorlogin(base: str, data: bytes, release_version: str):
    url = f"{base.rstrip('/')}/MajorLogin"
    h = _BASE_HEADERS.copy()
    h["ReleaseVersion"] = release_version
    try:
        r = await client.post(url, headers=h, content=bytes(data))
        if r.status_code != 200:
            return None
        rc = r.content
        if len(rc) < 20:
            return None
        for offset in [0] + ([64] if len(rc) > 64 else []) + list(range(1, min(160, len(rc)))):
            try:
                p = MASTERTeam_pb2.MajorLoginRes()
                p.ParseFromString(rc[offset:])
                if p.region and p.token:
                    return p
            except Exception:
                continue
    except Exception:
        return None
    return None


async def send_majorlogin(data: bytes, release_version: str, server_url: Optional[str]):
    if not data:
        return None
    candidates: List[str] = []
    if server_url:
        candidates.append(server_url.rstrip('/') + "/")
    for h in MAJORLOGIN_FALLBACK_HOSTS:
        n = h.rstrip('/') + "/"
        if n not in candidates:
            candidates.append(n)
    tasks = [asyncio.create_task(_try_majorlogin(b, data, release_version)) for b in candidates]
    try:
        done, pending = await asyncio.wait(tasks, timeout=HOST_RACE_TIMEOUT,
                                           return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            try:
                r = t.result()
                if r is not None:
                    for p in pending: p.cancel()
                    return r
            except Exception:
                continue
        for t in pending:
            try:
                r = await t
                if r is not None: return r
            except Exception:
                continue
    finally:
        for t in tasks:
            if not t.done(): t.cancel()
    return None


# ==================== GETLOGIN ====================
async def _try_getlogin(base: str, data: bytes, token: str, release_version: str):
    url = f"{base.rstrip('/')}/GetLoginData"
    h = _BASE_HEADERS.copy()
    h["ReleaseVersion"] = release_version
    h["Authorization"] = f"Bearer {token}"
    try:
        from urllib.parse import urlparse
        h["Host"] = urlparse(base).netloc
    except Exception:
        pass
    try:
        r = await client.post(url, headers=h, content=bytes(data))
        if r.status_code != 200:
            return None
        rc = r.content
        res_proto = MASTERTeam_pb2.GetLoginDataRes()
        parsed = False
        try:
            res_proto.ParseFromString(rc)
            if res_proto.functional_addrs or res_proto.informational_addrs:
                parsed = True
        except Exception:
            pass
        if not parsed:
            for offset in range(min(160, len(rc))):
                try:
                    c = MASTERTeam_pb2.GetLoginDataRes()
                    c.ParseFromString(rc[offset:])
                    if c.functional_addrs or c.informational_addrs:
                        res_proto, parsed = c, True
                        break
                except Exception:
                    continue
        if not parsed:
            return None
        dict_res = {}
        try:
            for item in Parser().parse(rc.hex()):
                if item.wire_type in ("varint", "string", "bytes"):
                    dict_res[str(item.field)] = {"data": item.data}
        except Exception:
            pass
        return res_proto, dict_res
    except Exception:
        return None


async def send_getlogin(data: bytes, base_url: Optional[str], token: str,
                        release_version: str, region: str = "IND"):
    if not data:
        return None
    reg = str(region).upper() if region else "IND"
    candidates: List[str] = []
    regional = REGIONAL_LOGIN_HOSTS.get(reg)
    if regional:
        candidates.append(regional.rstrip('/') + "/")
    if base_url:
        candidates.append(base_url.rstrip('/') + "/")
    for h in MAJORLOGIN_FALLBACK_HOSTS:
        n = h.rstrip('/') + "/"
        if n not in candidates:
            candidates.append(n)
    tasks = [asyncio.create_task(_try_getlogin(b, data, token, release_version)) for b in candidates]
    try:
        done, pending = await asyncio.wait(tasks, timeout=HOST_RACE_TIMEOUT,
                                           return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            try:
                r = t.result()
                if r is not None:
                    for p in pending: p.cancel()
                    return r
            except Exception:
                continue
        for t in pending:
            try:
                r = await t
                if r is not None: return r
            except Exception:
                continue
    finally:
        for t in tasks:
            if not t.done(): t.cancel()
    return None


# ==================== TCP PACKET ====================
async def build_tcp_startup_packet(account_id, token, server_time, key, iv,
                                    region="BD", typ='OnLine'):
    uid_hex = f"{int(account_id):016x}"
    ts_hex  = f"{int(server_time):08x}"
    enc_pkt = (await aes_encrypt(token.encode(), key, iv)).hex()
    enc_len = f"{len(enc_pkt) // 2:08x}"
    reg = str(region).upper() if region else "BD"

    if typ == 'OnLine':
        if reg == 'BD':       prefix = '7219'
        elif reg in ('IND','ID'): prefix = '7214'
        else:                  prefix = '7215'
        return f"{prefix}{uid_hex}{ts_hex}00000000{enc_len}{enc_pkt}"
    else:
        if reg == 'BD':       prefix = '8119'
        elif reg in ('IND','ID'): prefix = '8114'
        else:                  prefix = '8115'
        return f"{prefix}{uid_hex}{ts_hex}{enc_len}{enc_pkt}"


_KEEPALIVE_CACHE: Dict[str, bytes] = {}


async def send_keep_alive(region="BD") -> bytes:
    reg = str(region).upper() if region else "BD"
    if reg in _KEEPALIVE_CACHE:
        return _KEEPALIVE_CACHE[reg]
    if reg == "BD":            ka_hex = "0219"
    elif reg in ("IND","ID"):  ka_hex = "0214"
    else:                      ka_hex = "0215"
    try:
        data = bytes.fromhex(ka_hex)
    except Exception:
        data = bytes.fromhex("0219")
    _KEEPALIVE_CACHE[reg] = data
    return data


def get_proto_field(d, key, default=None):
    if not d or not isinstance(d, dict):
        return default
    if key in d:
        v = d[key].get('data')
        return v if v is not None else default
    if str(key) in d:
        v = d[str(key)].get('data')
        return v if v is not None else default
    return default


# ==================== LOGIN / ACTIVATION CORE ====================
async def login_account(uid: str, password: str) -> Dict:
    cached = await cache_get(str(uid))
    if cached:
        return cached

    async with _LOGIN_SEMAPHORE:
        release_version, client_version, server_url = await version_config(region="IND")

        tokengrant = await get_access_token(uid, password)
        if tokengrant is None:
            raise Exception("token grant failed (invalid uid/password)")
        open_id, access_token, platform = tokengrant

        device_info = await get_device_for_account(uid)
        payload = await build_majorlogin_payload(
            open_id, access_token, platform, client_version, device_info, region="IND"
        )
        majorlogin = await send_majorlogin(payload, release_version, server_url)
        if majorlogin is None:
            raise Exception("MajorLogin failed (all hosts)")

        getlogin = await send_getlogin(
            payload, majorlogin.url, majorlogin.token,
            release_version, region=majorlogin.region or "IND"
        )
        if getlogin is None:
            raise Exception("GetLoginData failed (all hosts)")
        res_proto, dict_res = getlogin

        level    = int(get_proto_field(dict_res, 6, 1))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{majorlogin.account_id}")
        region   = majorlogin.region or get_proto_field(dict_res, 3, "IND")
        if isinstance(region, bytes):
            region = region.decode('utf-8', errors='ignore')
        region = str(region).upper()

        account_data = {
            'account_id':        majorlogin.account_id,
            'nickname':          nickname,
            'region':            region,
            'level':             level,
            'open_id':           open_id,
            'access_token':      access_token,
            'platform':          str(platform),
            'token':             majorlogin.token,
            'server_time':       majorlogin.server_time,
            'aes_ak':            majorlogin.aes_ak,
            'iv_i':              majorlogin.iv_i,
            'functional_addrs':  res_proto.functional_addrs,
            'informational_addrs': res_proto.informational_addrs,
            'release_version':   release_version,
            'client_version':    client_version,
            'server_url':        majorlogin.url,
            'login_payload_data': payload,
            'auth_type':         'guest',
            'auth_uid':          uid,
            'auth_password':     password,
        }
        await cache_set(str(uid), account_data)
        return account_data


async def do_tcp_activation(account_data: Dict, online_duration: float = 3.0) -> bool:
    acc_id = str(account_data['account_id'])
    reg    = str(account_data.get('region', 'IND')).upper()

    tcp_packet = await build_tcp_startup_packet(
        account_data['account_id'],
        account_data['token'],
        account_data['server_time'],
        account_data['aes_ak'],
        account_data['iv_i'],
        region=reg, typ='OnLine',
    )

    addrs = account_data['functional_addrs']
    if ":" in addrs:
        ip, port = addrs.split(":")
    else:
        ip, port = addrs, "10001"

    resolved_ip = await resolve_host(ip)
    reader = writer = None
    last_err = None

    # try resolved direct
    for _ in range(3):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(resolved_ip, int(port)), timeout=5.0)
            break
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.5)

    # fallback via regional host
    if writer is None:
        try:
            reg_host = REGIONAL_LOGIN_HOSTS.get(reg, REGIONAL_LOGIN_HOSTS["IND"])
            host_only = reg_host.replace("https://", "").replace("http://", "").strip("/")
            fb_ip = await resolve_host(host_only)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(fb_ip, int(port)), timeout=5.0)
        except Exception as e:
            last_err = e

    if writer is None:
        raise Exception(f"TCP connect failed: {last_err}")

    raw_sock = writer.get_extra_info('socket')
    if raw_sock:
        optimize_tcp_socket(raw_sock)

    writer.write(bytes.fromhex(tcp_packet))
    await writer.drain()

    try:
        ka = await send_keep_alive(reg)
        if ka and not writer.is_closing():
            writer.write(ka)
            await asyncio.wait_for(writer.drain(), timeout=1.5)
    except Exception:
        pass

    # stay online
    start_time = time.time()
    keepalive  = await send_keep_alive(reg)
    while time.time() - start_time < online_duration:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=0.3)
            if not data:
                break
        except asyncio.TimeoutError:
            try:
                if writer and not writer.is_closing():
                    writer.write(keepalive)
                    await writer.drain()
            except Exception:
                break
        except Exception:
            break

    try:
        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=0.8)
    except Exception:
        pass
    return True


# ==================== FASTAPI APP ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, _LOGIN_SEMAPHORE
    client = httpx.AsyncClient(
        verify=False,
        timeout=httpx.Timeout(15.0, connect=6.0),
        limits=httpx.Limits(
            max_connections=MAX_CONNECTIONS,
            max_keepalive_connections=MAX_KEEPALIVE,
        ),
        follow_redirects=True,
        http2=True,
    )
    _LOGIN_SEMAPHORE = asyncio.Semaphore(LOGIN_CONCURRENCY)
    log_info("API started — client + semaphore ready")
    yield
    await client.aclose()
    log_info("API shutdown — client closed")


app = FastAPI(
    title="Free Fire Account Activator API",
    description="Activates FF guest accounts (uid + password) via regional servers.",
    version="1.0.0",
    lifespan=lifespan,
)


class ActivateRequest(BaseModel):
    uid: str
    password: str
    online_duration: float = 3.0     # optional, seconds


def _err(message: str) -> dict:
    return {
        "status": "error",
        "message": f"❌ {message}",
        "account_id": None,
        "account_name": None,
        "region": None,
        "activated": False,
        "dev": DEV_INFO,
    }


@app.get("/")
def root():
    return {
        "api": "Free Fire Account Activator",
        "version": "1.0.0",
        "endpoint": "POST /activate  →  {uid, password, [online_duration]}",
        "regions_supported": list(REGIONAL_LOGIN_HOSTS.keys()),
        "dev": DEV_INFO,
    }


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.post("/activate")
async def activate(req: ActivateRequest):
    uid = (req.uid or "").strip()
    pwd = (req.password or "").strip()
    if not uid or not pwd:
        return _err("uid and password are required")

    duration = min(max(float(req.online_duration or 3.0), 0.5), 30.0)

    # 1) Login
    try:
        acc = await login_account(uid, pwd)
    except Exception as e:
        log_err(f"login {uid}: {e}")
        return _err(f"Login failed: {e}")

    # 2) TCP activation
    try:
        await do_tcp_activation(acc, online_duration=duration)
    except Exception as e:
        log_err(f"tcp {uid}: {e}")
        return _err(f"TCP activation failed: {e}")

    log_ok(f"activated {acc['account_id']} ({acc.get('region')})")
    return {
        "status": "success",
        "message": "✅ Account Activated Successfully",
        "account_id": int(acc['account_id']),
        "account_name": acc.get('nickname', f"Player_{acc['account_id']}"),
        "region": acc.get('region', 'IND'),
        "activated": True,
        "dev": DEV_INFO,
    }


# ==================== MAIN ====================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")