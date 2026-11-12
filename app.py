import asyncio
import collections
import re
import ssl
import string
import urllib.parse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
import datetime
import grpc
import requests
import io
import base64
import uuid
import time
import qrcode

APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, APP_DIR)

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn

import stream_list_pb2
import stream_list_pb2_grpc

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bilibili_api import (
    Credential,
    Danmaku,
    login_v2,
    get_registered_clients,
    get_selected_client,
    select_client,
    register_client,
)
from bilibili_api.exceptions import NetworkException, ResponseCodeException
from bilibili_api.utils.network import get_buvid, Api
from bilibili_api.live import LiveRoom, API as LIVE_API

app = FastAPI()

APP_VERSION = "v2.1.0"
GITHUB_REPO = "TZFC/Youtube-Twitch-Livechat-To-Bilibili"
CONFIG_FILE = "config.json"

BILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "https://www.bilibili.com/",
}

default_config = {
    "SESSDATA": "",
    "BILI_JCT": "",
    "BUVID3": "",
    "BUVID4": "",
    "DEDEUSERID": "",
    "AC_TIME_VALUE": "",
    "BILIBILI_ROOM_ID": 23596840,
    "YOUTUBE_CHANNEL_HANDLE": "@沐晓空",
    "TWITCH_CHANNEL": "muxiaokong",
    "YOUTUBE_API_KEY": "",
    "OUTPUT_LANGUAGE": "en",
    "INCLUDE_USERNAME": True,
    "BANNED_WORDS": "",
    "YOUTUBE_POLL_BASE_DELAY_SECONDS": "2",
    "YOUTUBE_POLL_MULTIPLIER": "2",
    "YOUTUBE_MAX_POLL_DELAY_SECONDS": "30"
}

config = default_config.copy()

def load_config():
    global config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                config.update(loaded)
        except:
            pass

def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

load_config()

def ensure_bilibili_http_client() -> tuple[bool, str]:
    """Ensure bilibili_api has a registered and selected async HTTP client."""
    try:
        client_name, _ = get_selected_client()
        return True, client_name
    except Exception:
        pass

    try:
        registered = get_registered_clients()
        for preferred in ("curl_cffi", "aiohttp", "httpx"):
            if preferred in registered:
                select_client(preferred)
                return True, preferred

        import importlib
        from bilibili_api.clients import ALL_PROVIDED_CLIENTS
        for module_name, client_cls_name, settings in ALL_PROVIDED_CLIENTS:
            try:
                importlib.import_module(module_name)
                client_mod = importlib.import_module(f"bilibili_api.clients.{client_cls_name}")
                client_cls = getattr(client_mod, client_cls_name)
                register_client(module_name, client_cls, settings)
                return True, module_name
            except Exception:
                continue
    except Exception as e:
        return False, f"Failed to initialize Bilibili HTTP client: {e}"

    return False, "No Bilibili HTTP client library available (please install curl_cffi)"

def build_bilibili_credential() -> Credential:
    return Credential(
        sessdata=config.get("SESSDATA", ""),
        bili_jct=config.get("BILI_JCT", ""),
        buvid3=config.get("BUVID3", ""),
        buvid4=config.get("BUVID4", ""),
        dedeuserid=str(config.get("DEDEUSERID", "")) if config.get("DEDEUSERID") else None,
        ac_time_value=config.get("AC_TIME_VALUE", ""),
    )

async def check_and_refresh_bilibili_credentials(force: bool = False) -> dict:
    global config
    if not config.get("SESSDATA"):
        return {"status": "no_credentials", "message": "No SESSDATA configured", "valid": False, "needs_refresh": False}

    client_ok, client_info = ensure_bilibili_http_client()
    if not client_ok:
        msg = f"Failed to verify/refresh Bilibili cookies: {client_info}"
        add_log(msg)
        return {"status": "error", "message": msg, "valid": False, "needs_refresh": False}

    cred = build_bilibili_credential()

    is_valid = False
    check_valid_error = None
    try:
        is_valid = await cred.check_valid()
    except Exception as e:
        check_valid_error = e

    if check_valid_error is not None:
        msg = f"Failed to verify Bilibili cookies: {check_valid_error}"
        add_log(msg)
        return {"status": "error", "message": msg, "valid": False, "needs_refresh": False}

    needs_refresh = False
    try:
        needs_refresh = await cred.check_refresh()
    except Exception:
        needs_refresh = False

    should_refresh = force or needs_refresh or (not is_valid and bool(config.get("AC_TIME_VALUE")))

    if should_refresh:
        if not config.get("AC_TIME_VALUE"):
            if force and is_valid and not needs_refresh:
                return {
                    "status": "ok",
                    "valid": True,
                    "needs_refresh": False,
                    "message": "Cookies are valid (no refresh needed; AC_TIME_VALUE not configured)"
                }
            msg = "Bilibili cookies need refresh, but no AC_TIME_VALUE (refresh token) is present. Please re-login via QR code."
            if not is_valid or needs_refresh:
                add_log(msg)
            return {"status": "missing_token", "message": msg, "valid": is_valid, "needs_refresh": needs_refresh}

        try:
            add_log("Attempting Bilibili cookie refresh with AC_TIME_VALUE...")
            await cred.refresh()
            config["SESSDATA"] = cred.sessdata or ""
            config["BILI_JCT"] = cred.bili_jct or ""
            config["DEDEUSERID"] = str(cred.dedeuserid or "")
            config["AC_TIME_VALUE"] = cred.ac_time_value or ""
            save_config()
            add_log("Bilibili cookies successfully refreshed and updated in config.json!")
            return {"status": "refreshed", "message": "Cookies refreshed successfully", "valid": True, "needs_refresh": False}
        except Exception as e:
            msg = f"Failed to refresh Bilibili cookies: {e}. Please re-login via QR code."
            add_log(msg)
            return {"status": "error", "message": msg, "valid": is_valid, "needs_refresh": needs_refresh}

    return {
        "status": "ok" if is_valid else "invalid",
        "valid": is_valid,
        "needs_refresh": needs_refresh,
        "message": "Cookies are valid" if is_valid else "Cookies invalid or expired"
    }

async def bilibili_cookie_keeper_task():
    await asyncio.sleep(5)
    while True:
        try:
            if config.get("SESSDATA") and config.get("AC_TIME_VALUE"):
                await check_and_refresh_bilibili_credentials(force=False)
        except Exception as e:
            add_log(f"Cookie keeper error: {e}")
        await asyncio.sleep(6 * 3600)

def add_log(msg: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {msg}"
    print(log_line)
    try:
        with open("log.txt", "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception:
        pass

# Global state
shutdown_event = None
bridge_task = None
active_grpc_call = None

bilibili_live_room = None
bilibili_real_room_id = None
youtube_api_client = None

twitch_privmsg_pattern = re.compile(r"^(?:@([^\s]+)\s+)?:(\w+)!.*?PRIVMSG.*? :(.*)$")
twitch_usernotice_pattern = re.compile(r"^(?:@([^\s]+)\s+)?:tmi\.twitch\.tv\s+USERNOTICE\s+#[^\s]+(?:\s+:(.*))?$")
youtube_colon_emote_pattern = re.compile(r":[^:\s]{1,64}:")
youtube_bracket_emote_pattern = re.compile(r"\[[^\[\]\n]{1,64}\]")

def parse_twitch_tags(tags_string):
    tags = {}
    if not tags_string:
        return tags
    for tag in tags_string.split(";"):
        if "=" in tag:
            k, v = tag.split("=", 1)
            tags[k] = v
    return tags

async def wait_until_timeout_or_shutdown_event(timeout_duration_seconds):
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=timeout_duration_seconds)
    except asyncio.TimeoutError:
        pass

def is_text_empty_or_only_punctuation(text_to_check):
    if not text_to_check:
        return True
    return all(character in string.punctuation or character.isspace() for character in text_to_check)

def remove_twitch_emotes_from_message(raw_chat_message, twitch_tags_string):
    if not twitch_tags_string:
        return raw_chat_message

    extracted_emotes_string = ""
    for tag in twitch_tags_string.split(";"):
        if tag.startswith("emotes="):
            extracted_emotes_string = tag[7:]
            break

    if not extracted_emotes_string:
        return raw_chat_message

    emote_index_ranges = []
    for emote_data in extracted_emotes_string.split("/"):
        if ":" not in emote_data:
            continue

        emote_positions = emote_data.split(":")[1]
        for position_range in emote_positions.split(","):
            start_index, end_index = position_range.split("-")
            emote_index_ranges.append((int(start_index), int(end_index)))

    emote_index_ranges.sort(key=lambda index_pair: index_pair[0], reverse=True)
    message_characters_list = list(raw_chat_message)

    for start_index, end_index in emote_index_ranges:
        del message_characters_list[start_index:end_index + 1]

    return "".join(message_characters_list).strip()

def clean_whitespace_and_youtube_emotes_from_message(message_text):
    message_without_colon_emotes = youtube_colon_emote_pattern.sub(" ", message_text)
    message_without_bracket_emotes = youtube_bracket_emote_pattern.sub(" ", message_without_colon_emotes)
    return re.sub(r"\s+", " ", message_without_bracket_emotes).strip()

def clean_whitespace_from_message(message_text):
    return re.sub(r"\s+", " ", message_text).strip()

def format_message_with_username_and_truncate(message_content, author_username, username_prefix_separator, maximum_allowed_length=40):
    include_user = config.get("INCLUDE_USERNAME", True)
    if isinstance(include_user, str):
        include_user = include_user.lower() == "true"
        
    if include_user:
        clean_username_without_prefix = author_username.lstrip(username_prefix_separator)
        combined_message_and_username = f"{message_content} {username_prefix_separator}{clean_username_without_prefix}"
    else:
        combined_message_and_username = message_content
        
    if len(combined_message_and_username) > maximum_allowed_length:
        final_msg = combined_message_and_username[: maximum_allowed_length - 1] + "…"
    else:
        final_msg = combined_message_and_username

    # Banned words check
    banned_words_raw = config.get("BANNED_WORDS", "")
    if banned_words_raw:
        # Clean all whitespace from the final message for comparison
        clean_final_msg = "".join(final_msg.split()).lower()
        
        # Split banned words supporting both English and Chinese commas, strip all whitespaces from each word, and check
        normalized_banned_words = banned_words_raw.replace("，", ",")
        for raw_word in normalized_banned_words.split(","):
            clean_word = "".join(raw_word.split()).lower()
            if clean_word and clean_word in clean_final_msg:
                return None # Comment will not be forwarded
                
    return final_msg

channel_id_cache = {}

def get_youtube_channel_id_from_handle(channel_handle):
    clean_handle = channel_handle.strip()
    if not clean_handle:
        return None
    if clean_handle.startswith("UC") and len(clean_handle) == 24:
        return clean_handle
    if clean_handle in channel_id_cache:
        return channel_id_cache[clean_handle]

    normalized_channel_handle = clean_handle
    if normalized_channel_handle.startswith("@"):
        normalized_channel_handle = normalized_channel_handle[1:]

    if not youtube_api_client:
        return None

    try:
        youtube_api_response = youtube_api_client.channels().list(
            part="id",
            forHandle=normalized_channel_handle,
            maxResults=1,
        ).execute()

        response_items_list = youtube_api_response.get("items", [])
        if not response_items_list:
            raise RuntimeError(f"Could not resolve YouTube handle: @{normalized_channel_handle}")

        cid = response_items_list[0]["id"]
        channel_id_cache[clean_handle] = cid
        return cid
    except Exception as e:
        add_log(f"Error resolving YouTube channel handle: {e}")
        return None

def _parse_youtube_live_chat_item(item):
    video_id = item.get("id")
    snippet = item.get("snippet", {})
    details = item.get("liveStreamingDetails", {})

    broadcast_content = snippet.get("liveBroadcastContent", "none")
    active_live_chat_id = details.get("activeLiveChatId")
    actual_end_time = details.get("actualEndTime")

    # Must be an active or upcoming broadcast with an available live chat ID and not already ended
    if broadcast_content in ("live", "upcoming") and active_live_chat_id and not actual_end_time:
        return {
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "chat_id": active_live_chat_id,
            "status": broadcast_content
        }
    return None

def verify_youtube_live_chat_status_batch(video_ids):
    unique_ids = [vid for vid in dict.fromkeys(video_ids) if vid]
    if not unique_ids or not youtube_api_client:
        return None
    try:
        res = youtube_api_client.videos().list(
            part="snippet,liveStreamingDetails",
            id=",".join(unique_ids[:50]),
        ).execute()
        items_by_id = {item.get("id"): item for item in res.get("items", []) if item.get("id")}
        for vid in unique_ids:
            item = items_by_id.get(vid)
            if not item:
                continue
            verified = _parse_youtube_live_chat_item(item)
            if verified:
                return verified
    except Exception as e:
        add_log(f"Error checking YouTube videos {unique_ids} live status: {e}")
    return None

def verify_youtube_live_chat_status(video_id):
    return verify_youtube_live_chat_status_batch([video_id])

def get_current_live_video_info_from_youtube_channel(channel_handle):
    """Discover an active or upcoming live stream via official YouTube Data API v3 only (no HTML scraping)."""
    if not youtube_api_client or not channel_handle:
        return None

    try:
        resolved_channel_id = get_youtube_channel_id_from_handle(channel_handle)
        if not resolved_channel_id or not resolved_channel_id.startswith("UC"):
            return None

        uploads_playlist_id = "UU" + resolved_channel_id[2:]
        playlist_response = youtube_api_client.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=10,
        ).execute()

        playlist_vids = [
            item.get("contentDetails", {}).get("videoId")
            for item in playlist_response.get("items", [])
            if item.get("contentDetails", {}).get("videoId")
        ]
        if playlist_vids:
            return verify_youtube_live_chat_status_batch(playlist_vids)
    except Exception as e:
        add_log(f"[YouTube] Error checking channel uploads for live stream: {e}")

    return None

def wait_in_thread_for_shutdown(timeout_seconds):
    """Sleep up to timeout_seconds while checking asyncio.Event shutdown_event thread-safely."""
    deadline = time.monotonic() + timeout_seconds
    while not shutdown_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.25, remaining))
    return True

def _get_youtube_poll_backoff_settings():
    """Read YouTube idle-chat exponential backoff settings from config."""
    try:
        base_delay = max(0.5, float(config.get("YOUTUBE_POLL_BASE_DELAY_SECONDS", 2.0)))
    except (TypeError, ValueError):
        base_delay = 2.0
    try:
        multiplier = max(1.0, float(config.get("YOUTUBE_POLL_MULTIPLIER", 2.0)))
    except (TypeError, ValueError):
        multiplier = 2.0
    try:
        max_delay = max(base_delay, float(config.get("YOUTUBE_MAX_POLL_DELAY_SECONDS", 30.0)))
    except (TypeError, ValueError):
        max_delay = max(base_delay, 30.0)
    return base_delay, multiplier, max_delay

def run_grpc_stream_with_reconnect(active_live_chat_id, api_key, chat_message_queue, main_loop):
    global active_grpc_call
    creds = grpc.ssl_channel_credentials()
    grpc_channel_options = [
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", True),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.http2.min_time_between_pings_ms", 10000),
        ("grpc.http2.min_ping_interval_without_data_ms", 5000),
    ]

    metadata = (("x-goog-api-key", api_key),)
    next_page_token = None
    is_initial_chat_poll = True
    previously_seen_message_ids_set = set()
    previously_seen_message_ids_queue = collections.deque(maxlen=3000)

    base_poll_delay, poll_multiplier, max_poll_delay = _get_youtube_poll_backoff_settings()
    idle_poll_delay_seconds = base_poll_delay
    error_retry_delay_seconds = base_poll_delay

    def track_seen_message_id(msg_id):
        if not msg_id or msg_id in previously_seen_message_ids_set:
            return False
        if len(previously_seen_message_ids_queue) == 3000:
            oldest_tracked_message_id = previously_seen_message_ids_queue.popleft()
            previously_seen_message_ids_set.discard(oldest_tracked_message_id)
        previously_seen_message_ids_queue.append(msg_id)
        previously_seen_message_ids_set.add(msg_id)
        return True

    def enqueue_msg(tag, msg):
        if not msg or shutdown_event.is_set():
            return
        try:
            main_loop.call_soon_threadsafe(chat_message_queue.put_nowait, (tag, msg))
        except Exception as e:
            add_log(f"Failed to queue YouTube message: {e}")

    while not shutdown_event.is_set():
        try:
            with grpc.secure_channel("dns:///youtube.googleapis.com:443", creds, options=grpc_channel_options) as channel:
                stub = stream_list_pb2_grpc.V3DataLiveChatMessageServiceStub(channel)

                while not shutdown_event.is_set():
                    request_kwargs = {
                        "part": ["id", "snippet", "authorDetails"],
                        "live_chat_id": active_live_chat_id,
                        "max_results": 200,
                    }
                    if next_page_token:
                        request_kwargs["page_token"] = next_page_token
                    request = stream_list_pb2.LiveChatMessageListRequest(**request_kwargs)

                    should_recreate_channel = False
                    saw_initial_poll_this_call = False
                    saw_new_chat_items = False
                    next_wait_seconds = error_retry_delay_seconds
                    is_error_retry = False

                    try:
                        stream_call = stub.StreamList(request, metadata=metadata)
                        active_grpc_call = stream_call
                        for response in stream_call:
                            if shutdown_event.is_set():
                                return "shutdown"

                            error_retry_delay_seconds = base_poll_delay  # Connection is healthy

                            if is_initial_chat_poll:
                                for item in response.items:
                                    track_seen_message_id(item.id)
                                is_initial_chat_poll = False
                                saw_initial_poll_this_call = True
                                if response.next_page_token:
                                    next_page_token = response.next_page_token
                                continue

                            has_live_stream_ended = False
                            for item in response.items:
                                if not track_seen_message_id(item.id):
                                    continue

                                event_type = item.snippet.type

                                # Protobuf enums: CHAT_ENDED_EVENT is 4
                                if event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.CHAT_ENDED_EVENT:
                                    has_live_stream_ended = True
                                    break

                                saw_new_chat_items = True
                                author_display_name = item.author_details.display_name if item.HasField("author_details") else "unknown"

                                if event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.TEXT_MESSAGE_EVENT:
                                    if item.snippet.HasField("text_message_details"):
                                        raw_text = item.snippet.text_message_details.message_text
                                    elif item.snippet.HasField("display_message"):
                                        raw_text = item.snippet.display_message
                                    else:
                                        continue

                                    cleaned_chat = clean_whitespace_and_youtube_emotes_from_message(raw_text)
                                    if is_text_empty_or_only_punctuation(cleaned_chat):
                                        continue
                                    formatted_msg = format_message_with_username_and_truncate(cleaned_chat, author_display_name, "@")
                                    enqueue_msg("[YT]", formatted_msg)

                                elif event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.SUPER_CHAT_EVENT:
                                    if item.snippet.HasField("super_chat_details"):
                                        details = item.snippet.super_chat_details
                                        amount = details.amount_display_string if details.HasField("amount_display_string") else ""
                                        comment = details.user_comment if details.HasField("user_comment") else ""
                                        msg = f"{comment} {amount}".strip()
                                        formatted_msg = format_message_with_username_and_truncate(msg, author_display_name, "@")
                                        enqueue_msg("[YT]", formatted_msg)

                                elif event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.SUPER_STICKER_EVENT:
                                    if item.snippet.HasField("super_sticker_details"):
                                        details = item.snippet.super_sticker_details
                                        amount = details.amount_display_string if details.HasField("amount_display_string") else ""
                                        lang = config.get("OUTPUT_LANGUAGE", "en")
                                        sticker_name = "Super Sticker" if lang == "en" else "超级贴纸"
                                        msg = f"{sticker_name} {amount}".strip()
                                        formatted_msg = format_message_with_username_and_truncate(msg, author_display_name, "@")
                                        enqueue_msg("[YT]", formatted_msg)

                                elif event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.NEW_SPONSOR_EVENT:
                                    lang = config.get("OUTPUT_LANGUAGE", "en")
                                    msg = "New Membership!" if lang == "en" else "新会员！"
                                    formatted_msg = format_message_with_username_and_truncate(msg, author_display_name, "@")
                                    enqueue_msg("[YT]", formatted_msg)

                                elif event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.MEMBERSHIP_GIFTING_EVENT:
                                    if item.snippet.HasField("membership_gifting_details"):
                                        details = item.snippet.membership_gifting_details
                                        count = details.gift_memberships_count if details.HasField("gift_memberships_count") else 1
                                        lang = config.get("OUTPUT_LANGUAGE", "en")
                                        msg = f"Gifted {count} Memberships!" if lang == "en" else f"赠送了 {count} 个会员！"
                                        formatted_msg = format_message_with_username_and_truncate(msg, author_display_name, "@")
                                        enqueue_msg("[YT]", formatted_msg)

                                elif event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.GIFT_MEMBERSHIP_RECEIVED_EVENT:
                                    lang = config.get("OUTPUT_LANGUAGE", "en")
                                    msg = "Received a Gift Membership!" if lang == "en" else "收到了赠送的会员！"
                                    formatted_msg = format_message_with_username_and_truncate(msg, author_display_name, "@")
                                    enqueue_msg("[YT]", formatted_msg)

                            if response.next_page_token:
                                next_page_token = response.next_page_token

                            if has_live_stream_ended:
                                add_log("[YouTube] Stream ended event received from chat.")
                                return "stream_ended"

                        if shutdown_event.is_set():
                            return "shutdown"

                        if saw_new_chat_items or saw_initial_poll_this_call:
                            idle_poll_delay_seconds = base_poll_delay
                            next_wait_seconds = base_poll_delay
                        else:
                            idle_poll_delay_seconds = min(idle_poll_delay_seconds * poll_multiplier, max_poll_delay)
                            next_wait_seconds = idle_poll_delay_seconds

                    except grpc.RpcError as rpc_error:
                        is_error_retry = True
                        if shutdown_event.is_set():
                            return "shutdown"

                        status_code = rpc_error.code()
                        details = rpc_error.details() or ""

                        if status_code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION):
                            add_log(f"[YouTube] Live chat ended or unavailable ({status_code.name}): {details}")
                            return "chat_closed"

                        if status_code == grpc.StatusCode.INVALID_ARGUMENT:
                            if next_page_token:
                                add_log(f"[YouTube] Resumption page_token expired or invalid ({details}); resetting page_token and reconnecting in {error_retry_delay_seconds:.0f}s...")
                                next_page_token = None
                            else:
                                add_log(f"[YouTube] Live chat unavailable (INVALID_ARGUMENT): {details}")
                                return "chat_closed"
                        elif status_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                            error_retry_delay_seconds = max(error_retry_delay_seconds, 300.0)
                            add_log(f"[YouTube] Quota/rate exhausted ({status_code.name}): {details}. Pausing reconnect for {error_retry_delay_seconds:.0f}s...")
                        else:
                            # Transient error (e.g. UNAVAILABLE, DEADLINE_EXCEEDED, CANCELLED)
                            add_log(f"[YouTube] gRPC stream disconnected ({status_code.name if status_code else 'Error'}), resuming in {error_retry_delay_seconds:.0f}s (page_token preserved={bool(next_page_token)})... ({details})")
                            if status_code == grpc.StatusCode.UNAVAILABLE:
                                should_recreate_channel = True
                        next_wait_seconds = error_retry_delay_seconds
                    finally:
                        active_grpc_call = None

                    if wait_in_thread_for_shutdown(next_wait_seconds):
                        return "shutdown"
                    if is_error_retry:
                        error_retry_delay_seconds = min(error_retry_delay_seconds * 1.5, 30.0)
                    if should_recreate_channel:
                        break

        except Exception as e:
            if shutdown_event.is_set():
                return "shutdown"
            add_log(f"[YouTube] gRPC channel error: {e}, reconnecting in {error_retry_delay_seconds:.0f}s...")
            if wait_in_thread_for_shutdown(error_retry_delay_seconds):
                return "shutdown"
            error_retry_delay_seconds = min(error_retry_delay_seconds * 1.5, 30.0)

    return "shutdown"

async def listen_to_youtube_chat_and_queue_messages(chat_message_queue: asyncio.Queue, channel_handle_string: str):
    if not channel_handle_string:
        return

    main_loop = asyncio.get_running_loop()

    while not shutdown_event.is_set():
        api_key = config.get("YOUTUBE_API_KEY", "")
        if not api_key:
            add_log("[YouTube] API key is missing. Waiting...")
            await wait_until_timeout_or_shutdown_event(10.0)
            continue

        # Step 1: Discover active live stream or upcoming stream with active chat
        live_info = None
        has_logged_waiting = False
        last_log_time = 0

        while not shutdown_event.is_set():
            live_info = await asyncio.to_thread(get_current_live_video_info_from_youtube_channel, channel_handle_string)

            if live_info:
                add_log(f"[YouTube] Found active stream '{live_info['title']}' (ID: {live_info['video_id']}, status: {live_info['status']}). Live chat ID: {live_info['chat_id']}")
                break

            now = datetime.datetime.now().timestamp()
            if not has_logged_waiting or (now - last_log_time >= 300):
                add_log(f"[YouTube] Channel {channel_handle_string} is not streaming yet. Monitoring for live stream...")
                has_logged_waiting = True
                last_log_time = now

            await wait_until_timeout_or_shutdown_event(30.0)

        if shutdown_event.is_set() or not live_info:
            return

        # Step 2: Stream chat messages via gRPC with auto-reconnection
        active_live_chat_id = live_info["chat_id"]
        result = await asyncio.to_thread(
            run_grpc_stream_with_reconnect,
            active_live_chat_id,
            api_key,
            chat_message_queue,
            main_loop,
        )

        if shutdown_event.is_set():
            return

        if result in ("stream_ended", "chat_closed"):
            add_log("[YouTube] Stream ended or chat closed. Returning to monitoring mode for next stream...")
            await wait_until_timeout_or_shutdown_event(10.0)

async def listen_to_twitch_chat_and_queue_messages(chat_message_queue: asyncio.Queue):
    twitch_irc_server_address = "irc.chat.twitch.tv"
    anonymous_twitch_nickname = "justinfan12345"

    twitch_connection_options = [
        {"port": 6667, "ssl_context": None, "label": "plain"},
        {"port": 6697, "ssl_context": ssl.create_default_context(), "label": "ssl"},
    ]
    
    twitch_channel = config.get("TWITCH_CHANNEL", "")
    if not twitch_channel:
        return

    while not shutdown_event.is_set():
        stream_writer = None
        try:
            stream_reader = None

            for connection_option in twitch_connection_options:
                try:
                    stream_reader, stream_writer = await asyncio.wait_for(
                        asyncio.open_connection(twitch_irc_server_address, connection_option["port"], ssl=connection_option["ssl_context"]),
                        timeout=10.0,
                    )
                    break
                except Exception:
                    stream_reader, stream_writer = None, None

            if stream_reader is None or stream_writer is None:
                raise RuntimeError("Could not connect to Twitch IRC on any port")

            stream_writer.write(f"NICK {anonymous_twitch_nickname}\r\n".encode("utf-8"))
            stream_writer.write(b"CAP REQ :twitch.tv/tags twitch.tv/commands\r\n")
            stream_writer.write(f"JOIN #{twitch_channel.lower()}\r\n".encode("utf-8"))
            await stream_writer.drain()

            while not shutdown_event.is_set():
                try:
                    raw_irc_line = await asyncio.wait_for(stream_reader.readline(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                if not raw_irc_line:
                    raise RuntimeError("Twitch IRC connection closed")

                decoded_irc_line = raw_irc_line.decode("utf-8", errors="ignore").strip()

                if decoded_irc_line.startswith("PING"):
                    stream_writer.write(b"PONG\r\n")
                    await stream_writer.drain()
                    continue

                if "PRIVMSG" in decoded_irc_line:
                    privmsg_regex_match = twitch_privmsg_pattern.match(decoded_irc_line)
                    if not privmsg_regex_match:
                        continue

                    twitch_tags_string = privmsg_regex_match.group(1)
                    twitch_author_username = privmsg_regex_match.group(2)
                    raw_chat_message = privmsg_regex_match.group(3)

                    message_without_twitch_emotes = remove_twitch_emotes_from_message(raw_chat_message, twitch_tags_string)
                    cleaned_chat_message = clean_whitespace_from_message(message_without_twitch_emotes)

                    tags = parse_twitch_tags(twitch_tags_string)
                    bits = tags.get("bits", "0")
                    if bits.isdigit() and int(bits) > 0:
                        lang = config.get("OUTPUT_LANGUAGE", "en")
                        bits_text = "Bits" if lang == "en" else "碎晶"
                        cleaned_chat_message = f"{cleaned_chat_message} {bits} {bits_text}".strip()

                    if is_text_empty_or_only_punctuation(cleaned_chat_message):
                        continue

                    formatted_final_message = format_message_with_username_and_truncate(cleaned_chat_message, twitch_author_username, "#")
                    if formatted_final_message:
                        await chat_message_queue.put(("[TW]", formatted_final_message))

                elif "USERNOTICE" in decoded_irc_line:
                    usernotice_match = twitch_usernotice_pattern.match(decoded_irc_line)
                    if usernotice_match:
                        tags_str = usernotice_match.group(1)
                        user_msg = usernotice_match.group(2) or ""
                        tags = parse_twitch_tags(tags_str)
                        
                        msg_id = tags.get("msg-id")
                        display_name = tags.get("display-name") or tags.get("login") or "unknown"
                        system_msg = tags.get("system-msg", "").replace("\\s", " ")
                        
                        if system_msg:
                            message_content = f"{system_msg} {user_msg}".strip()
                            formatted_final_message = format_message_with_username_and_truncate(message_content, display_name, "#")
                            if formatted_final_message:
                                await chat_message_queue.put(("[TW]", formatted_final_message))

        except Exception as e:
            add_log(f"Twitch listener error: {e}")
            await wait_until_timeout_or_shutdown_event(3.0)

        finally:
            if stream_writer is not None:
                try:
                    stream_writer.close()
                    await stream_writer.wait_closed()
                except Exception:
                    pass

def parse_bilibili_censor_result(raw_resp, fallback_text=""):
    """Inspect Bilibili /msg/send JSON response for censorship ('f' = platform, 'k' = room/streamer)."""
    if not isinstance(raw_resp, dict):
        return False, None, None

    msg_flag = raw_resp.get("msg")
    message_flag = raw_resp.get("message")
    censor_code = None
    if msg_flag in ("f", "k"):
        censor_code = msg_flag
    elif message_flag in ("f", "k"):
        censor_code = message_flag

    if not censor_code:
        return False, None, None

    censor_scope = "platform/system (msg='f')" if censor_code == "f" else "streamer/room keyword (msg='k')"
    censored_content = fallback_text

    data = raw_resp.get("data")
    if isinstance(data, dict):
        mode_info = data.get("mode_info")
        if isinstance(mode_info, dict):
            extra_raw = mode_info.get("extra")
            if isinstance(extra_raw, str) and extra_raw:
                try:
                    extra_json = json.loads(extra_raw)
                    if isinstance(extra_json, dict) and extra_json.get("content"):
                        censored_content = extra_json["content"]
                except Exception:
                    pass
            elif isinstance(extra_raw, dict) and extra_raw.get("content"):
                censored_content = extra_raw["content"]

    return True, censor_scope, censored_content

async def send_bilibili_danmaku_raw(room, text, room_id=None):
    """Send danmaku and return full raw JSON response (preserving msg/message for censor checks)."""
    room.credential.raise_for_no_sessdata()
    target_room_id = room_id if room_id else (await room.get_room_play_info())["room_id"]
    danmaku = Danmaku(text)
    data = {
        "mode": danmaku.mode,
        "msg": danmaku.text,
        "roomid": target_room_id,
        "bubble": 0,
        "rnd": int(time.time()),
        "color": int(danmaku.color, 16),
        "fontsize": danmaku.font_size,
    }
    resp_data = await Api(**LIVE_API["operate"]["send_danmaku"], credential=room.credential).update_data(**data).request(raw=True)
    if isinstance(resp_data, dict):
        code = resp_data.get("code")
        if code is None:
            raise ResponseCodeException(-1, "API returned payload without code field", resp_data)
        if code != 0:
            msg = resp_data.get("msg")
            if msg is None:
                msg = resp_data.get("message")
            if msg is None:
                msg = "API error"
            raise ResponseCodeException(code, msg, resp_data)
    return resp_data

async def send_queued_messages_to_bilibili_live_room(chat_message_queue: asyncio.Queue):
    global bilibili_real_room_id
    max_blocked_retries = 3
    blocked_retry_interval = 3.0
    max_rate_limit_waits = 30
    min_send_interval = 1.1
    last_send_time = 0.0

    while not shutdown_event.is_set() or not chat_message_queue.empty():
        try:
            message_source_tag, chat_message_content = await asyncio.wait_for(chat_message_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            if not bilibili_live_room:
                continue

            # Cache real room_id once to avoid redundant get_room_play_info HTTP calls on every message
            if bilibili_real_room_id is None and bilibili_live_room:
                try:
                    room_info = await bilibili_live_room.get_room_play_info()
                    bilibili_real_room_id = room_info.get("room_id")
                except Exception as e:
                    add_log(f"Warning: Could not get Bilibili real room id: {e}")
                    bilibili_real_room_id = int(config.get("BILIBILI_ROOM_ID", 0))

            target_room_id = bilibili_real_room_id if bilibili_real_room_id else None
            blocked_retries_used = 0
            rate_limit_waits = 0
            refreshed_session = False

            while not shutdown_event.is_set():
                # Enforce spacing before each attempt without adding unnecessary delay when queue was idle
                elapsed = time.monotonic() - last_send_time
                if last_send_time > 0 and elapsed < min_send_interval:
                    await wait_until_timeout_or_shutdown_event(min_send_interval - elapsed)
                    if shutdown_event.is_set():
                        break

                blocked_error_desc = None
                try:
                    raw_resp = await send_bilibili_danmaku_raw(
                        bilibili_live_room,
                        chat_message_content,
                        room_id=target_room_id
                    )
                    last_send_time = time.monotonic()

                    is_censored, censor_scope, censored_content = parse_bilibili_censor_result(raw_resp, chat_message_content)
                    if is_censored:
                        add_log(f"[CENSORED] Bilibili danmaku censored by {censor_scope} ({message_source_tag}): {censored_content}")
                        break

                    # Sent and accepted cleanly (not censored): done (exactly once)
                    break

                except ResponseCodeException as e:
                    raw_err = e.raw if isinstance(e.raw, dict) else {}
                    is_censored, censor_scope, censored_content = parse_bilibili_censor_result(raw_err, chat_message_content)
                    if is_censored:
                        add_log(f"[CENSORED] Bilibili danmaku censored by {censor_scope} ({message_source_tag}): {censored_content}")
                        break

                    err_msg = e.msg or str(e)
                    is_rate_limited = (
                        e.code in (10030, 10031)
                        or (("频率" in err_msg or "过快" in err_msg) and "风险" not in err_msg)
                    )
                    if is_rate_limited:
                        rate_limit_waits += 1
                        min_send_interval = min(min_send_interval + 0.5, 3.0)
                        if rate_limit_waits <= max_rate_limit_waits:
                            await wait_until_timeout_or_shutdown_event(blocked_retry_interval)
                            continue
                        add_log(f"[BLOCKED] Bilibili rate-limit persisted after {max_rate_limit_waits} waits ({message_source_tag}): {chat_message_content}")
                        break

                    if e.code == -101 and not refreshed_session:
                        refreshed_session = True
                        add_log("Bilibili returned -101 during danmaku send; checking credential refresh...")
                        try:
                            refresh_res = await check_and_refresh_bilibili_credentials(force=False)
                            if refresh_res.get("status") in ("ok", "refreshed") and bilibili_live_room:
                                bilibili_live_room.credential = build_bilibili_credential()
                                continue
                        except Exception as refresh_err:
                            add_log(f"Bilibili credential refresh attempt failed: {refresh_err}")

                    blocked_error_desc = f"Bilibili API error: {e}"

                except NetworkException as e:
                    blocked_error_desc = f"Bilibili non-200 HTTP response: {e}"

                except Exception as e:
                    blocked_error_desc = f"Bilibili send error: {e}"

                blocked_retries_used += 1
                if blocked_retries_used <= max_blocked_retries:
                    add_log(f"{blocked_error_desc} ({message_source_tag}). Retrying ({blocked_retries_used}/{max_blocked_retries}) in {blocked_retry_interval:.0f}s...")
                    await wait_until_timeout_or_shutdown_event(blocked_retry_interval)
                else:
                    add_log(f"[BLOCKED] Bilibili message still blocked after {max_blocked_retries} retries ({message_source_tag}): {chat_message_content} | Error: {blocked_error_desc}")
                    break
        finally:
            chat_message_queue.task_done()

async def run_youtube_and_twitch_to_bilibili_chat_bridge():
    bridge_message_queue = asyncio.Queue()

    try:
        results = await asyncio.gather(
            listen_to_youtube_chat_and_queue_messages(bridge_message_queue, config.get("YOUTUBE_CHANNEL_HANDLE", "")),
            listen_to_twitch_chat_and_queue_messages(bridge_message_queue),
            send_queued_messages_to_bilibili_live_room(bridge_message_queue),
            return_exceptions=True
        )
        task_names = ("YouTube listener", "Twitch listener", "Bilibili sender")
        for task_name, res in zip(task_names, results):
            if isinstance(res, Exception):
                add_log(f"{task_name} stopped with unexpected error: {res}")
    except Exception as e:
        add_log(f"Bridge runner error: {e}")


# ---- API Endpoints ----
@app.get("/")
def get_ui():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/favicon.ico")
def get_favicon():
    return FileResponse("favicon.ico")

@app.get("/api/logs")
def api_get_logs():
    if not os.path.exists("log.txt"):
        return {"logs": ""}
    try:
        with open("log.txt", "r", encoding="utf-8") as f:
            # Return last 100 lines
            lines = f.readlines()
            return {"logs": "".join(lines[-100:])}
    except Exception:
        return {"logs": ""}

@app.get("/api/config")
def api_get_config():
    return config

@app.get("/api/status")
def api_get_status():
    return {
        "running": bridge_task is not None and not bridge_task.done(),
        "version": get_installed_version(),
    }

def _parse_semver(tag: str) -> tuple:
    nums = [int(n) for n in re.findall(r"\d+", str(tag or ""))]
    if not nums:
        return ()
    while len(nums) > 1 and nums[-1] == 0:
        nums.pop()
    return tuple(nums)

def is_newer_version(candidate_tag: str, current_tag: str) -> bool:
    candidate_ver = _parse_semver(candidate_tag)
    current_ver = _parse_semver(current_tag)
    if not candidate_ver:
        return False
    if not current_ver:
        return True
    return candidate_ver > current_ver

def _version_not_newer_message(candidate_tag: str, current_tag: str) -> str:
    candidate_ver = _parse_semver(candidate_tag)
    current_ver = _parse_semver(current_tag)
    if candidate_ver and current_ver and candidate_ver < current_ver:
        return f"Update rejected: release {candidate_tag} is older than installed version {current_tag} (downgrade is not allowed)."
    return f"Already on version {current_tag} (latest release is {candidate_tag}); no newer version available."

def _get_version_marker_path(base_dir: str = APP_DIR) -> str:
    python_dir = os.path.join(base_dir, "python")
    if os.path.isdir(python_dir):
        return os.path.join(python_dir, ".installed_version")
    return os.path.join(base_dir, ".installed_version")

def get_installed_version(base_dir: str = APP_DIR) -> str:
    best_ver = APP_VERSION
    marker_path = _get_version_marker_path(base_dir)
    if os.path.exists(marker_path):
        try:
            with open(marker_path, "r", encoding="utf-8") as f:
                marked = f.read().strip()
            if marked and _parse_semver(marked) >= _parse_semver(best_ver):
                best_ver = marked
        except Exception:
            pass
    return best_ver

def fetch_latest_github_release() -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"Youtube-Twitch-Livechat-To-Bilibili/{APP_VERSION}",
    }
    latest_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
    resp = requests.get(latest_url, headers=headers, timeout=15)
    if resp.status_code == 404:
        list_resp = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases", headers=headers, timeout=15)
        list_resp.raise_for_status()
        releases = list_resp.json()
        if not releases:
            raise RuntimeError("No GitHub releases found.")
        release = releases[0]
    else:
        resp.raise_for_status()
        release = resp.json()

    tag_name = (release.get("tag_name") or "").strip()
    if not tag_name:
        raise RuntimeError("Latest GitHub release does not contain a tag_name.")

    download_url = ""
    for asset in release.get("assets", []):
        name = (asset.get("name") or "").lower()
        if name.endswith(".zip") and asset.get("browser_download_url"):
            download_url = asset["browser_download_url"]
            break
    if not download_url:
        download_url = release.get("zipball_url") or f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{tag_name}.zip"

    return {
        "tag_name": tag_name,
        "name": release.get("name") or tag_name,
        "body": release.get("body") or "",
        "html_url": release.get("html_url") or f"https://github.com/{GITHUB_REPO}/releases/tag/{tag_name}",
        "download_url": download_url,
    }

def _is_protected_update_path(rel_path: str) -> bool:
    norm = rel_path.replace("\\", "/").lstrip("/")
    if not norm:
        return True
    top_segment = norm.split("/", 1)[0]
    protected_exact = {
        "config.json",
        "log.txt",
        "python",
        ".git",
        "__pycache__",
        ".installed_version",
        ".venv",
        ".venv313",
    }
    if top_segment in protected_exact:
        return True
    if top_segment.startswith("log.") or (top_segment.startswith("twitch_events_") and top_segment.endswith(".log")):
        return True
    return False

def _safe_extract_zip(zf: zipfile.ZipFile, dest_dir: str):
    real_dest = os.path.realpath(dest_dir)
    for member in zf.infolist():
        target_path = os.path.realpath(os.path.join(dest_dir, member.filename))
        if target_path != real_dest and not target_path.startswith(real_dest + os.sep):
            raise RuntimeError(f"Unsafe zip entry detected: {member.filename}")
    zf.extractall(dest_dir)

def _sync_dependencies_and_proto(base_dir: str = APP_DIR):
    python_dir = os.path.join(base_dir, "python")
    if not os.path.isdir(python_dir):
        return

    req_file = os.path.join(base_dir, "requirements.txt")
    req_marker = os.path.join(python_dir, ".requirements.txt")
    if os.path.exists(req_file):
        req_changed = not os.path.exists(req_marker)
        if not req_changed:
            with open(req_file, "rb") as f1, open(req_marker, "rb") as f2:
                req_changed = f1.read() != f2.read()
        if req_changed:
            res = subprocess.run([sys.executable, "-m", "pip", "install", "-r", req_file], cwd=base_dir, capture_output=True, text=True)
            if res.returncode == 0:
                shutil.copyfile(req_file, req_marker)
            else:
                try:
                    os.remove(req_marker)
                except OSError:
                    pass

    proto_file = os.path.join(base_dir, "stream_list.proto")
    proto_marker = os.path.join(python_dir, ".stream_list.proto")
    if os.path.exists(proto_file):
        proto_changed = not os.path.exists(proto_marker)
        if not proto_changed:
            with open(proto_file, "rb") as f1, open(proto_marker, "rb") as f2:
                proto_changed = f1.read() != f2.read()
        if proto_changed:
            res = subprocess.run(
                [sys.executable, "-m", "grpc_tools.protoc", "-I.", "--python_out=.", "--grpc_python_out=.", "stream_list.proto"],
                cwd=base_dir,
                capture_output=True,
                text=True,
            )
            if res.returncode == 0:
                shutil.copyfile(proto_file, proto_marker)
            else:
                try:
                    os.remove(proto_marker)
                except OSError:
                    pass

def apply_github_release_update(base_dir: str = APP_DIR, release_info: dict = None, zip_bytes: bytes = None, sync_deps: bool = True) -> dict:
    previous_version = get_installed_version(base_dir)
    if release_info is None:
        release_info = fetch_latest_github_release()

    latest_tag = release_info["tag_name"]
    if not is_newer_version(latest_tag, previous_version):
        return {
            "status": "up_to_date",
            "previous_version": previous_version,
            "updated_version": previous_version,
            "latest_version": latest_tag,
            "updated_files": 0,
            "config_preserved": True,
            "message": _version_not_newer_message(latest_tag, previous_version),
        }

    temp_dir = tempfile.mkdtemp(prefix="yt_bili_update_")
    config_path = os.path.join(base_dir, CONFIG_FILE)
    config_backup_bytes = None
    if os.path.exists(config_path):
        with open(config_path, "rb") as f:
            config_backup_bytes = f.read()

    try:
        if zip_bytes is None:
            headers = {"User-Agent": f"Youtube-Twitch-Livechat-To-Bilibili/{APP_VERSION}"}
            dl_resp = requests.get(release_info["download_url"], headers=headers, timeout=60, allow_redirects=True)
            dl_resp.raise_for_status()
            zip_bytes = dl_resp.content

        zip_path = os.path.join(temp_dir, "release.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)

        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad_file = zf.testzip()
            if bad_file is not None:
                raise RuntimeError(f"Corrupted file in release archive: {bad_file}")
            _safe_extract_zip(zf, extract_dir)

        # Unwrap single top-level directory produced by GitHub source zipballs
        top_entries = os.listdir(extract_dir)
        source_root = extract_dir
        if len(top_entries) == 1 and os.path.isdir(os.path.join(extract_dir, top_entries[0])):
            source_root = os.path.join(extract_dir, top_entries[0])

        updated_files = 0
        for root, dirs, files in os.walk(source_root):
            rel_root = os.path.relpath(root, source_root)
            rel_root = "" if rel_root == "." else rel_root
            dirs[:] = [d for d in dirs if not _is_protected_update_path(os.path.join(rel_root, d) if rel_root else d)]

            for filename in files:
                rel_file = os.path.join(rel_root, filename) if rel_root else filename
                if _is_protected_update_path(rel_file):
                    continue
                src_file = os.path.join(root, filename)
                dst_file = os.path.join(base_dir, rel_file)
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                shutil.copy2(src_file, dst_file)
                updated_files += 1

        if config_backup_bytes is not None:
            with open(config_path, "wb") as f:
                f.write(config_backup_bytes)

        marker_path = _get_version_marker_path(base_dir)
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as f:
            f.write(latest_tag)

        if sync_deps:
            _sync_dependencies_and_proto(base_dir)

        log_path = os.path.join(base_dir, "log.txt")
        if os.path.exists(log_path):
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("")

        return {
            "status": "updated",
            "previous_version": previous_version,
            "updated_version": latest_tag,
            "updated_files": updated_files,
            "config_preserved": True,
        }
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def _schedule_self_restart(delay_seconds: float = 0.8):
    app_entry = os.path.abspath(__file__)
    helper_code = (
        "import socket, subprocess, sys, time; "
        "deadline = time.monotonic() + 15.0; "
        "time.sleep(1.2); "
        "\nwhile time.monotonic() < deadline:\n"
        "    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        "    in_use = s.connect_ex(('127.0.0.1', 8000)) == 0\n"
        "    s.close()\n"
        "    if not in_use:\n"
        "        break\n"
        "    time.sleep(0.3)\n"
        "flags = getattr(subprocess, 'CREATE_NEW_CONSOLE', 0)\n"
        "subprocess.Popen([sys.executable, sys.argv[1]], cwd=sys.argv[2], creationflags=flags)\n"
    )

    def _spawn_and_exit():
        try:
            subprocess.Popen([sys.executable, "-c", helper_code, app_entry, APP_DIR], cwd=APP_DIR)
        except Exception as e:
            add_log(f"Failed to launch updated process: {e}")
        os._exit(0)

    asyncio.get_event_loop().call_later(delay_seconds, _spawn_and_exit)

@app.get("/api/update/check")
async def api_update_check():
    current_version = get_installed_version()
    try:
        release_info = await asyncio.to_thread(fetch_latest_github_release)
        latest_version = release_info["tag_name"]
        update_available = is_newer_version(latest_version, current_version)
        return {
            "status": "ok",
            "current_version": current_version,
            "latest_version": latest_version,
            "update_available": update_available,
            "release_name": release_info["name"],
            "release_notes": release_info["body"],
            "html_url": release_info["html_url"],
        }
    except Exception as e:
        return {
            "status": "error",
            "current_version": current_version,
            "latest_version": current_version,
            "update_available": False,
            "message": str(e),
        }

@app.post("/api/update")
async def api_perform_update():
    global shutdown_event, bridge_task
    try:
        current_version = get_installed_version()
        release_info = await asyncio.to_thread(fetch_latest_github_release)
        latest_version = release_info["tag_name"]
        if not is_newer_version(latest_version, current_version):
            return {
                "status": "up_to_date",
                "previous_version": current_version,
                "updated_version": current_version,
                "latest_version": latest_version,
                "updated_files": 0,
                "config_preserved": True,
                "restarting": False,
                "message": _version_not_newer_message(latest_version, current_version),
            }

        if bridge_task and not bridge_task.done():
            if shutdown_event:
                shutdown_event.set()
            cancel_active_grpc_stream()
            add_log("Stopping bridge tasks for application update...")
            await bridge_task
            bridge_task = None

        result = await asyncio.to_thread(apply_github_release_update, APP_DIR, release_info)
        if result.get("status") != "updated":
            result["restarting"] = False
            return result
        add_log(f"Updated application from {result['previous_version']} to {result['updated_version']} (config.json preserved). Restarting...")
        _schedule_self_restart(delay_seconds=0.8)
        result["restarting"] = True
        return result
    except Exception as e:
        add_log(f"Update failed: {e}")
        return {"status": "error", "message": str(e)}

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(bilibili_cookie_keeper_task())

qr_login_sessions = {}

def clean_expired_qr_sessions():
    now = time.time()
    expired = [sid for sid, s in qr_login_sessions.items() if now - s.get("created_at", 0) > 600]
    for sid in expired:
        qr_login_sessions.pop(sid, None)

@app.get("/api/bilibili/status")
async def api_bilibili_status():
    if not config.get("SESSDATA"):
        return {
            "logged_in": False,
            "valid": False,
            "needs_refresh": False,
            "dedeuserid": "",
            "has_ac_time_value": False
        }

    client_ok, client_info = ensure_bilibili_http_client()
    if not client_ok:
        return {
            "logged_in": False,
            "valid": False,
            "needs_refresh": False,
            "dedeuserid": str(config.get("DEDEUSERID", "")),
            "has_ac_time_value": bool(config.get("AC_TIME_VALUE")),
            "error": client_info
        }

    cred = build_bilibili_credential()
    is_valid = False
    needs_refresh = False
    check_error = None
    try:
        is_valid = await cred.check_valid()
    except Exception as e:
        check_error = str(e)
        is_valid = False

    if check_error is None:
        try:
            needs_refresh = await cred.check_refresh()
        except Exception:
            needs_refresh = False

    result = {
        "logged_in": is_valid,
        "valid": is_valid,
        "needs_refresh": needs_refresh,
        "dedeuserid": str(config.get("DEDEUSERID", "")),
        "has_ac_time_value": bool(config.get("AC_TIME_VALUE"))
    }
    if check_error is not None:
        result["error"] = check_error
    return result

@app.post("/api/bilibili/refresh")
async def api_bilibili_refresh(force: bool = True):
    result = await check_and_refresh_bilibili_credentials(force=force)
    return result

@app.post("/api/bilibili/qrcode/start")
async def api_bilibili_qrcode_start():
    clean_expired_qr_sessions()
    session_id = str(uuid.uuid4())
    loop = asyncio.get_running_loop()

    def _generate():
        resp = requests.get(
            "https://passport.bilibili.com/x/passport-login/web/qrcode/generate",
            headers=BILI_HEADERS,
            timeout=10
        )
        return resp.json()

    try:
        data = await loop.run_in_executor(None, _generate)
    except Exception as e:
        return {"status": "error", "message": f"Network error generating QR code: {e}"}

    if data.get("code") != 0:
        return {"status": "error", "message": data.get("message", "Failed to generate QR code from Bilibili")}

    qr_data = data.get("data", {})
    qr_url = qr_data.get("url", "")
    qrcode_key = qr_data.get("qrcode_key", "")

    qr_builder = qrcode.QRCode(box_size=6, border=2)
    qr_builder.add_data(qr_url)
    qr_builder.make(fit=True)
    img = qr_builder.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    img_base64 = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("utf-8")

    qr_login_sessions[session_id] = {
        "qrcode_key": qrcode_key,
        "qr_url": qr_url,
        "created_at": time.time()
    }

    return {
        "session_id": session_id,
        "qrcode_img": img_base64,
        "url": qr_url
    }

@app.get("/api/bilibili/qrcode/poll")
async def api_bilibili_qrcode_poll(session_id: str):
    clean_expired_qr_sessions()
    session_data = qr_login_sessions.get(session_id)
    if not session_data:
        return {"status": "timeout", "message": "QR code session expired. Please refresh."}

    qrcode_key = session_data["qrcode_key"]
    loop = asyncio.get_running_loop()

    def _poll():
        session = requests.Session()
        resp = session.get(
            "https://passport.bilibili.com/x/passport-login/web/qrcode/poll",
            params={"qrcode_key": qrcode_key},
            headers=BILI_HEADERS,
            timeout=10
        )
        json_data = resp.json()
        cookies_dict = session.cookies.get_dict()

        # Follow crossDomain URL if present to capture cross-domain tickets/cookies
        url = json_data.get("data", {}).get("url", "")
        if json_data.get("data", {}).get("code") == 0 and url:
            try:
                session.get(url, headers=BILI_HEADERS, timeout=10)
                cookies_dict.update(session.cookies.get_dict())
            except Exception:
                pass

        return json_data, cookies_dict

    try:
        res_json, captured_cookies = await loop.run_in_executor(None, _poll)
    except Exception as e:
        return {"status": "waiting", "message": f"Polling error: {e}"}

    poll_data = res_json.get("data", {})
    code = poll_data.get("code")

    if code == 86101:
        return {"status": "scan", "message": "Waiting for scan..."}
    elif code == 86090:
        return {"status": "conf", "message": "Scanned! Please confirm login on your Bilibili App."}
    elif code == 86038:
        qr_login_sessions.pop(session_id, None)
        return {"status": "timeout", "message": "QR code timed out. Please try again."}
    elif code == 0:
        qr_login_sessions.pop(session_id, None)

        sessdata = captured_cookies.get("SESSDATA", "")
        bili_jct = captured_cookies.get("bili_jct", "")
        dedeuserid = captured_cookies.get("DedeUserID", "")
        refresh_token = poll_data.get("refresh_token", "")

        # Fallback to query string if needed
        url = poll_data.get("url", "")
        if ("?" in url) and (not sessdata or not bili_jct or not dedeuserid):
            qs = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            if not sessdata and "SESSDATA" in qs:
                sessdata = qs["SESSDATA"][0]
            if not bili_jct and "bili_jct" in qs:
                bili_jct = qs["bili_jct"][0]
            if not dedeuserid and "DedeUserID" in qs:
                dedeuserid = qs["DedeUserID"][0]

        if not sessdata or not bili_jct:
            add_log(f"Warning: QR login code=0 but cookies missing. captured={list(captured_cookies.keys())}, url={url}")
            return {
                "status": "error",
                "message": "Login succeeded on phone, but cookies could not be captured. Please try again."
            }

        try:
            buvid3, buvid4 = await get_buvid()
        except Exception:
            buvid3, buvid4 = "", ""

        config["SESSDATA"] = sessdata
        config["BILI_JCT"] = bili_jct
        config["DEDEUSERID"] = str(dedeuserid)
        config["AC_TIME_VALUE"] = refresh_token
        if buvid3:
            config["BUVID3"] = buvid3
        if buvid4:
            config["BUVID4"] = buvid4
        save_config()

        add_log(f"Bilibili login successful via QR code for UID {config['DEDEUSERID']}! SESSDATA, bili_jct, and AC_TIME_VALUE captured.")
        return {
            "status": "done",
            "message": "Login successful!",
            "dedeuserid": config["DEDEUSERID"],
            "config": config
        }
    else:
        return {"status": "waiting", "message": "Waiting for scan..."}

@app.post("/api/config")
async def api_save_config(request: Request):
    global config, channel_id_cache
    new_config = await request.json()
    for cookie_key in ("SESSDATA", "BILI_JCT", "BUVID3", "BUVID4", "DEDEUSERID", "AC_TIME_VALUE"):
        if cookie_key in new_config and isinstance(new_config[cookie_key], str):
            new_config[cookie_key] = new_config[cookie_key].strip()
    config.update(new_config)
    channel_id_cache.clear()
    save_config()
    return {"status": "ok"}

@app.post("/api/start")
async def api_start():
    global bridge_task, shutdown_event, bilibili_live_room, bilibili_real_room_id, youtube_api_client, channel_id_cache
    
    if bridge_task and not bridge_task.done():
        return {"status": "already running"}
        
    shutdown_event = asyncio.Event()
    channel_id_cache.clear()
    bilibili_real_room_id = None

    try:
        if config.get("SESSDATA"):
            if config.get("AC_TIME_VALUE"):
                await check_and_refresh_bilibili_credentials(force=False)
            bilibili_credential = build_bilibili_credential()
            bilibili_live_room = LiveRoom(int(config.get("BILIBILI_ROOM_ID", 0)), credential=bilibili_credential)
        else:
            bilibili_live_room = None
            
        yt_api_key = config.get("YOUTUBE_API_KEY", "")
        if yt_api_key:
            youtube_api_client = build("youtube", "v3", developerKey=yt_api_key)
        else:
            youtube_api_client = None
    except Exception as e:
        add_log(f"Error starting bridge: {e}")
        return {"status": "error", "message": str(e)}

    add_log("Starting bridge tasks...")
    bridge_task = asyncio.create_task(run_youtube_and_twitch_to_bilibili_chat_bridge())
    return {"status": "started"}

def cancel_active_grpc_stream():
    global active_grpc_call
    call = active_grpc_call
    if call is not None:
        try:
            call.cancel()
        except Exception:
            pass

@app.post("/api/stop")
async def api_stop():
    global shutdown_event, bridge_task
    if shutdown_event:
        shutdown_event.set()
    cancel_active_grpc_stream()
    if bridge_task:
        add_log("Stopping bridge tasks...")
        await bridge_task
        bridge_task = None
        add_log("Bridge tasks stopped.")
    return {"status": "stopped"}

@app.post("/api/shutdown")
async def api_shutdown():
    global shutdown_event
    if shutdown_event:
        shutdown_event.set()
    cancel_active_grpc_stream()
    
    def exit_server():
        os._exit(0)
    
    asyncio.get_event_loop().call_later(0.5, exit_server)
    return {"status": "shutting down"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)