import asyncio
import collections
import re
import ssl
import string
import urllib.request
import urllib.parse
import json
import os
import sys
import datetime
import grpc
import requests
import io
import base64
import uuid
import time
import qrcode

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn

import stream_list_pb2
import stream_list_pb2_grpc

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bilibili_api import Credential, Danmaku, login_v2
from bilibili_api.utils.network import get_buvid
from bilibili_api.live import LiveRoom

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

app = FastAPI()

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
    "BANNED_WORDS": ""
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
        return {"status": "no_credentials", "message": "No SESSDATA configured", "valid": False}

    cred = build_bilibili_credential()

    is_valid = False
    try:
        is_valid = await cred.check_valid()
    except Exception:
        is_valid = False

    needs_refresh = False
    try:
        needs_refresh = await cred.check_refresh()
    except Exception:
        needs_refresh = False

    should_refresh = force or needs_refresh or (not is_valid and bool(config.get("AC_TIME_VALUE")))

    if should_refresh:
        if not config.get("AC_TIME_VALUE"):
            msg = "Bilibili cookies need refresh, but no AC_TIME_VALUE (refresh token) is present. Please re-login via QR code."
            add_log(msg)
            return {"status": "missing_token", "message": msg, "valid": is_valid}

        try:
            add_log("Attempting Bilibili cookie refresh with AC_TIME_VALUE...")
            await cred.refresh()
            config["SESSDATA"] = cred.sessdata or ""
            config["BILI_JCT"] = cred.bili_jct or ""
            config["DEDEUSERID"] = str(cred.dedeuserid or "")
            config["AC_TIME_VALUE"] = cred.ac_time_value or ""
            save_config()
            add_log("Bilibili cookies successfully refreshed and updated in config.json!")
            return {"status": "refreshed", "message": "Cookies refreshed successfully", "valid": True}
        except Exception as e:
            msg = f"Failed to refresh Bilibili cookies: {e}. Please re-login via QR code."
            add_log(msg)
            return {"status": "error", "message": msg, "valid": is_valid}

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
    if clean_handle in channel_id_cache:
        return channel_id_cache[clean_handle]

    normalized_channel_handle = clean_handle
    if normalized_channel_handle.startswith("@"):
        normalized_channel_handle = normalized_channel_handle[1:]

    url_encoded_channel_handle = urllib.parse.quote(normalized_channel_handle)

    if not youtube_api_client:
        return None

    try:
        youtube_api_response = youtube_api_client.channels().list(
            part="id",
            forHandle=url_encoded_channel_handle,
            maxResults=1,
        ).execute()

        response_items_list = youtube_api_response.get("items", [])
        if not response_items_list:
            raise RuntimeError(f"Could not resolve YouTube handle: @{url_encoded_channel_handle}")

        cid = response_items_list[0]["id"]
        channel_id_cache[clean_handle] = cid
        return cid
    except Exception as e:
        add_log(f"Error resolving YouTube channel handle: {e}")
        return None

def verify_youtube_live_chat_status(video_id):
    if not video_id or not youtube_api_client:
        return None
    try:
        res = youtube_api_client.videos().list(
            part="snippet,liveStreamingDetails",
            id=video_id,
        ).execute()
        items = res.get("items", [])
        if not items:
            return None
        item = items[0]
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
    except Exception as e:
        add_log(f"Error checking YouTube video {video_id} live status: {e}")
    return None

def get_current_live_video_info_from_youtube_channel(channel_handle):
    normalized_channel_handle = channel_handle.strip()
    if not normalized_channel_handle.startswith("@") and not normalized_channel_handle.startswith("UC"):
        normalized_channel_handle = "@" + normalized_channel_handle

    candidate_video_ids = []
    encoded_handle = urllib.parse.quote(normalized_channel_handle)
    youtube_live_url = f"https://www.youtube.com/{encoded_handle}/live"
    html_content_string = ""
    final_url = ""

    # Primary method: Fetch /live using curl_cffi with Chrome TLS fingerprinting
    if curl_requests is not None:
        try:
            resp = curl_requests.get(youtube_live_url, impersonate="chrome", timeout=10)
            if resp.status_code == 200:
                html_content_string = resp.text
                final_url = resp.url
        except Exception:
            pass

    # Fallback to urllib with modern browser headers
    if not html_content_string:
        try:
            http_request = urllib.request.Request(
                youtube_live_url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Cache-Control': 'no-cache'
                }
            )
            with urllib.request.urlopen(http_request, timeout=10) as http_response:
                html_content_string = http_response.read().decode('utf-8', errors='ignore')
                final_url = http_response.geturl()
        except Exception:
            pass

    if html_content_string:
        # Check redirect
        m_redirect = re.search(r"[?&]v=([a-zA-Z0-9_-]+)", final_url)
        if m_redirect:
            candidate_video_ids.append(m_redirect.group(1))

        # Check canonical watch link
        canonical_link_match = re.search(r'<link rel="canonical" href="https://www.youtube.com/watch\?v=([a-zA-Z0-9_-]+)">', html_content_string)
        if canonical_link_match:
            candidate_video_ids.append(canonical_link_match.group(1))

        # Check liveStreamRenderer
        live_stream_renderer_match = re.search(r'"liveStreamRenderer":\s*\{\s*"videoId":\s*"([a-zA-Z0-9_-]+)"', html_content_string)
        if live_stream_renderer_match:
            candidate_video_ids.append(live_stream_renderer_match.group(1))

        # Check videoDetails
        video_details_match = re.search(r'"videoDetails":\s*\{\s*"videoId":\s*"([a-zA-Z0-9_-]+)"', html_content_string)
        if video_details_match:
            candidate_video_ids.append(video_details_match.group(1))

    # Verify candidates scraped from /live page
    seen_candidates = set()
    for vid in candidate_video_ids:
        if vid and vid not in seen_candidates:
            seen_candidates.add(vid)
            verified = verify_youtube_live_chat_status(vid)
            if verified:
                return verified

    # Secondary method: Check Channel Uploads Playlist (cost: 1 quota unit for playlist + 1 unit for video verification)
    try:
        resolved_channel_id = get_youtube_channel_id_from_handle(channel_handle)
        if resolved_channel_id and resolved_channel_id.startswith("UC") and youtube_api_client:
            uploads_playlist_id = "UU" + resolved_channel_id[2:]
            playlist_response = youtube_api_client.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=3,
            ).execute()
            for item in playlist_response.get("items", []):
                vid = item.get("contentDetails", {}).get("videoId")
                if vid and vid not in seen_candidates:
                    seen_candidates.add(vid)
                    verified = verify_youtube_live_chat_status(vid)
                    if verified:
                        return verified
    except Exception:
        pass

    return None

def run_grpc_stream_with_reconnect(active_live_chat_id, api_key, chat_message_queue, main_loop):
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
    retry_delay_seconds = 2.0

    def enqueue_msg(tag, msg):
        if not msg:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(chat_message_queue.put((tag, msg)), main_loop)
            future.result(timeout=5.0)
        except Exception as e:
            add_log(f"Failed to queue YouTube message: {e}")

    while not shutdown_event.is_set():
        try:
            with grpc.secure_channel("dns:///youtube.googleapis.com:443", creds, options=grpc_channel_options) as channel:
                stub = stream_list_pb2_grpc.V3DataLiveChatMessageServiceStub(channel)

                while not shutdown_event.is_set():
                    request = stream_list_pb2.LiveChatMessageListRequest(
                        part=["id", "snippet", "authorDetails"],
                        live_chat_id=active_live_chat_id,
                        max_results=200,
                        page_token=next_page_token,
                    )

                    try:
                        for response in stub.StreamList(request, metadata=metadata):
                            if shutdown_event.is_set():
                                return "shutdown"

                            retry_delay_seconds = 2.0  # Reset backoff on receiving messages

                            if is_initial_chat_poll:
                                for item in response.items:
                                    msg_id = item.id
                                    if msg_id and msg_id not in previously_seen_message_ids_set:
                                        previously_seen_message_ids_queue.append(msg_id)
                                        previously_seen_message_ids_set.add(msg_id)
                                is_initial_chat_poll = False
                                next_page_token = response.next_page_token
                                continue

                            has_live_stream_ended = False
                            for item in response.items:
                                msg_id = item.id
                                if not msg_id or msg_id in previously_seen_message_ids_set:
                                    continue

                                if len(previously_seen_message_ids_queue) == 3000:
                                    oldest_tracked_message_id = previously_seen_message_ids_queue.popleft()
                                    previously_seen_message_ids_set.discard(oldest_tracked_message_id)

                                previously_seen_message_ids_queue.append(msg_id)
                                previously_seen_message_ids_set.add(msg_id)

                                event_type = item.snippet.type

                                # Protobuf enums: CHAT_ENDED_EVENT is 4
                                if event_type == stream_list_pb2.LiveChatMessageSnippet.TypeWrapper.CHAT_ENDED_EVENT:
                                    has_live_stream_ended = True
                                    break

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

                            if has_live_stream_ended:
                                add_log("[YouTube] Stream ended event received from chat.")
                                return "stream_ended"

                            next_page_token = response.next_page_token
                            if not next_page_token:
                                break

                    except grpc.RpcError as rpc_error:
                        if shutdown_event.is_set():
                            return "shutdown"

                        status_code = rpc_error.code()
                        details = rpc_error.details() or ""

                        # If chat is ended, closed or not found
                        if status_code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.INVALID_ARGUMENT):
                            add_log(f"[YouTube] Live chat ended or unavailable ({status_code.name}): {details}")
                            return "chat_closed"

                        # Transient error (e.g. 1-hour stream max duration, UNAVAILABLE, DEADLINE_EXCEEDED, CANCELLED)
                        add_log(f"[YouTube] gRPC stream disconnected ({status_code.name if status_code else 'Error'}), reconnecting in {retry_delay_seconds:.0f}s... ({details})")
                        break

        except Exception as e:
            if shutdown_event.is_set():
                return "shutdown"
            add_log(f"[YouTube] gRPC channel error: {e}, reconnecting in {retry_delay_seconds:.0f}s...")

        if shutdown_event.wait(timeout=retry_delay_seconds):
            return "shutdown"
        retry_delay_seconds = min(retry_delay_seconds * 1.5, 30.0)

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

            await wait_until_timeout_or_shutdown_event(15.0)

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

async def send_queued_messages_to_bilibili_live_room(chat_message_queue: asyncio.Queue):
    global bilibili_real_room_id
    while not shutdown_event.is_set() or not chat_message_queue.empty():
        try:
            message_source_tag, chat_message_content = await asyncio.wait_for(chat_message_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        if not bilibili_live_room:
            chat_message_queue.task_done()
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
        max_retries = 3
        send_success = False

        for attempt in range(max_retries + 1):
            if shutdown_event.is_set():
                break

            try:
                await bilibili_live_room.send_danmaku(
                    Danmaku(chat_message_content),
                    room_id=target_room_id
                )
                send_success = True
                break
            except Exception as e:
                if attempt < max_retries:
                    add_log(f"Bilibili send error ({message_source_tag}): {e}. Retrying ({attempt + 1}/{max_retries}) in 1s...")
                    await wait_until_timeout_or_shutdown_event(1.0)
                else:
                    add_log(f"Bilibili send error ({message_source_tag}): {e}. Dropping message after {max_retries} retries: {chat_message_content}")

        chat_message_queue.task_done()

        if send_success:
            # Maintain 1 second spacing between successfully delivered messages to respect Bilibili danmaku rate limits
            await wait_until_timeout_or_shutdown_event(1.0)

async def run_youtube_and_twitch_to_bilibili_chat_bridge():
    bridge_message_queue = asyncio.Queue()

    try:
        await asyncio.gather(
            listen_to_youtube_chat_and_queue_messages(bridge_message_queue, config.get("YOUTUBE_CHANNEL_HANDLE", "")),
            listen_to_twitch_chat_and_queue_messages(bridge_message_queue),
            send_queued_messages_to_bilibili_live_room(bridge_message_queue),
            return_exceptions=True
        )
    except Exception:
        pass


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
    return {"running": bridge_task is not None and not bridge_task.done()}

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

    cred = build_bilibili_credential()
    is_valid = False
    needs_refresh = False
    try:
        is_valid = await cred.check_valid()
    except Exception:
        is_valid = False

    try:
        needs_refresh = await cred.check_refresh()
    except Exception:
        needs_refresh = False

    return {
        "logged_in": is_valid,
        "valid": is_valid,
        "needs_refresh": needs_refresh,
        "dedeuserid": str(config.get("DEDEUSERID", "")),
        "has_ac_time_value": bool(config.get("AC_TIME_VALUE"))
    }

@app.post("/api/bilibili/refresh")
async def api_bilibili_refresh():
    result = await check_and_refresh_bilibili_credentials(force=True)
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
            except Exception as e:
                add_log(f"CrossDomain ticket fetch notice: {e}")

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

@app.post("/api/stop")
async def api_stop():
    global shutdown_event, bridge_task
    if shutdown_event:
        shutdown_event.set()
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
    
    def exit_server():
        os._exit(0)
    
    asyncio.get_event_loop().call_later(0.5, exit_server)
    return {"status": "shutting down"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)