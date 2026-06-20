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

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bilibili_api import Credential, Danmaku
from bilibili_api.live import LiveRoom

app = FastAPI()

CONFIG_FILE = "config.json"

default_config = {
    "SESSDATA": "",
    "BILI_JCT": "",
    "BUVID3": "",
    "DEDEUSERID": "",
    "BILIBILI_ROOM_ID": 23596840,
    "YOUTUBE_CHANNEL_HANDLE": "@沐晓空",
    "TWITCH_CHANNEL": "muxiaokong",
    "YOUTUBE_API_KEY": "",
    "YOUTUBE_POLL_BASE_DELAY_SECONDS": 2.0,
    "YOUTUBE_POLL_MULTIPLIER": 2.0,
    "YOUTUBE_MAX_POLL_DELAY_SECONDS": 30.0
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

# Global state
shutdown_event = None
bridge_task = None

bilibili_live_room = None
youtube_api_client = None

twitch_privmsg_pattern = re.compile(r"^(?:@([^\s]+)\s+)?:(\w+)!.*?PRIVMSG.*? :(.*)$")
youtube_colon_emote_pattern = re.compile(r":[^:\s]{1,64}:")
youtube_bracket_emote_pattern = re.compile(r"\[[^\[\]\n]{1,64}\]")

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
    clean_username_without_prefix = author_username.lstrip(username_prefix_separator)
    combined_message_and_username = f"{message_content} {username_prefix_separator}{clean_username_without_prefix}"
    if len(combined_message_and_username) > maximum_allowed_length:
        return combined_message_and_username[: maximum_allowed_length - 1] + "…"
    return combined_message_and_username

def get_youtube_channel_id_from_handle(channel_handle):
    normalized_channel_handle = channel_handle.strip()
    if normalized_channel_handle.startswith("@"):
        normalized_channel_handle = normalized_channel_handle[1:]

    url_encoded_channel_handle = urllib.parse.quote(normalized_channel_handle)

    youtube_api_response = youtube_api_client.channels().list(
        part="id",
        forHandle=url_encoded_channel_handle,
        maxResults=1,
    ).execute()

    response_items_list = youtube_api_response.get("items", [])
    if not response_items_list:
        raise RuntimeError(f"Could not resolve YouTube handle: @{url_encoded_channel_handle}")

    return response_items_list[0]["id"]

def get_current_live_video_id_from_youtube_channel_handle(channel_handle):
    normalized_channel_handle = channel_handle.strip()
    if not normalized_channel_handle.startswith("@"):
        normalized_channel_handle = "@" + normalized_channel_handle

    url_encoded_channel_handle = urllib.parse.quote(normalized_channel_handle)
    youtube_live_url = f"https://www.youtube.com/{url_encoded_channel_handle}/live"

    try:
        http_request = urllib.request.Request(youtube_live_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(http_request, timeout=10) as http_response:
            html_content_string = http_response.read().decode('utf-8')

            canonical_link_match = re.search(r'<link rel="canonical" href="https://www.youtube.com/watch\?v=([a-zA-Z0-9_-]+)">', html_content_string)
            if canonical_link_match:
                return canonical_link_match.group(1)

            live_stream_renderer_match = re.search(r'"liveStreamRenderer":\s*\{\s*"videoId":\s*"([a-zA-Z0-9_-]+)"', html_content_string)
            if live_stream_renderer_match:
                return live_stream_renderer_match.group(1)

            video_details_match = re.search(r'"videoDetails":\s*\{\s*"videoId":\s*"([a-zA-Z0-9_-]+)"', html_content_string)
            if video_details_match:
                return video_details_match.group(1)
    except Exception:
        pass

    try:
        api_key = config.get("YOUTUBE_API_KEY")
        if api_key and api_key != "PASTE_YOUR_YOUTUBE_DATA_API_KEY_HERE":
            resolved_channel_id = get_youtube_channel_id_from_handle(channel_handle)
            youtube_api_search_response = youtube_api_client.search().list(
                part="id",
                channelId=resolved_channel_id,
                eventType="live",
                type="video",
                maxResults=1,
                order="date",
            ).execute()

            search_response_items_list = youtube_api_search_response.get("items", [])
            if search_response_items_list:
                return search_response_items_list[0]["id"]["videoId"]
    except Exception:
        pass

    return None

def get_active_live_chat_id_from_youtube_video_id(video_id):
    youtube_api_video_response = youtube_api_client.videos().list(
        part="liveStreamingDetails",
        id=video_id,
    ).execute()

    video_response_items_list = youtube_api_video_response.get("items", [])
    if not video_response_items_list:
        raise RuntimeError(f"Video not found for video_id={video_id}")

    live_streaming_details_dictionary = video_response_items_list[0].get("liveStreamingDetails", {})
    active_live_chat_id_string = live_streaming_details_dictionary.get("activeLiveChatId")

    if not active_live_chat_id_string:
        raise RuntimeError("YouTube stream found, but no activeLiveChatId is available.")

    return active_live_chat_id_string

def check_if_error_is_youtube_quota_exhaustion(caught_error_exception):
    if not isinstance(caught_error_exception, HttpError):
        return False

    if getattr(caught_error_exception, "status_code", None) != 403 and getattr(getattr(caught_error_exception, "resp", None), "status", None) != 403:
        return False

    try:
        error_payload_details = caught_error_exception.error_details
    except Exception:
        error_payload_details = None

    error_reason_strings_list = []
    if isinstance(error_payload_details, list):
        for error_item_dictionary in error_payload_details:
            if isinstance(error_item_dictionary, dict):
                error_reason_value = error_item_dictionary.get("reason")
                error_message_value = error_item_dictionary.get("message")
                if error_reason_value:
                    error_reason_strings_list.append(str(error_reason_value))
                if error_message_value:
                    error_reason_strings_list.append(str(error_message_value))

    combined_error_text_lowercase = " ".join(error_reason_strings_list).lower()
    combined_error_text_lowercase += " " + str(caught_error_exception).lower()

    quota_exhaustion_marker_strings = [
        "quotaexceeded", "dailylimitexceeded", "ratelimitexceeded",
        "usagerateexceeded", "quota", "daily limit exceeded", "rate limit exceeded",
    ]

    return any(marker_string in combined_error_text_lowercase for marker_string in quota_exhaustion_marker_strings)

async def listen_to_youtube_chat_and_queue_messages(chat_message_queue: asyncio.Queue, channel_handle_string: str):
    current_live_video_id = None
    active_live_chat_id = None

    if not channel_handle_string:
        return

    while not shutdown_event.is_set():
        try:
            current_live_video_id = get_current_live_video_id_from_youtube_channel_handle(channel_handle_string)

            if not current_live_video_id:
                await wait_until_timeout_or_shutdown_event(60.0)
                continue

            active_live_chat_id = get_active_live_chat_id_from_youtube_video_id(current_live_video_id)
            break

        except HttpError as http_error_exception:
            if check_if_error_is_youtube_quota_exhaustion(http_error_exception):
                print("\n*** YouTube API Quota exhausted! Shutting down YouTube listener. Twitch-to-Bilibili bridge remains 100% active. ***\n")
                return
            await wait_until_timeout_or_shutdown_event(60.0)
        except Exception:
            await wait_until_timeout_or_shutdown_event(10.0)

    if shutdown_event.is_set():
        return

    previously_seen_message_ids_set = set()
    previously_seen_message_ids_queue = collections.deque(maxlen=2000)

    youtube_api_next_page_token = None
    is_initial_chat_poll = True
    
    current_exponential_backoff_delay = float(config.get("YOUTUBE_POLL_BASE_DELAY_SECONDS", 2.0))

    while not shutdown_event.is_set():
        try:
            youtube_api_chat_response = youtube_api_client.liveChatMessages().list(
                liveChatId=active_live_chat_id,
                part="id,snippet,authorDetails",
                pageToken=youtube_api_next_page_token,
                maxResults=200,
            ).execute()

            current_chat_items_list = youtube_api_chat_response.get("items", [])
            youtube_api_next_page_token = youtube_api_chat_response.get("nextPageToken")
            youtube_api_polling_interval_milliseconds = youtube_api_chat_response.get("pollingIntervalMillis", 3000)

            has_live_stream_ended = False
            for chat_item_dictionary in current_chat_items_list:
                if chat_item_dictionary.get("snippet", {}).get("type") == "liveChatEndedEvent":
                    has_live_stream_ended = True
                    break

            if has_live_stream_ended:
                shutdown_event.set()
                break

            if is_initial_chat_poll:
                for chat_item_dictionary in current_chat_items_list:
                    message_id_string = chat_item_dictionary.get("id")
                    if message_id_string and message_id_string not in previously_seen_message_ids_set:
                        previously_seen_message_ids_queue.append(message_id_string)
                        previously_seen_message_ids_set.add(message_id_string)

                is_initial_chat_poll = False
                await wait_until_timeout_or_shutdown_event(youtube_api_polling_interval_milliseconds / 1000.0)
                continue

            processed_new_messages_count = 0

            for chat_item_dictionary in current_chat_items_list:
                message_id_string = chat_item_dictionary.get("id")
                if not message_id_string or message_id_string in previously_seen_message_ids_set:
                    continue

                if len(previously_seen_message_ids_queue) == 2000:
                    oldest_tracked_message_id = previously_seen_message_ids_queue.popleft()
                    previously_seen_message_ids_set.discard(oldest_tracked_message_id)

                previously_seen_message_ids_queue.append(message_id_string)
                previously_seen_message_ids_set.add(message_id_string)

                chat_snippet_dictionary = chat_item_dictionary.get("snippet", {})
                chat_author_details_dictionary = chat_item_dictionary.get("authorDetails", {})

                if chat_snippet_dictionary.get("type") != "textMessageEvent":
                    continue

                text_message_details_dictionary = chat_snippet_dictionary.get("textMessageDetails", {})
                raw_message_text_string = text_message_details_dictionary.get("messageText", "").strip()

                if not raw_message_text_string:
                    raw_message_text_string = chat_snippet_dictionary.get("displayMessage", "").strip()

                author_display_name_string = chat_author_details_dictionary.get("displayName", "unknown")

                cleaned_chat_message = clean_whitespace_and_youtube_emotes_from_message(raw_message_text_string)
                if is_text_empty_or_only_punctuation(cleaned_chat_message):
                    continue

                formatted_final_message = format_message_with_username_and_truncate(cleaned_chat_message, author_display_name_string, "@")
                if not chat_message_queue.full():
                    await chat_message_queue.put(("[YT]", formatted_final_message))
                    processed_new_messages_count += 1

            base_delay = float(config.get("YOUTUBE_POLL_BASE_DELAY_SECONDS", 2.0))
            multiplier = float(config.get("YOUTUBE_POLL_MULTIPLIER", 2.0))
            max_delay = float(config.get("YOUTUBE_MAX_POLL_DELAY_SECONDS", 30.0))

            if processed_new_messages_count > 0:
                current_exponential_backoff_delay = base_delay
            else:
                current_exponential_backoff_delay = min(current_exponential_backoff_delay * multiplier, max_delay)

            mandatory_api_sleep_minimum_seconds = youtube_api_polling_interval_milliseconds / 1000.0
            optimized_sleep_duration_seconds = max(mandatory_api_sleep_minimum_seconds, current_exponential_backoff_delay)

            await wait_until_timeout_or_shutdown_event(optimized_sleep_duration_seconds)

        except HttpError as http_error_exception:
            if check_if_error_is_youtube_quota_exhaustion(http_error_exception):
                print("\n*** YouTube API Quota exhausted! Shutting down YouTube listener. Twitch-to-Bilibili bridge remains 100% active. ***\n")
                return
            await wait_until_timeout_or_shutdown_event(5.0)

        except Exception:
            await wait_until_timeout_or_shutdown_event(5.0)

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
            stream_writer.write(b"CAP REQ :twitch.tv/tags\r\n")
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

                if "PRIVMSG" not in decoded_irc_line:
                    continue

                privmsg_regex_match = twitch_privmsg_pattern.match(decoded_irc_line)
                if not privmsg_regex_match:
                    continue

                twitch_tags_string = privmsg_regex_match.group(1)
                twitch_author_username = privmsg_regex_match.group(2)
                raw_chat_message = privmsg_regex_match.group(3)

                message_without_twitch_emotes = remove_twitch_emotes_from_message(raw_chat_message, twitch_tags_string)
                cleaned_chat_message = clean_whitespace_from_message(message_without_twitch_emotes)

                if is_text_empty_or_only_punctuation(cleaned_chat_message):
                    continue

                formatted_final_message = format_message_with_username_and_truncate(cleaned_chat_message, twitch_author_username, "#")

                if not chat_message_queue.full():
                    await chat_message_queue.put(("[TW]", formatted_final_message))

        except Exception:
            await wait_until_timeout_or_shutdown_event(3.0)

        finally:
            if stream_writer is not None:
                try:
                    stream_writer.close()
                    await stream_writer.wait_closed()
                except Exception:
                    pass

async def send_queued_messages_to_bilibili_live_room(chat_message_queue: asyncio.Queue):
    while not shutdown_event.is_set() or not chat_message_queue.empty():
        try:
            message_source_tag, chat_message_content = await asyncio.wait_for(chat_message_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            if bilibili_live_room:
                await bilibili_live_room.send_danmaku(Danmaku(chat_message_content))
            await asyncio.sleep(1.0)
        except Exception:
            await asyncio.sleep(1.0)
        finally:
            chat_message_queue.task_done()

async def run_youtube_and_twitch_to_bilibili_chat_bridge():
    bridge_message_queue = asyncio.Queue(maxsize=50)

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

@app.get("/api/config")
def api_get_config():
    return config

@app.get("/api/status")
def api_get_status():
    return {"running": bridge_task is not None and not bridge_task.done()}

@app.post("/api/config")
async def api_save_config(request: Request):
    global config
    new_config = await request.json()
    config.update(new_config)
    save_config()
    return {"status": "ok"}

@app.post("/api/update_backoff")
async def api_update_backoff(request: Request):
    global config
    new_config = await request.json()
    if "YOUTUBE_POLL_BASE_DELAY_SECONDS" in new_config:
        config["YOUTUBE_POLL_BASE_DELAY_SECONDS"] = float(new_config["YOUTUBE_POLL_BASE_DELAY_SECONDS"])
    if "YOUTUBE_POLL_MULTIPLIER" in new_config:
        config["YOUTUBE_POLL_MULTIPLIER"] = float(new_config["YOUTUBE_POLL_MULTIPLIER"])
    if "YOUTUBE_MAX_POLL_DELAY_SECONDS" in new_config:
        config["YOUTUBE_MAX_POLL_DELAY_SECONDS"] = float(new_config["YOUTUBE_MAX_POLL_DELAY_SECONDS"])
    save_config()
    return {"status": "ok"}

@app.post("/api/start")
async def api_start():
    global bridge_task, shutdown_event, bilibili_live_room, youtube_api_client
    
    if bridge_task and not bridge_task.done():
        return {"status": "already running"}
        
    shutdown_event = asyncio.Event()

    try:
        if config.get("SESSDATA"):
            bilibili_credential = Credential(
                sessdata=config.get("SESSDATA", ""),
                bili_jct=config.get("BILI_JCT", ""),
                buvid3=config.get("BUVID3", ""),
                dedeuserid=config.get("DEDEUSERID", ""),
            )
            bilibili_live_room = LiveRoom(int(config.get("BILIBILI_ROOM_ID", 0)), credential=bilibili_credential)
        else:
            bilibili_live_room = None
            
        yt_api_key = config.get("YOUTUBE_API_KEY", "")
        if yt_api_key:
            youtube_api_client = build("youtube", "v3", developerKey=yt_api_key)
        else:
            youtube_api_client = None
    except Exception as e:
        return {"status": "error", "message": str(e)}

    bridge_task = asyncio.create_task(run_youtube_and_twitch_to_bilibili_chat_bridge())
    return {"status": "started"}

@app.post("/api/stop")
async def api_stop():
    global shutdown_event, bridge_task
    if shutdown_event:
        shutdown_event.set()
    if bridge_task:
        await bridge_task
        bridge_task = None
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