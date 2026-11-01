"""QQ 防撤回插件（AstrBot / aiocqhttp / OneBot v11）。

缓存群聊与私聊中的文本、图片、视频消息，当消息被撤回时，
把原消息补发到原会话（或指定的转发目标会话）。

依赖 AstrBot >= 4.0.0，仅支持 aiocqhttp 平台。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from astrbot.core import AstrBotConfig
from astrbot.core.message.components import At, Image, Plain, Video
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

PLUGIN_NAME = "astrbot_plugin_qq_antirecall"
DEFAULT_HEADER = "【防撤回】{sender_name}({sender_id}) 撤回了一条消息："
DEFAULT_OPERATOR_HEADER = (
    "【防撤回】{sender_name}({sender_id}) 的消息被 "
    "{operator_name}({operator_id}) 撤回："
)


@dataclass(slots=True)
class CachedPart:
    """被缓存消息中的单个片段。"""

    kind: str  # text / image / video
    text: str = ""
    path: str = ""  # 已落盘的本地文件
    url: str = ""  # 远端地址，落盘失败时的兜底
    failed: bool = False
    """媒体既没有本地文件也没有可用地址。"""


@dataclass(slots=True)
class CachedMessage:
    """一条被缓存的消息。"""

    message_id: str
    chat_type: str  # group / private
    chat_id: str
    sender_id: str
    sender_name: str
    ts: float
    parts: list[CachedPart] = field(default_factory=list)
    recalled: bool = False


@register(
    PLUGIN_NAME,
    "bixie",
    "QQ 防撤回：缓存文本、图片、视频消息，撤回后自动补发",
    "v1.0.0",
    None,
)
class QqAntiRecallPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.conf = config or {}

        self.enabled = bool(self.conf.get("enabled", True))
        self.enable_group = bool(self.conf.get("enable_group", True))
        self.enable_private = bool(self.conf.get("enable_private", True))
        self.enable_text = bool(self.conf.get("enable_text", True))
        self.enable_image = bool(self.conf.get("enable_image", True))
        self.enable_video = bool(self.conf.get("enable_video", True))
        self.ignore_self = bool(self.conf.get("ignore_self", True))

        self.cache_ttl = max(10, self._as_int(self.conf.get("cache_ttl"), 300))
        self.max_entries = max(50, self._as_int(self.conf.get("max_entries"), 2000))
        self.max_text_len = max(100, self._as_int(self.conf.get("max_text_len"), 2000))

        self.download_media = bool(self.conf.get("download_media", True))
        self.max_media_mb = max(1, self._as_int(self.conf.get("max_media_mb"), 30))
        self.max_media_bytes = self.max_media_mb * 1024 * 1024
        self.media_timeout = max(5, self._as_int(self.conf.get("media_timeout"), 30))

        self.notify_template = str(
            self.conf.get("notify_template") or DEFAULT_HEADER
        )
        self.operator_template = str(
            self.conf.get("operator_template") or DEFAULT_OPERATOR_HEADER
        )
        self.silent_when_missing = bool(
            self.conf.get("silent_when_missing", True)
        )
        self.recall_wait_media = max(
            0, self._as_int(self.conf.get("recall_wait_media"), 8)
        )

        # 撤回内容的去向：log=只写日志（默认）/ session=发回原会话 / forward=发到指定目标
        delivery_mode = str(self.conf.get("delivery_mode") or "log").strip().lower()
        if delivery_mode not in ("log", "session", "forward"):
            delivery_mode = "log"
        self.delivery_mode = delivery_mode
        self.log_enabled = bool(self.conf.get("log_enabled", True))
        self.log_keep = max(50, self._as_int(self.conf.get("log_keep"), 1000))

        self.group_whitelist = {
            str(x).strip() for x in self._as_list(self.conf.get("group_whitelist"))
        }
        self.user_blacklist = {
            str(x).strip() for x in self._as_list(self.conf.get("user_blacklist"))
        }
        self.forward_targets = [
            t for t in (self._parse_target(x) for x in self._as_list(self.conf.get("forward_targets"))) if t
        ]

        self._cache: dict[str, CachedMessage] = {}
        self._pending: dict[str, list[asyncio.Task]] = {}
        self._gc_task: asyncio.Task | None = None
        self._last_gc = 0.0

        self.media_dir = (
            Path(get_astrbot_plugin_data_path()) / PLUGIN_NAME / "media"
        )
        self.log_path = (
            Path(get_astrbot_plugin_data_path()) / PLUGIN_NAME / "recall_log.jsonl"
        )
        self._dirs_ready = False

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_list(value: Any) -> list:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [x for x in value.replace("，", ",").split(",") if x.strip()]
        return [value]

    @staticmethod
    def _parse_target(raw: Any) -> tuple[str, str] | None:
        """解析转发目标。

        支持：#群号 / @QQ号 / group:群号 / private:QQ号 / 纯数字（默认群）。
        """
        text = str(raw).strip()
        if not text:
            return None
        if text.startswith("#"):
            cid = text[1:].strip()
            return ("group", cid) if cid.isdigit() else None
        if text.startswith("@"):
            cid = text[1:].strip()
            return ("private", cid) if cid.isdigit() else None
        if ":" in text:
            kind, _, cid = text.partition(":")
            kind = kind.strip().lower()
            cid = cid.strip()
            if kind in ("group", "群", "群聊"):
                return ("group", cid) if cid.isdigit() else None
            if kind in ("private", "私聊", "user", "friend"):
                return ("private", cid) if cid.isdigit() else None
            return None
        return ("group", text) if text.isdigit() else None

    def _ensure_dirs(self) -> None:
        if self._dirs_ready:
            return
        try:
            self.media_dir.mkdir(parents=True, exist_ok=True)
            self._dirs_ready = True
        except OSError as e:
            self.logger.error(f"创建媒体缓存目录失败: {e}")

    # -------------------------------------------------------------- lifecycle

    async def initialize(self) -> None:
        self._ensure_dirs()
        self._gc_task = asyncio.create_task(self._gc_loop())
        self.logger.info(
            "QQ 防撤回已启动：群聊=%s 私聊=%s 文本=%s 图片=%s 视频=%s TTL=%ss",
            self.enable_group,
            self.enable_private,
            self.enable_text,
            self.enable_image,
            self.enable_video,
            self.cache_ttl,
        )

    async def terminate(self) -> None:
        if self._gc_task:
            self._gc_task.cancel()
            self._gc_task = None
        self._cache.clear()

    async def _gc_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                self._prune()
            except Exception as e:  # 后台任务不能因单次异常退出
                self.logger.error(f"缓存清理失败: {e}")

    # ---------------------------------------------------------------- caching

    def _should_cache(self, chat_type: str, chat_id: str, sender_id: str) -> bool:
        if not self.enabled:
            return False
        if chat_type == "group":
            if not self.enable_group:
                return False
            if self.group_whitelist and chat_id not in self.group_whitelist:
                return False
        else:
            if not self.enable_private:
                return False
        if sender_id in self.user_blacklist:
            return False
        return True

    def _prune(self) -> None:
        """清理过期缓存和磁盘上的孤儿媒体文件。"""
        now = time.time()
        expired = [
            mid for mid, item in self._cache.items() if now - item.ts > self.cache_ttl
        ]
        for mid in expired:
            self._drop(mid)

        while len(self._cache) > self.max_entries:
            oldest = next(iter(self._cache))
            self._drop(oldest)

        self._last_gc = now
        self._clean_orphan_files(now)

    def _drop(self, message_id: str) -> None:
        item = self._cache.pop(message_id, None)
        if not item:
            return
        for part in item.parts:
            self._remove_file(part.path)

    @staticmethod
    def _remove_file(path: str) -> None:
        if not path:
            return
        try:
            os.remove(path)
        except OSError:
            pass

    def _clean_orphan_files(self, now: float) -> None:
        if not self._dirs_ready:
            return
        cutoff = now - max(self.cache_ttl * 2, 600)
        try:
            for entry in self.media_dir.iterdir():
                try:
                    if entry.is_file() and entry.stat().st_mtime < cutoff:
                        entry.unlink(missing_ok=True)
                except OSError:
                    continue
        except OSError:
            return

    def _on_message(self, event: AstrMessageEvent, raw: dict) -> None:
        """缓存一条普通消息中的文本/图片/视频。"""
        message_obj = event.message_obj
        sender = message_obj.sender
        sender_id = str(sender.user_id) if sender else ""
        if not sender_id:
            return

        chat_type = "group" if message_obj.group_id else "private"
        chat_id = str(message_obj.group_id or sender_id)

        if not self._should_cache(chat_type, chat_id, sender_id):
            return
        if self.ignore_self and sender_id == str(event.get_self_id()):
            return

        parts: list[CachedPart] = []
        media_jobs: list[tuple[CachedPart, Any]] = []

        for comp in message_obj.message or []:
            if isinstance(comp, Plain):
                if self.enable_text and comp.text.strip():
                    parts.append(CachedPart(kind="text", text=comp.text))
            elif isinstance(comp, At):
                if self.enable_text:
                    label = f"@{comp.name}" if getattr(comp, "name", "") else "@某人"
                    parts.append(CachedPart(kind="text", text=label))
            elif isinstance(comp, Image):
                if not self.enable_image:
                    continue
                part = CachedPart(
                    kind="image",
                    url=self._comp_url(comp),
                )
                parts.append(part)
                if self.download_media:
                    media_jobs.append((part, comp))
            elif isinstance(comp, Video):
                if not self.enable_video:
                    continue
                part = CachedPart(
                    kind="video",
                    url=self._comp_url(comp),
                )
                parts.append(part)
                if self.download_media:
                    media_jobs.append((part, comp))

        if not parts:
            return

        # 长文本截断，避免缓存占用过大
        for part in parts:
            if part.kind == "text" and len(part.text) > self.max_text_len:
                part.text = part.text[: self.max_text_len] + "…"

        message_id = str(message_obj.message_id or raw.get("message_id") or "")
        if not message_id:
            return

        cached = CachedMessage(
            message_id=message_id,
            chat_type=chat_type,
            chat_id=chat_id,
            sender_id=sender_id,
            sender_name=str(getattr(sender, "nickname", "") or sender_id),
            ts=time.time(),
            parts=parts,
        )
        self._cache[message_id] = cached

        if media_jobs:
            task = asyncio.create_task(self._fill_media_batch(media_jobs))
            self._pending[message_id] = [task]
            task.add_done_callback(
                lambda _t, mid=message_id: self._forget_pending(mid)
            )

        if time.time() - self._last_gc > 60:
            self._prune()

    def _forget_pending(self, message_id: str) -> None:
        self._pending.pop(message_id, None)

    async def _fill_media_batch(self, jobs: list[tuple[CachedPart, Any]]) -> None:
        try:
            await asyncio.gather(*(self._fill_media(part, comp) for part, comp in jobs))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.debug(f"批量媒体缓存异常: {e}")

    @staticmethod
    def _comp_url(comp: Any) -> str:
        """尽力取出组件的远端地址，落盘失败时用于兜底发送。"""
        for attr in ("url", "file"):
            value = getattr(comp, attr, "") or ""
            if isinstance(value, str) and (
                value.startswith("http://") or value.startswith("https://")
            ):
                return value
        return ""

    async def _fill_media(self, part: CachedPart, comp: Any) -> None:
        """把媒体下载/转换成本地文件，供撤回后重新发送。"""
        try:
            path = await asyncio.wait_for(
                comp.convert_to_file_path(), timeout=self.media_timeout
            )
            if not path or not os.path.isfile(path):
                part.failed = True
                return
            size = os.path.getsize(path)
            if size <= 0 or size > self.max_media_bytes:
                self.logger.debug(
                    "跳过媒体缓存：%s 大小 %s 字节，超过限制 %s 字节",
                    part.kind,
                    size,
                    self.max_media_bytes,
                )
                part.failed = True
                return
            part.path = self._store_media(path, part.kind)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            part.failed = True
            self.logger.debug(f"媒体缓存失败（{part.kind}）: {e}")

    def _store_media(self, src: str, kind: str) -> str:
        self._ensure_dirs()
        if not self._dirs_ready:
            return ""
        ext = os.path.splitext(src)[1].lower()
        if not ext or len(ext) > 6:
            ext = ".jpg" if kind == "image" else ".mp4"
        name = f"{int(time.time())}_{uuid.uuid4().hex[:10]}{ext}"
        dst = self.media_dir / name
        shutil.copyfile(src, dst)
        return str(dst)

    async def _wait_pending(self, message_id: str, timeout: float = 4.0) -> None:
        tasks = [t for t in self._pending.get(message_id, []) if not t.done()]
        if not tasks:
            return
        try:
            await asyncio.wait(tasks, timeout=timeout)
        except Exception:
            pass

    # --------------------------------------------------------------- recall

    async def _on_recall(self, event: AstrMessageEvent, raw: dict) -> None:
        notice_type = str(raw.get("notice_type") or "")
        message_id = str(raw.get("message_id") or "")
        if not message_id:
            return

        if notice_type == "group_recall":
            chat_type = "group"
            chat_id = str(raw.get("group_id") or "")
            sender_id = str(raw.get("user_id") or "")
        elif notice_type == "friend_recall":
            chat_type = "private"
            chat_id = str(raw.get("user_id") or "")
            sender_id = str(raw.get("user_id") or "")
        else:
            return

        if not self.enabled:
            return
        if chat_type == "group":
            if not self.enable_group:
                return
            if self.group_whitelist and chat_id not in self.group_whitelist:
                return
        elif not self.enable_private:
            return
        if sender_id in self.user_blacklist:
            return

        await self._wait_pending(message_id, timeout=self.recall_wait_media)
        cached = self._cache.get(message_id)
        if not cached:
            if not self.silent_when_missing:
                await self._send_plain(
                    event,
                    raw,
                    chat_type,
                    chat_id,
                    "【防撤回】有一条消息被撤回了，但原内容不在缓存中（可能已过期）。",
                )
            return

        bot = getattr(event, "bot", None)
        if bot is None:
            self.logger.error("无法获取 aiocqhttp bot 实例，跳过撤回补发")
            return
        if cached.recalled:
            return
        cached.recalled = True

        operator_id = str(raw.get("operator_id") or sender_id)
        operator_name = await self._operator_name(
            event, bot, chat_type, chat_id, operator_id
        )
        header = self._format_header(cached, operator_id, operator_name)

        # 先落日志：即使后面发送失败，记录也不会丢
        await self._write_log(self._build_record(cached, operator_id, operator_name))

        if self.delivery_mode == "session":
            targets = [(cached.chat_type, cached.chat_id)]
        elif self.delivery_mode == "forward":
            targets = self.forward_targets or [(cached.chat_type, cached.chat_id)]
        else:  # log：只记录，不发到任何会话
            targets = []

        for target_type, target_id in targets:
            try:
                await self._deliver(bot, raw, target_type, target_id, cached, header)
            except Exception as e:
                self.logger.error(f"补发撤回消息失败（{target_type}:{target_id}）: {e}")

        self.logger.info(
            "%s撤回消息 %s（%s:%s，发送者 %s，操作者 %s，去向 %s）",
            "已补发" if targets else "已记录",
            message_id,
            chat_type,
            chat_id,
            sender_id,
            operator_id,
            ",".join(f"{k}:{v}" for k, v in targets) or "仅日志",
        )

    async def _operator_name(
        self,
        event: AstrMessageEvent,
        bot: Any,
        chat_type: str,
        chat_id: str,
        operator_id: str,
    ) -> str:
        """撤回操作者昵称：优先从事件里取，其次查协议端，最后用 QQ 号。"""
        try:
            raw = getattr(event.message_obj, "raw_message", None) or {}
            sender = raw.get("sender") or {}
            if str(sender.get("user_id") or "") == operator_id:
                name = sender.get("card") or sender.get("nickname")
                if name:
                    return str(name)
        except Exception:
            pass

        if not operator_id.isdigit():
            return operator_id
        try:
            if chat_type == "group" and chat_id.isdigit():
                info = await asyncio.wait_for(
                    bot.call_action(
                        "get_group_member_info",
                        group_id=int(chat_id),
                        user_id=int(operator_id),
                        no_cache=False,
                    ),
                    timeout=2,
                )
            else:
                info = await asyncio.wait_for(
                    bot.call_action(
                        "get_stranger_info",
                        user_id=int(operator_id),
                        no_cache=False,
                    ),
                    timeout=2,
                )
            name = (info or {}).get("card") or (info or {}).get("nickname") or (
                info or {}
            ).get("nick")
            if name:
                return str(name)
        except Exception:
            pass
        return operator_id

    def _format_header(
        self, cached: CachedMessage, operator_id: str, operator_name: str
    ) -> str:
        is_operator = bool(operator_id) and operator_id != cached.sender_id
        template = self.operator_template if is_operator else self.notify_template
        values = {
            "sender_name": cached.sender_name,
            "sender_id": cached.sender_id,
            "operator_name": operator_name,
            "operator_id": operator_id,
            "group_id": cached.chat_id if cached.chat_type == "group" else "",
            "chat_id": cached.chat_id,
            "time": time.strftime("%H:%M:%S", time.localtime(cached.ts)),
        }
        try:
            return template.format(**values)
        except (KeyError, IndexError, ValueError):
            return DEFAULT_HEADER.format(**values)

    # ------------------------------------------------------------ recall log

    def _build_record(
        self, cached: CachedMessage, operator_id: str, operator_name: str
    ) -> dict:
        """把一条撤回整理成结构化日志记录。"""
        texts = [p.text for p in cached.parts if p.kind == "text" and p.text]
        images = [p for p in cached.parts if p.kind == "image"]
        videos = [p for p in cached.parts if p.kind == "video"]
        text = "\n".join(texts)
        if len(text) > 1000:
            text = text[:1000] + "…"
        return {
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "ts": round(cached.ts, 3),
            "chat_type": cached.chat_type,
            "chat_id": cached.chat_id,
            "sender_id": cached.sender_id,
            "sender_name": cached.sender_name,
            "operator_id": operator_id,
            "operator_name": operator_name,
            "self_recall": operator_id == cached.sender_id,
            "text": text,
            "images": len(images),
            "videos": len(videos),
            "media": [p.path for p in images + videos if p.path],
            "message_id": cached.message_id,
        }

    async def _write_log(self, record: dict) -> None:
        if not self.log_enabled:
            return
        try:
            await asyncio.to_thread(self._append_log, record)
        except Exception as e:
            self.logger.error(f"写入撤回日志失败: {e}")
        preview = record["text"].replace("\n", " ")
        if len(preview) > 80:
            preview = preview[:80] + "…"
        self.logger.info(
            "撤回记录 | %s %s | 被撤回 %s(%s) | 操作者 %s(%s) | 文本:%s 图片:%d 视频:%d",
            "群" if record["chat_type"] == "group" else "私聊",
            record["chat_id"],
            record["sender_name"],
            record["sender_id"],
            record["operator_name"],
            record["operator_id"],
            preview or "（无）",
            record["images"],
            record["videos"],
        )

    def _append_log(self, record: dict) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        self._trim_log()

    def _trim_log(self) -> None:
        """日志超过 2MB 时只保留最近 log_keep 条，避免无限增长。"""
        try:
            if self.log_path.stat().st_size < 2 * 1024 * 1024:
                return
            with open(self.log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.writelines(lines[-self.log_keep :])
        except OSError:
            pass

    def _read_logs(self, limit: int, chat_id: str | None = None) -> list[dict]:
        if not self.log_path.exists():
            return []
        records: list[dict] = []
        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if chat_id and str(rec.get("chat_id")) != chat_id:
                        continue
                    records.append(rec)
        except OSError:
            return []
        return records[-limit:]

    @staticmethod
    def _format_records(records: list[dict]) -> str:
        if not records:
            return "还没有撤回记录。"
        lines = [f"最近 {len(records)} 条撤回记录："]
        for i, rec in enumerate(records, 1):
            where = "群" if rec.get("chat_type") == "group" else "私聊"
            if rec.get("self_recall"):
                who = "本人撤回"
            else:
                who = "被 {} ({}) 撤回".format(
                    rec.get("operator_name"), rec.get("operator_id")
                )
            lines.append(
                f"{i}. {rec.get('time')} | {where} {rec.get('chat_id')}"
                f"\n   被撤回：{rec.get('sender_name')}({rec.get('sender_id')}) · {who}"
            )
            extra = []
            if rec.get("images"):
                extra.append(f"图片×{rec['images']}")
            if rec.get("videos"):
                extra.append(f"视频×{rec['videos']}")
            text = (rec.get("text") or "").replace("\n", " ")
            if len(text) > 80:
                text = text[:80] + "…"
            if text:
                content = f"   内容：{text}"
                if extra:
                    content += "  [{}]".format(" ".join(extra))
            elif extra:
                content = "   内容：[{}]".format(" ".join(extra))
            else:
                content = "   内容：（空）"
            lines.append(content)
        return "\n".join(lines)

    def _clear_log(self) -> None:
        try:
            self.log_path.unlink(missing_ok=True)
        except OSError:
            pass

    # ---------------------------------------------------------------- sending

    async def _deliver(
        self,
        bot: Any,
        raw_event: Any,
        chat_type: str,
        chat_id: str,
        cached: CachedMessage,
        header: str,
    ) -> None:
        """先发提示 + 文本 + 图片，再逐个发送视频。"""
        chain = MessageChain()
        if header:
            chain.chain.append(Plain(header))

        texts = [p.text for p in cached.parts if p.kind == "text" and p.text]
        if texts:
            chain.chain.append(Plain("\n".join(texts)))

        images: list[Any] = []
        videos: list[CachedPart] = []
        for part in cached.parts:
            if part.kind == "image":
                comp = self._media_component(part)
                if comp is not None:
                    images.append(comp)
            elif part.kind == "video":
                videos.append(part)

        for comp in images:
            chain.chain.append(comp)

        if len(chain.chain) > 1 or (header and not videos):
            try:
                await self._send_chain(bot, raw_event, chat_type, chat_id, chain)
            except Exception as e:
                self.logger.error(f"撤回消息主内容发送失败: {e}")
                if images:
                    await self._send_text_fallback(
                        bot, raw_event, chat_type, chat_id, header, cached
                    )
        elif header and videos:
            # 只有视频时，提示语单独一条，避免部分协议端丢弃混合消息
            await self._send_chain(
                bot, raw_event, chat_type, chat_id, MessageChain(chain=[Plain(header)])
            )

        for part in videos:
            comp = self._media_component(part)
            if comp is None:
                continue
            try:
                await self._send_chain(
                    bot, raw_event, chat_type, chat_id, MessageChain(chain=[comp])
                )
            except Exception as e:
                # 本地文件发送失败时退回远端地址再试一次
                if part.url:
                    try:
                        await self._send_chain(
                            bot,
                            raw_event,
                            chat_type,
                            chat_id,
                            MessageChain(chain=[Video(file=part.url)]),
                        )
                        continue
                    except Exception as e2:
                        self.logger.error(f"视频补发失败: {e2}")
                self.logger.error(f"视频补发失败: {e}")

    @staticmethod
    def _media_component(part: CachedPart) -> Any:
        if part.path and os.path.isfile(part.path):
            if part.kind == "image":
                return Image.fromFileSystem(part.path)
            return Video.fromFileSystem(part.path)
        if part.url:
            if part.kind == "image":
                return Image(file=part.url)
            return Video(file=part.url)
        return None

    async def _send_text_fallback(
        self,
        bot: Any,
        raw_event: Any,
        chat_type: str,
        chat_id: str,
        header: str,
        cached: CachedMessage,
    ) -> None:
        """图片等媒体发送失败时，至少把文字内容补发出去。"""
        try:
            texts = [p.text for p in cached.parts if p.kind == "text" and p.text]
            text = header or ""
            if texts:
                text = f"{text}\n" + "\n".join(texts) if text else "\n".join(texts)
            text = f"{text}\n（原消息中的图片/视频发送失败，可能已过期）"
            await self._send_chain(
                bot, raw_event, chat_type, chat_id, MessageChain(chain=[Plain(text)])
            )
        except Exception as e:
            self.logger.error(f"文字兜底发送失败: {e}")

    async def _send_chain(
        self,
        bot: Any,
        raw_event: Any,
        chat_type: str,
        chat_id: str,
        chain: MessageChain,
    ) -> None:
        await AiocqhttpMessageEvent.send_message(
            bot=bot,
            message_chain=chain,
            event=raw_event,
            is_group=chat_type == "group",
            session_id=str(chat_id),
        )

    async def _send_plain(
        self,
        event: AstrMessageEvent,
        raw_event: Any,
        chat_type: str,
        chat_id: str,
        text: str,
    ) -> None:
        bot = getattr(event, "bot", None)
        if bot is None:
            return
        try:
            await self._send_chain(
                bot,
                raw_event,
                chat_type,
                chat_id,
                MessageChain(chain=[Plain(text)]),
            )
        except Exception as e:
            self.logger.error(f"发送提示失败: {e}")

    # --------------------------------------------------------------- handlers

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=1000)
    async def on_any_event(self, event: AstrMessageEvent):
        """统一入口：普通消息入缓存，撤回通知触发补发。"""
        raw = getattr(event.message_obj, "raw_message", None)
        if not isinstance(raw, dict):
            return
        post_type = raw.get("post_type")
        try:
            if post_type == "message":
                self._on_message(event, raw)
            elif post_type == "notice" and raw.get("notice_type") in (
                "group_recall",
                "friend_recall",
            ):
                await self._on_recall(event, raw)
        except Exception as e:
            self.logger.error(f"处理事件失败: {e}", exc_info=True)

    @filter.command("防撤回状态")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def anti_recall_status(self, event: AstrMessageEvent):
        """查看防撤回插件的运行状态。"""
        lines = [
            f"防撤回：{'开启' if self.enabled else '关闭'}",
            f"群聊/私聊：{'开' if self.enable_group else '关'}/{'开' if self.enable_private else '关'}",
            f"文本/图片/视频：{'开' if self.enable_text else '关'}/{'开' if self.enable_image else '关'}/{'开' if self.enable_video else '关'}",
            f"缓存：{len(self._cache)} 条，保留 {self.cache_ttl} 秒",
            f"媒体落盘：{'开' if self.download_media else '关'}（上限 {self.max_media_mb}MB）",
            f"撤回内容去向：{self._mode_text()}",
            f"转发目标：{self._targets_brief()}",
            f"撤回日志：{'开' if self.log_enabled else '关'}",
        ]
        yield event.plain_result("\n".join(lines))

    def _mode_text(self) -> str:
        return {
            "log": "仅写日志（不发到群里）",
            "session": "发回原会话",
            "forward": "发到指定目标",
        }.get(self.delivery_mode, self.delivery_mode)

    @filter.command("撤回转发")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def set_forward_target(self, event: AstrMessageEvent):
        """设置撤回内容的转发目标（群或人）。"""
        text = (event.get_message_str() or "").strip()
        matched = re.search(r"撤回转发\s*(.*)$", text)
        arg = (matched.group(1) if matched else "").strip()

        if not arg or arg in ("查看", "状态", "list"):
            yield event.plain_result(self._format_targets())
            return
        if arg in ("清空", "重置", "取消", "clear", "reset"):
            await self._save_targets([])
            yield event.plain_result("已清空转发目标，撤回内容恢复发送到原会话。")
            return

        targets: list[tuple[str, str]] = []
        invalid: list[str] = []
        for token in arg.split():
            parsed = self._parse_target(token)
            if parsed:
                if parsed not in targets:
                    targets.append(parsed)
            else:
                invalid.append(token)

        if not targets:
            yield event.plain_result(
                "没识别到有效目标。\n"
                "用法：/撤回转发 #群号 @QQ号（可写多个）\n"
                "· # 开头表示群，@ 开头表示私聊\n"
                "· 查看：/撤回转发 查看；恢复原会话：/撤回转发 清空",
            )
            return

        await self._save_targets(targets)
        msg = "已设置转发目标，撤回内容只发到：\n" + "\n".join(
            f"· {'群' if k == 'group' else '私聊'} {v}" for k, v in targets
        )
        msg += "\n不再发回原会话。"
        if invalid:
            msg += f"\n已忽略无法识别的内容：{' '.join(invalid)}"
        yield event.plain_result(msg)

    @filter.command("撤回记录")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def view_recall_log(self, event: AstrMessageEvent):
        """查看最近的撤回记录：/撤回记录 [数量] [#群号]；/撤回记录 清空。"""
        text = (event.get_message_str() or "").strip()
        matched = re.search(r"撤回记录\s*(.*)$", text)
        arg = (matched.group(1) if matched else "").strip()

        if arg in ("清空", "清除", "clear"):
            await asyncio.to_thread(self._clear_log)
            yield event.plain_result("撤回记录已清空。")
            return

        limit = 10
        chat_id = None
        for token in arg.split():
            if token.startswith("#") and token[1:].isdigit():
                chat_id = token[1:]
            elif token.isdigit():
                limit = min(50, max(1, int(token)))

        records = await asyncio.to_thread(self._read_logs, limit, chat_id)
        yield event.plain_result(self._format_records(records))

    def _targets_brief(self) -> str:
        if not self.forward_targets:
            return "原会话"
        return "、".join(
            f"{'群' if k == 'group' else '私聊'}{v}" for k, v in self.forward_targets
        )

    def _format_targets(self) -> str:
        if not self.forward_targets:
            return (
                "当前未设置转发目标，撤回内容发送到原会话。\n"
                "设置：/撤回转发 #群号 @QQ号（可多个）；清空：/撤回转发 清空"
            )
        lines = ["当前转发目标（撤回内容只发到这些会话）："]
        for kind, chat_id in self.forward_targets:
            lines.append(f"· {'群' if kind == 'group' else '私聊'} {chat_id}")
        lines.append("修改：/撤回转发 #群号 @QQ号；恢复原会话：/撤回转发 清空")
        return "\n".join(lines)

    async def _save_targets(self, targets: list[tuple[str, str]]) -> None:
        self.forward_targets = targets
        try:
            self.conf["forward_targets"] = [f"{k}:{v}" for k, v in targets]
            saver = getattr(self.conf, "save_config_async", None)
            if callable(saver):
                await saver()
            else:
                self.conf.save_config()
        except Exception as e:
            self.logger.error(f"保存转发目标失败: {e}")
