from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.autoreact")

CUSTOM_EMOJI_RE = re.compile(r"^<?a?:[A-Za-z0-9_]{2,32}:\d{2,}>?$")


class AutoReact(commands.Cog):
    """설정된 채널의 모든 새 메시지에 지정 이모지를 자동 반응합니다."""

    __author__ = "Codex"
    __version__ = "1.0.0"

    DEFAULT_QUEUE_MAXSIZE = 1000
    RATELIMIT_TUNING_ENABLED = False

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA710A67E, force_registration=True)

        self.config.register_guild(
            enabled=False,
            channel_id=None,
            emoji_raw=None,
            ignore_bots=False,
            ignore_webhooks=False,
            per_item_delay_ms=350,
            max_retry=1,
            auto_disable_on_forbidden=False,
            logging_enabled=False,
            log_channel_id=None,
        )

        self._queues: Dict[int, asyncio.Queue[discord.Message]] = {}
        self._workers: Dict[int, asyncio.Task] = {}
        self._dropped_count: defaultdict[int, int] = defaultdict(int)
        self._failure_count: defaultdict[int, int] = defaultdict(int)
        self._forbidden_warned_at: Dict[Tuple[int, int], float] = {}

    async def cog_unload(self) -> None:
        for task in self._workers.values():
            task.cancel()
        self._workers.clear()
        self._queues.clear()

    async def red_delete_data_for_user(self, **kwargs: Any) -> None:
        return

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        guild = message.guild
        if guild is None:
            return

        settings = await self.config.guild(guild).all()
        if not settings["enabled"]:
            return

        channel_id = settings["channel_id"]
        emoji_raw = settings["emoji_raw"]
        if channel_id is None or emoji_raw is None:
            return

        if message.channel.id != channel_id:
            return

        if settings["ignore_bots"] and message.author.bot:
            return

        if settings["ignore_webhooks"] and message.webhook_id is not None:
            return

        queue = self._queues.get(guild.id)
        if queue is None:
            queue = asyncio.Queue(maxsize=self.DEFAULT_QUEUE_MAXSIZE)
            self._queues[guild.id] = queue

        self._ensure_worker(guild.id)

        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            self._dropped_count[guild.id] += 1
            if settings["logging_enabled"]:
                await self._maybe_log(
                    guild,
                    f"AutoReact queue is full ({self.DEFAULT_QUEUE_MAXSIZE}); new messages are being dropped.",
                )

    def _ensure_worker(self, guild_id: int) -> None:
        task = self._workers.get(guild_id)
        if task and not task.done():
            return

        queue = self._queues[guild_id]
        self._workers[guild_id] = asyncio.create_task(self._worker_loop(guild_id, queue))

    async def _worker_loop(self, guild_id: int, queue: asyncio.Queue[discord.Message]) -> None:
        while True:
            try:
                message = await queue.get()
                try:
                    await self._process_message(message)
                finally:
                    queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("AutoReact worker crashed unexpectedly in guild %s", guild_id)
                await asyncio.sleep(1.0)

    async def _process_message(self, message: discord.Message) -> None:
        guild = message.guild
        if guild is None:
            return

        settings = await self.config.guild(guild).all()
        if not settings["enabled"]:
            return

        channel_id = settings["channel_id"]
        emoji_raw = settings["emoji_raw"]
        if channel_id is None or emoji_raw is None:
            return

        if message.channel.id != channel_id:
            return

        emoji = self._to_runtime_emoji(emoji_raw)
        if emoji is None:
            self._failure_count[guild.id] += 1
            return

        if self._has_same_reaction(message, emoji):
            await self._sleep_delay(settings)
            return

        max_retry = max(0, int(settings["max_retry"]))
        attempt = 0
        while True:
            try:
                await message.add_reaction(emoji)
                break
            except discord.Forbidden:
                self._failure_count[guild.id] += 1
                await self._handle_forbidden(message, settings)
                break
            except discord.NotFound:
                break
            except discord.HTTPException as exc:
                self._failure_count[guild.id] += 1
                if attempt < max_retry and (exc.status == 429 or 500 <= exc.status < 600):
                    attempt += 1
                    await asyncio.sleep(1.0 + (attempt * 2.0))
                    continue
                break

        await self._sleep_delay(settings)

    async def _handle_forbidden(self, message: discord.Message, settings: Dict[str, Any]) -> None:
        guild = message.guild
        if guild is None:
            return

        channel_id = message.channel.id
        warn_key = (guild.id, channel_id)
        now = time.monotonic()
        last_warn = self._forbidden_warned_at.get(warn_key, 0.0)

        if now - last_warn >= 60.0:
            self._forbidden_warned_at[warn_key] = now
            if settings["logging_enabled"]:
                await self._maybe_log(
                    guild,
                    f"AutoReact missing permissions in <#{channel_id}> (need View Channel / Read Message History / Add Reactions).",
                )

        if settings["auto_disable_on_forbidden"]:
            await self.config.guild(guild).enabled.set(False)
            if settings["logging_enabled"]:
                await self._maybe_log(
                    guild,
                    "AutoReact has been automatically disabled due to Forbidden errors.",
                )

    async def _sleep_delay(self, settings: Dict[str, Any]) -> None:
        delay_ms = int(settings["per_item_delay_ms"])
        delay_ms = min(2000, max(100, delay_ms))
        await asyncio.sleep(delay_ms / 1000)

    def _to_runtime_emoji(self, emoji_raw: str) -> Optional[Union[str, discord.PartialEmoji, discord.Emoji]]:
        parsed = discord.PartialEmoji.from_str(emoji_raw)
        if parsed.id is not None:
            real_emoji = self.bot.get_emoji(parsed.id)
            return real_emoji or parsed
        if parsed.name:
            return parsed.name
        return None

    def _has_same_reaction(self, message: discord.Message, emoji: Union[str, discord.PartialEmoji, discord.Emoji]) -> bool:
        target = self._emoji_key(emoji)
        for reaction in message.reactions:
            if self._emoji_key(reaction.emoji) == target:
                return True
        return False

    def _emoji_key(self, emoji: Union[str, discord.PartialEmoji, discord.Emoji]) -> str:
        if isinstance(emoji, str):
            return f"u:{emoji}"
        if isinstance(emoji, discord.Emoji):
            prefix = "a" if emoji.animated else "s"
            return f"c:{prefix}:{emoji.id}"
        prefix = "a" if emoji.animated else "s"
        if emoji.id is not None:
            return f"c:{prefix}:{emoji.id}"
        return f"u:{emoji.name or ''}"

    async def _maybe_log(self, guild: discord.Guild, text: str) -> None:
        settings = await self.config.guild(guild).all()
        if not settings["logging_enabled"]:
            return

        log_channel_id = settings["log_channel_id"]
        if not log_channel_id:
            return

        channel = guild.get_channel(log_channel_id)
        if channel is None or not isinstance(channel, discord.TextChannel):
            return

        try:
            await channel.send(text)
        except discord.HTTPException:
            pass

    async def _send_component(
        self,
        ctx: commands.Context,
        title: str,
        lines: Optional[List[str]] = None,
    ) -> None:
        body_lines = lines or []
        view = discord.ui.LayoutView(timeout=60)
        view.add_item(discord.ui.TextDisplay(f"## {title}"))
        if body_lines:
            view.add_item(discord.ui.Separator(visible=True))
            view.add_item(discord.ui.TextDisplay("\n".join(body_lines)))
        await ctx.send(view=view)

    def _validate_emoji_input(self, emoji_raw: str) -> Tuple[bool, Optional[str]]:
        value = emoji_raw.strip()
        if not value:
            return False, None

        if CUSTOM_EMOJI_RE.match(value):
            return True, value

        # Treat non-ASCII single-token input as unicode emoji candidate.
        if any(ord(ch) > 127 for ch in value) and all(not ch.isspace() for ch in value):
            return True, value

        parsed = discord.PartialEmoji.from_str(value)
        if parsed.id is not None and parsed.name:
            return True, value

        return False, None

    def _parse_on_off(self, value: str) -> Optional[bool]:
        lowered = value.strip().lower()
        if lowered in {"on", "true", "yes", "y", "1"}:
            return True
        if lowered in {"off", "false", "no", "n", "0"}:
            return False
        return None

    @commands.guild_only()
    @commands.group(name="autoreact", invoke_without_command=True)
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_group(self, ctx: commands.Context) -> None:
        """자동 반응 설정을 관리합니다."""
        await self._send_component(
            ctx,
            "AutoReact 명령 안내",
            [
                "`[p]autoreact setchannel <#채널|none>`",
                "`[p]autoreact setemoji <이모지>`",
                "`[p]autoreact enable` / `[p]autoreact disable`",
                "`[p]autoreact status`",
                "`[p]autoreact set ignorebots <on|off>`",
                "`[p]autoreact set ignorewebhooks <on|off>`",
            ],
        )

    @autoreact_group.command(name="enable")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_enable(self, ctx: commands.Context) -> None:
        """자동 반응을 활성화합니다."""
        settings = await self.config.guild(ctx.guild).all()
        missing = []
        if settings["channel_id"] is None:
            missing.append("channel")
        if settings["emoji_raw"] is None:
            missing.append("emoji")

        if missing:
            await self._send_component(
                ctx,
                "활성화 실패",
                [f"누락된 설정: {', '.join(missing)}"],
            )
            return

        await self.config.guild(ctx.guild).enabled.set(True)
        await self._send_component(ctx, "AutoReact를 활성화했습니다.")
        await self._maybe_log(ctx.guild, f"{ctx.author.mention} 님이 AutoReact를 활성화했습니다.")

    @autoreact_group.command(name="disable")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_disable(self, ctx: commands.Context) -> None:
        """자동 반응을 비활성화합니다."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await self._send_component(ctx, "AutoReact를 비활성화했습니다.")
        await self._maybe_log(ctx.guild, f"{ctx.author.mention} 님이 AutoReact를 비활성화했습니다.")

    @autoreact_group.command(name="setchannel")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_setchannel(self, ctx: commands.Context, *, channel: Optional[str] = None) -> None:
        """대상 채널을 설정합니다. 채널 멘션 또는 `none`을 사용하세요."""
        if channel is None or channel.strip().lower() == "none":
            await self.config.guild(ctx.guild).channel_id.set(None)
            await self.config.guild(ctx.guild).enabled.set(False)
            await self._send_component(
                ctx,
                "대상 채널 설정 해제",
                ["오동작 방지를 위해 AutoReact도 함께 비활성화했습니다."],
            )
            await self._maybe_log(
                ctx.guild,
                f"{ctx.author.mention} 님이 AutoReact 대상 채널을 해제하여 기능이 비활성화되었습니다.",
            )
            return

        try:
            converted = await commands.TextChannelConverter().convert(ctx, channel)
        except commands.BadArgument:
            await self._send_component(
                ctx,
                "채널 입력 오류",
                ["`#일반` 같은 채널 멘션 또는 `none`을 사용하세요."],
            )
            return

        await self.config.guild(ctx.guild).channel_id.set(converted.id)
        await self._send_component(
            ctx,
            "대상 채널 설정 완료",
            [f"{converted.mention}"],
        )
        await self._maybe_log(
            ctx.guild,
            f"{ctx.author.mention} 님이 AutoReact 대상 채널을 {converted.mention}(으)로 설정했습니다.",
        )

    @autoreact_group.command(name="setemoji")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_setemoji(self, ctx: commands.Context, *, emoji: str) -> None:
        """자동 반응에 사용할 이모지를 설정합니다."""
        valid, normalized = self._validate_emoji_input(emoji)
        if not valid or normalized is None:
            await self._send_component(
                ctx,
                "이모지 입력 오류",
                ["예시: `✅`, `<:name:id>`, `<a:name:id>`, `name:id`"],
            )
            return

        await self.config.guild(ctx.guild).emoji_raw.set(normalized)
        await self._send_component(
            ctx,
            "반응 이모지 설정 완료",
            [f"`{normalized}`"],
        )
        await self._maybe_log(
            ctx.guild,
            f"{ctx.author.mention} 님이 AutoReact 이모지를 `{normalized}`(으)로 변경했습니다.",
        )

    @autoreact_group.command(name="status")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_status(self, ctx: commands.Context) -> None:
        """현재 설정과 큐 상태를 확인합니다."""
        settings = await self.config.guild(ctx.guild).all()
        channel_mention = "미설정"
        if settings["channel_id"]:
            channel = ctx.guild.get_channel(settings["channel_id"])
            channel_mention = channel.mention if channel else f"삭제된 채널 ({settings['channel_id']})"

        queue_size = self._queues.get(ctx.guild.id).qsize() if ctx.guild.id in self._queues else 0
        dropped = self._dropped_count[ctx.guild.id]
        failed = self._failure_count[ctx.guild.id]

        lines = [
            f"enabled: {settings['enabled']}",
            f"channel: {channel_mention}",
            f"emoji: {settings['emoji_raw'] or '미설정'}",
            f"ignore_bots: {settings['ignore_bots']}",
            f"ignore_webhooks: {settings['ignore_webhooks']}",
            f"per_item_delay_ms: {settings['per_item_delay_ms']}",
            f"max_retry: {settings['max_retry']}",
            f"auto_disable_on_forbidden: {settings['auto_disable_on_forbidden']}",
            f"logging_enabled: {settings['logging_enabled']}",
            f"log_channel_id: {settings['log_channel_id']}",
            f"queue_length: {queue_size}",
            f"dropped_count: {dropped}",
            f"failure_count: {failed}",
        ]
        await self._send_component(ctx, "AutoReact 상태", lines)

    @autoreact_group.group(name="set", invoke_without_command=True)
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_group(self, ctx: commands.Context) -> None:
        """AutoReact 고급 옵션을 설정합니다."""
        await self._send_component(
            ctx,
            "AutoReact 고급 설정 명령",
            [
                "`[p]autoreact set ignorebots <on|off>`",
                "`[p]autoreact set ignorewebhooks <on|off>`",
                "`[p]autoreact set ratelimit <ms>`",
                "`[p]autoreact set autodisableforbidden <on|off>`",
                "`[p]autoreact set logging <on|off>`",
                "`[p]autoreact set logchannel <#채널|none>`",
            ],
        )

    @autoreact_set_group.command(name="ignorebots")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_ignorebots(self, ctx: commands.Context, value: str) -> None:
        """봇 메시지를 무시할지 설정합니다."""
        parsed = self._parse_on_off(value)
        if parsed is None:
            await self._send_component(ctx, "입력 오류", ["`on` 또는 `off`를 입력하세요."])
            return

        await self.config.guild(ctx.guild).ignore_bots.set(parsed)
        await self._send_component(ctx, "설정 변경 완료", [f"`ignore_bots` = `{parsed}`"])

    @autoreact_set_group.command(name="ignorewebhooks")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_ignorewebhooks(self, ctx: commands.Context, value: str) -> None:
        """웹훅 메시지를 무시할지 설정합니다."""
        parsed = self._parse_on_off(value)
        if parsed is None:
            await self._send_component(ctx, "입력 오류", ["`on` 또는 `off`를 입력하세요."])
            return

        await self.config.guild(ctx.guild).ignore_webhooks.set(parsed)
        await self._send_component(ctx, "설정 변경 완료", [f"`ignore_webhooks` = `{parsed}`"])

    @autoreact_set_group.command(name="ratelimit")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_ratelimit(self, ctx: commands.Context, ms: int) -> None:
        """메시지당 처리 지연(ms)을 설정합니다."""
        if not self.RATELIMIT_TUNING_ENABLED:
            await self._send_component(
                ctx,
                "변경 불가",
                ["레이트리밋 튜닝은 기본적으로 잠겨 있습니다(OFF)."],
            )
            return

        if ms < 100 or ms > 2000:
            await self._send_component(ctx, "입력 오류", ["레이트리밋 값은 100~2000ms 사이여야 합니다."])
            return

        await self.config.guild(ctx.guild).per_item_delay_ms.set(ms)
        await self._send_component(ctx, "설정 변경 완료", [f"`per_item_delay_ms` = `{ms}`"])

    @autoreact_set_group.command(name="autodisableforbidden")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_autodisable_forbidden(self, ctx: commands.Context, value: str) -> None:
        """권한 오류 시 자동 비활성화 여부를 설정합니다."""
        parsed = self._parse_on_off(value)
        if parsed is None:
            await self._send_component(ctx, "입력 오류", ["`on` 또는 `off`를 입력하세요."])
            return

        await self.config.guild(ctx.guild).auto_disable_on_forbidden.set(parsed)
        await self._send_component(
            ctx,
            "설정 변경 완료",
            [f"`auto_disable_on_forbidden` = `{parsed}`"],
        )

    @autoreact_set_group.command(name="logging")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_logging(self, ctx: commands.Context, value: str) -> None:
        """이 Cog의 로그 기능을 활성화/비활성화합니다."""
        parsed = self._parse_on_off(value)
        if parsed is None:
            await self._send_component(ctx, "입력 오류", ["`on` 또는 `off`를 입력하세요."])
            return

        await self.config.guild(ctx.guild).logging_enabled.set(parsed)
        await self._send_component(ctx, "설정 변경 완료", [f"`logging_enabled` = `{parsed}`"])

    @autoreact_set_group.command(name="logchannel")
    @commands.admin_or_permissions(manage_guild=True)
    async def autoreact_set_logchannel(self, ctx: commands.Context, *, channel: Optional[str] = None) -> None:
        """로그 채널을 설정합니다. 채널 멘션 또는 `none`을 사용하세요."""
        if channel is None or channel.strip().lower() == "none":
            await self.config.guild(ctx.guild).log_channel_id.set(None)
            await self._send_component(ctx, "설정 변경 완료", ["`log_channel_id`를 해제했습니다."])
            return

        try:
            converted = await commands.TextChannelConverter().convert(ctx, channel)
        except commands.BadArgument:
            await self._send_component(
                ctx,
                "채널 입력 오류",
                ["채널 멘션 또는 `none`을 사용하세요."],
            )
            return

        await self.config.guild(ctx.guild).log_channel_id.set(converted.id)
        await self._send_component(
            ctx,
            "설정 변경 완료",
            [f"`log_channel_id` = {converted.mention}"],
        )
