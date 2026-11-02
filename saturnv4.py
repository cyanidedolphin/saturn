import os
import re
import json
import math
import time
import asyncio
from urllib.parse import quote
from datetime import datetime
from typing import Literal
from loyalty import run, start_listener

import aiohttp
import discord
from discord import app_commands

# ---------------------------------------------------------------- config
# Set these as environment variables instead of hardcoding:
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "")
API_KEYS = [k for k in (os.getenv("WYNN_KEY_1"), os.getenv("WYNN_KEY_2")) if k]

PLAYER_URL = "https://api.wynncraft.com/v3/player/{username}"
ONLINE_PLAYERS_URL = "https://api.wynncraft.com/v3/player?identifier=username"
GUILD_URL = "https://api.wynncraft.com/v3/guild/{guild}"
CHARACTERS_SUFFIX = "/characters"
MAX_RETRIES = 3

PER_KEY_SPACING = 0.5
GUEST_SPACING = 1.2
USERNAME_RE = re.compile(r"^\w{1,16}$")

LINKED_ACCOUNTS_FILE = "linked_accounts.json"
BLACKLIST_FILE = "blacklist.json"
PREVIOUS_STATS_FILE = "previous_stats.json"
LEVEL_CHECK_CACHE_FILE = "level_check_cache.json"
GUILDED_CACHE_FILE = "guilded_cache.json"
RECRUITABLE_CACHE_FILE = "recruitable_cache.json"

class MyBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        global rate_lock, heavy_lock
        rate_lock = asyncio.Lock()
        heavy_lock = asyncio.Lock()
#        await self.tree.sync() 


bot = MyBot()
rate_lock: asyncio.Lock = None
heavy_lock: asyncio.Lock = None  # serialises /check and /refresh
api_key_index = 0
active_keys = list(API_KEYS)  
key_state = {}                # key -> {'remaining': int|None, 'reset_at': float}
last_check_results = []  # raw (unescaped) names


# ---------------------------------------------------------------- persistence
def load_json(path, default):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return default


def save_json(path, data):
    """Atomic write: temp file then replace."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"Error saving {path}: {e}")


def load_name_set(path, key):
    data = load_json(path, {})
    return set(str(n).lower() for n in data.get(key, []))


linked_accounts = load_json(LINKED_ACCOUNTS_FILE, {})
previous_stats = load_json(PREVIOUS_STATS_FILE, {})
blacklist = load_name_set(BLACKLIST_FILE, "blacklist")
level_check_cache = load_name_set(LEVEL_CHECK_CACHE_FILE, "passed")
guilded_cache = load_name_set(GUILDED_CACHE_FILE, "guilded")
recruitable_cache = load_name_set(RECRUITABLE_CACHE_FILE, "recruitable")


def save_blacklist():
    save_json(BLACKLIST_FILE, {"blacklist": sorted(blacklist)})


def save_level_cache():
    save_json(LEVEL_CHECK_CACHE_FILE, {"passed": sorted(level_check_cache)})


def save_guilded_cache():
    save_json(GUILDED_CACHE_FILE, {"guilded": sorted(guilded_cache)})


def save_recruitable_cache():
    save_json(RECRUITABLE_CACHE_FILE, {"recruitable": sorted(recruitable_cache)})


# ---------------------------------------------------------------- helpers
def esc(name):
    return name.replace("_", "\\_")


def valid_username(name):
    return bool(USERNAME_RE.match(name or ""))


def chunk_lines(text, limit=1900):
    chunks, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)
    return chunks or [""]


async def notify(interaction: discord.Interaction, text: str):
    """Send via channel (works after the 15-min interaction token expires),
    fall back to followup."""
    for chunk in chunk_lines(text):
        try:
            if interaction.channel is None:
                raise RuntimeError("no channel")
            await interaction.channel.send(chunk)
        except Exception:
            try:
                await interaction.followup.send(chunk)
            except Exception as e:
                print(f"Failed to send message: {e}")


def new_session():
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))


# ---------------------------------------------------------------- API
def current_delay():
    n = len(active_keys)
    return PER_KEY_SPACING / n if n else GUEST_SPACING


def _num(headers, name):
    try:
        return float(headers.get(name))
    except (TypeError, ValueError):
        return None


def pick_key():
    global api_key_index
    if not active_keys:
        return None
    key = active_keys[api_key_index % len(active_keys)]
    api_key_index += 1
    return key


async def fetch_json(session, url):
    """Rate-limited GET. Returns parsed JSON for 200 and 300 (MultipleObjectsReturned);
    None on any other outcome."""
    for attempt in range(MAX_RETRIES):
        async with rate_lock:
            await asyncio.sleep(current_delay())
            key = pick_key()
            if key:
                st = key_state.setdefault(key, {"remaining": None, "reset_at": 0.0})
                if st["remaining"] is not None and st["remaining"] <= 1:
                    wait = st["reset_at"] - time.time()
                    if wait > 0:
                        await asyncio.sleep(min(wait, 65))
                    st["remaining"] = None

        headers = {"Authorization": f"Bearer {key}"} if key else {}

        try:
            async with session.get(url, headers=headers) as response:

                if key:
                    remaining = _num(response.headers, "RateLimit-Remaining")
                    reset = _num(response.headers, "RateLimit-Reset")
                    st = key_state.setdefault(key, {"remaining": None, "reset_at": 0.0})
                    st["remaining"] = remaining
                    st["reset_at"] = time.time() + (reset or 0)

                if response.status in (200, 300):
                    return await response.json(content_type=None)

                if response.status == 429:
                    wait = (_num(response.headers, "RateLimit-Reset")
                            or _num(response.headers, "Retry-After") or 5)
                    print(f"Rate limited (429). Waiting {wait}s...")
                    await asyncio.sleep(min(wait, 65))
                    continue

                if response.status in (400, 403):
                    try:
                        body = await response.json(content_type=None)
                    except Exception:
                        body = {}
                    err = body.get("error") if isinstance(body, dict) else None
                    if err in ("InvalidTokenError", "MalformedTokenError") and key:
                        print(f"API rejected a token ({err}); removing it from rotation.")
                        if key in active_keys:
                            active_keys.remove(key)
                        continue
                    print(f"{response.status} {err} for {url}")
                    return None

                print(f"Error {response.status} for {url}")
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Request failed: {e}")
    return None

async def get_guild_members(session, guild="Cyanide Legion"):
    """fetching data"""
    data = await fetch_json(session, GUILD_URL.format(guild=quote(guild, safe="")))
    if not isinstance(data, dict):
        return []

    members = data.get("members") or {}
    out = []
    for rank_bucket in members.values():
        if not isinstance(rank_bucket, dict):
            continue
        for name, info in rank_bucket.items():
            if not isinstance(info, dict):
                continue
            gd = info.get("globalData") or {}
            out.append({
                "username": info.get("legacyName") or name,
                "wars": gd.get("wars"),
                "guildRaids": {
                    "total": (gd.get("currentGuildRaids") or {}).get("total")
                },
            })
    return out

async def fetch_player_json(session, username, suffix=""):
    """GET /v3/player/{username}{suffix}. Resolves 300 MultipleObjectsReturned by
    retrying with the UUID of the exact (case-insensitive) username match."""
    data = await fetch_json(session, PLAYER_URL.format(username=quote(username, safe="")) + suffix)

    if isinstance(data, dict) and data.get("error") == "MultipleObjectsReturned":
        objects = data.get("objects") or {}
        matches = [uuid for uuid, obj in objects.items()
                   if isinstance(obj, dict) and str(obj.get("username", "")).lower() == username.lower()]
        if len(matches) != 1:
            print(f"Ambiguous username {username}: {len(objects)} candidates, skipping.")
            return None
        data = await fetch_json(session, PLAYER_URL.format(username=matches[0]) + suffix)

    if not isinstance(data, dict) or data.get("error"):
        return None
    return data


def server_prefix(value):
    """'WC12' -> 'WC', 'NA3' -> 'NA'."""
    m = re.match(r"[A-Za-z]+", value) if isinstance(value, str) else None
    return m.group(0).upper() if m else ""


async def get_online_players(session):
    """Always returns (names, server_prefixes); both empty on failure.
    Note: the list can hide players (access rules) and is cached ~30s by the API."""
    print("Fetching online players...")
    data = await fetch_json(session, ONLINE_PLAYERS_URL)
    if not isinstance(data, dict) or not isinstance(data.get("players"), dict):
        print("Failed to get player data")
        return [], []

    players = list(data["players"].keys())
    servers = [server_prefix(v) for v in data["players"].values()]
    print(f"Found {len(players)} listed players (API total: {data.get('total')})")
    return players, servers


def level_expectations(characters):
    items = characters.values() if isinstance(characters, dict) else characters
    for char in items:
        if isinstance(char, dict):
            level = char.get("level", 0)
            if isinstance(level, (int, float)) and level >= 105:
                return True
    return False


async def has_high_level_character(session, username):
    if username.lower() in level_check_cache:
        return True

    # Response is a flat map keyed by character UUID, each with "level".
    # 403 = characterListAccess restricted -> can't tell, treated as not qualifying.
    data = await fetch_player_json(session, username, CHARACTERS_SUFFIX)
    if data and level_expectations(data):
        level_check_cache.add(username.lower())
        return True
    return False

def raid_expectations(profile):
    global_data = profile.get("globalData") or {}
    raids = global_data.get("raids") or {}
    raid_count = raids.get("total", 0)
    return isinstance(raid_count, (int, float)) and raid_count > 0

async def has_raid(session, username):
    data = await fetch_player_json(session, username)
    if data and raid_expectations(data):
        return True
    return False

async def is_guildless(session, username):
    """True = guildless, False = in a guild, None = unknown (API failure)."""
    data = await fetch_player_json(session, username)
    if data is None:
        return None
    return not data.get("guild")   # "guild" is an object or null


async def get_player_stats(session, username):
    data = await fetch_player_json(session, username)
    if data is None:
        return None

    if (data.get("restrictions") or {}).get("mainAccess"):
        return {
            "username": username,
            "wars": "Restricted",
            "currentGuildRaids": {"total": "Restricted"},
        }

    global_data = data.get("globalData") or {}
    guild_raids = global_data.get("currentGuildRaids") or {}

    return {
        "username": username,
        "wars": global_data.get("wars", 0),
        "currentGuildRaids": {
            "total": guild_raids.get("total", 0)
        },
    }

def format_since_last(all_stats):
    prev = previous_stats.get("players", {})

    raid_deltas = []
    war_deltas = []
    skipped = []

    for s in all_stats:
        name = s.get("username", "Unknown")
        name_key = name.lower()

        r_cur = s.get("guildRaids", {}).get("total")
        w_cur = s.get("wars")

        prev_entry = prev.get(name_key, {})

        if type(r_cur) is int:
            r_prev = prev_entry.get("raids", 0)
            raid_deltas.append((name, r_cur - r_prev)) if type(r_prev) is int else skipped.append(name)
        else:
            skipped.append(name)

        if type(w_cur) is int:
            w_prev = prev_entry.get("wars", 0)
            war_deltas.append((name, w_cur - w_prev)) if type(w_prev) is int else skipped.append(name)
        else:
            skipped.append(name)

    raid_deltas.sort(key=lambda item: item[1], reverse=True)
    war_deltas.sort(key=lambda item: item[1], reverse=True)

    total_r = sum(delta for _, delta in raid_deltas)
    total_w = sum(delta for _, delta in war_deltas)

    raid_body = "\n".join(
        f"- {esc(name)}: {delta}"
        for name, delta in raid_deltas
    ) or "- None"

    war_body = "\n".join(
        f"- {esc(name)}: {delta}"
        for name, delta in war_deltas
    ) or "- None"

    body = (
        f"**Raids this week:** {total_r}\n\n"
        f"{raid_body}\n\n"
        f"**Wars this week:** {total_w}\n\n"
        f"{war_body}"
    )

    if skipped:
        body += (
            "\n\n*Skipped: "
            + ", ".join(esc(name) for name in sorted(set(skipped), key=str.lower))
            + "*"
        )

    return body


def update_previous_stats(all_stats, can_save=False):
    global previous_stats
    players = previous_stats.get("players", {}).copy()
    for s in all_stats:
        if isinstance(s["guildRaids"]["total"], int):
            players[s["username"].lower()] = {
                "raids": s["guildRaids"]["total"],
                "wars": s["wars"],
            }

    previous_stats = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "players": players,
    }

    if can_save:
        save_json(PREVIOUS_STATS_FILE, previous_stats)

class CopyButton(discord.ui.View):
    def __init__(self, names):
        super().__init__(timeout=None)
        self.names = names

    @discord.ui.button(
        label="Copy to Clipboard",
        style=discord.ButtonStyle.primary
    )
    async def copy(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        run(self.names)

        await interaction.response.send_message(
            "copied to clipboard",
            ephemeral=True
        )
# ---------------------------------------------------------------- commands
@bot.tree.command(name="stats", description="weekly stats")
@app_commands.describe(save="to save stats for next time (requires manage server perms)")
async def stats_cmd(interaction: discord.Interaction, save: bool = False):
    await interaction.response.defer()

    async with new_session() as session:
        all_stats = await get_guild_members(session)
        if not all_stats:
            await interaction.followup.send(
                "Failed to fetch guild members. Check the guild name and API key."
            )
            return

        await interaction.followup.send(f"Checking {len(all_stats)} guild members...")

    await notify(interaction, format_since_last(all_stats))

    if save:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.followup.send("this is an admin command")
            return
        update_previous_stats(all_stats, can_save=True)
        await interaction.followup.send("successfully saved to previous_stats.json")

async def run_check(interaction, regions, amount, extended):
    global last_check_results
    start_time = time.time()
    amount = max(1, min(amount, 10000))
    region_list = [x.strip().upper() for x in regions.split(",") if x.strip()]

    async with new_session() as session:
        names, servers = await get_online_players(session)
        if not names:
            await interaction.followup.send(
                "Failed to get online players. API might be rate limited. Try again in 3 minutes."
            )
            return

        filtered = [
            p for p, srv in zip(names, servers)
            if p.lower() not in blacklist and srv in region_list
        ]
        if not filtered:
            seen = ", ".join(sorted(set(s for s in servers if s))) or "none"
            await interaction.followup.send(
                f"No players left after region/blacklist filtering. "
                f"Regions requested: {', '.join(region_list)}. Server prefixes seen in API: {seen}."
            )
            return
        actual_check = filtered[:amount]
        blacklisted_count = len(names) - len(filtered)

        # skip known-guilded players BEFORE spending requests on them
        skipped_guilded = 0
        to_check = actual_check
        if not extended:
            to_check = [p for p in actual_check if p.lower() not in guilded_cache]
            skipped_guilded = len(actual_check) - len(to_check)

        est_minutes = max(1, math.ceil(len(to_check) * 2 * current_delay() / 120))
        parts = []
        if blacklisted_count:
            parts.append(f"skipped {blacklisted_count} blacklisted")
        if skipped_guilded:
            parts.append(f"skipping {skipped_guilded} cached guilded (non-extended)")
        status = f" ({', '.join(parts)})" if parts else ""
        await interaction.followup.send(
            f"Checking {len(to_check)} players...{status} Estimated time: up to {est_minutes} minute(s)."
        )

        # level check
        meets_expectations = []
        for i, username in enumerate(to_check):
            print(f"Checking {username} ({i + 1}/{len(to_check)})...")
            if (await has_high_level_character(session, username)
                    and await has_raid(session, username)):
                meets_expectations.append(username)
        print(f"Completed level checks. Found {len(meets_expectations)} players")
        save_level_cache()

        def elapsed_str():
            e = time.time() - start_time
            return f"{int(e // 60)}m {int(e % 60)}s"

        # guild check
        guildless = []
        for i, username in enumerate(meets_expectations):
            print(f"Checking for {username} ({i + 1}/{len(meets_expectations)})...")
            result = await is_guildless(session, username)
            if result is None:
                print(f"  ? unknown {username}")
                continue  # API failure: don't touch caches
            if result:
                guildless.append(username)
                guilded_cache.discard(username.lower())
                print(f"  ✓ yay {username}")
            else:
                guilded_cache.add(username.lower())
                recruitable_cache.discard(username.lower())
                print(f"  ✗ nay {username}")
        save_guilded_cache()

        if not guildless:
            save_recruitable_cache()
            await notify(interaction,
                         f"No recruitable players found. Searched {len(actual_check)} players in {elapsed_str()}.")
            return

        print("\nFetching online players again to cross-reference...")
        final_names, _ = await get_online_players(session)

    if not final_names:
        await notify(interaction,
                     "**Recruitable players (couldn't verify current online status):**\n"
                     + "\n".join(esc(p) for p in guildless))
        return

    final_set = set(final_names)
    still_online = [p for p in guildless if p in final_set]
    if not still_online:
        await notify(interaction, "All recruitable players have gone offline.")
        return

    went_offline = len(guildless) - len(still_online)
    last_check_results = list(still_online)  # raw names
    for p in still_online:
        recruitable_cache.add(p.lower())
    save_recruitable_cache()

    message = (f"Searched **{len(actual_check)}** players in **{elapsed_str()}**.\n\n"
               f"**Recruitable players still online:**\n" + "\n".join(esc(p) for p in still_online))
    if went_offline:
        message += f"\n\n*{went_offline} player(s) went offline.*"
    await interaction.followup.send(
        message,
        view=CopyButton(still_online)
    )


@bot.tree.command(name="check", description="recruitment check")
@app_commands.describe(regions="included regions")
@app_commands.describe(extended="default: false")
@app_commands.describe(amount="number of players to check (only if in rush generally just don't touch)")
async def check(interaction: discord.Interaction, regions: str = "na, eu, as",
                amount: int = 10000, extended: bool = False):
    await interaction.response.defer()
    if heavy_lock.locked():
        await interaction.followup.send("Another /check or /refresh is already running.")
        return
    async with heavy_lock:
        try:
            await run_check(interaction, regions, amount, extended)
        except Exception as e:
            print(f"/check failed: {e}")
            await notify(interaction, f"/check failed: `{type(e).__name__}`")


@bot.tree.command(name="instacheck", description="lazy...")
async def instacheck(interaction: discord.Interaction):
    await interaction.response.defer()

    if not recruitable_cache:
        await interaction.followup.send("No cached recruitable players. Run /check first.")
        return

    async with new_session() as session:
        names, _ = await get_online_players(session)

    if not names:
        await interaction.followup.send(
            "Failed to get online players. API might be rate limited. Try again in 3 minutes."
        )
        return

    online_lower = {p.lower(): p for p in names}
    matches = []
    skipped_blacklisted = 0
    for cached in recruitable_cache:
        if cached not in online_lower:
            continue
        if cached in blacklist:
            skipped_blacklisted += 1
            continue
        matches.append(online_lower[cached])

    if not matches:
        msg = f"No cached recruitable players are currently online. ({len(recruitable_cache)} cached.)"
        if skipped_blacklisted:
            msg += f"\n*({skipped_blacklisted} were online but blacklisted.)*"
        await interaction.followup.send(msg)
        return

    message = f"**Instacheck Results:**\nFound **{len(matches)}** online from {len(recruitable_cache)} cached.\n\n"
    message += "**Online:**\n" + "\n".join(esc(p) for p in sorted(matches))
    if skipped_blacklisted:
        message += f"\n\n*({skipped_blacklisted} were online but blacklisted.)*"

    chunks = chunk_lines(message)
    await interaction.followup.send(chunks[0])
    for c in chunks[1:]:
        await interaction.followup.send(c)


@bot.tree.command(name="blacklist", description="Manage the blacklist")
@app_commands.describe(action="Action to perform")
@app_commands.describe(username="Username to add/remove (required for add/remove)")
@app_commands.default_permissions(manage_guild=True)
@app_commands.guild_only()
async def blacklist_cmd(interaction: discord.Interaction,
                        action: Literal["add", "remove", "list"], username: str = None):
    await interaction.response.defer()

    if action == "list":
        if not blacklist:
            await interaction.followup.send("Blacklist is empty.")
            return
        message = f"**Blacklisted players ({len(blacklist)}):**\n" + "\n".join(esc(n) for n in sorted(blacklist))
        for c in chunk_lines(message):
            await interaction.followup.send(c)
        return

    if not username or not valid_username(username):
        await interaction.followup.send("Please provide a valid username.")
        return

    key = username.lower()
    if action == "add":
        if key in blacklist:
            await interaction.followup.send(f"**{esc(username)}** is already blacklisted.")
        else:
            blacklist.add(key)
            save_blacklist()
            await interaction.followup.send(f"✅ Added **{esc(username)}** to blacklist.")
    else:  # remove
        if key in blacklist:
            blacklist.discard(key)
            save_blacklist()
            await interaction.followup.send(f"✅ Removed **{esc(username)}** from blacklist.")
        else:
            await interaction.followup.send(f"**{esc(username)}** is not in the blacklist.")


@bot.tree.command(name="ping", description="Test if bot is working")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong!")


@bot.tree.command(name="link", description="Link your Discord account to a Wynncraft username")
@app_commands.describe(username="Your Minecraft username")
async def link(interaction: discord.Interaction, username: str):
    await interaction.response.defer()

    if not valid_username(username):
        await interaction.followup.send("Invalid username.")
        return

    discord_id = str(interaction.user.id)
    old = linked_accounts.get(discord_id, {}).get("username")
    linked_accounts[discord_id] = {
        "username": username,
        "discord_name": str(interaction.user),
        "linked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_json(LINKED_ACCOUNTS_FILE, linked_accounts)

    if old:
        await interaction.followup.send(f"Updated your link from **{esc(old)}** to **{esc(username)}**")
    else:
        await interaction.followup.send(f"Linked your Discord account to Minecraft username: **{esc(username)}**")


@bot.tree.command(name="followup", description="Check if players from last search are still online")
async def followup(interaction: discord.Interaction):
    await interaction.response.defer()

    if not last_check_results:
        await interaction.followup.send("No previous check results found. Run /check first.")
        return

    async with new_session() as session:
        names, _ = await get_online_players(session)

    if not names:
        await interaction.followup.send(
            "Failed to get current online players. API might be rate limited. Try again in 3 minutes."
        )
        return

    online = set(names)
    still_online = [p for p in last_check_results if p in online]
    went_offline = len(last_check_results) - len(still_online)

    message = (f"**Follow-up check:**\n"
               f"Previously found: **{len(last_check_results)}** players\n"
               f"Still online: **{len(still_online)}** players\n"
               f"Went offline: **{went_offline}** players\n\n")
    if still_online:
        message += "**Still online:**\n" + "\n".join(esc(p) for p in still_online)

    chunks = chunk_lines(message)
    await interaction.followup.send(chunks[0])
    for c in chunks[1:]:
        await interaction.followup.send(c)


@bot.tree.command(name="refresh", description="quick refresh")
@app_commands.default_permissions(manage_guild=True)
@app_commands.guild_only()  
async def refresh(interaction: discord.Interaction):
    await interaction.response.defer()

    if not guilded_cache and not recruitable_cache:
        await interaction.followup.send("all good nothing changed")
        return
    if heavy_lock.locked():
        await interaction.followup.send("Another /check or /refresh is already running.")
        return

    async with heavy_lock:
        start_time = time.time()
        await interaction.followup.send(
            f"Refreshing caches... checking **{len(guilded_cache)}** guilded + "
            f"**{len(recruitable_cache)}** recruitable players."
        )

        guilded_removed = 0
        recruitable_removed = 0

        async with new_session() as session:
            for username in list(guilded_cache):
                if await is_guildless(session, username) is True:
                    guilded_cache.discard(username)
                    guilded_removed += 1
            save_guilded_cache()

            for username in list(recruitable_cache):
                if await is_guildless(session, username) is False:
                    recruitable_cache.discard(username)
                    guilded_cache.add(username)
                    recruitable_removed += 1

        save_recruitable_cache()
        save_guilded_cache()

        elapsed = time.time() - start_time
        message = (f"**Refresh Complete** ({int(elapsed // 60)}m {int(elapsed % 60)}s)\n\n"
                   f"**Guild cache:** removed {guilded_removed}, {len(guilded_cache)} remaining\n"
                   f"**Recruitable cache:** removed {recruitable_removed}, {len(recruitable_cache)} remaining\n")
        await notify(interaction, message)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

start_listener()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_TOKEN (and WYNN_KEY_1 / WYNN_KEY_2) as environment variables.")
    bot.run(TOKEN)
