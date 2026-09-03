"""
delta_monitor.py

Standalone daemon that continuously monitors Greek delta values for all
short option legs across IBKR paper and real accounts.

When |delta| exceeds a configurable threshold (default 0.65), an alert
is sent via Telegram (if .tg_bot_secret.json is present) and logged.

Uses a separate client_id to avoid conflicting with the main dashboard.
"""

import asyncio
import json
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ib_async import IB
import ib_async.wrapper

# ---------------------------------------------------------------------------
# Monkey-patch (same as ibkr_portfolio.py) to avoid KeyError on stale reqIds
# ---------------------------------------------------------------------------
_orig_contractDetails = ib_async.wrapper.Wrapper.contractDetails


def _patched_contractDetails(self, reqId: int, contractDetails):
    if reqId not in self._results:
        return
    _orig_contractDetails(self, reqId, contractDetails)


ib_async.wrapper.Wrapper.contractDetails = _patched_contractDetails

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
LOGGER = logging.getLogger("delta_monitor")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
_TG_SECRET_PATH = Path(__file__).resolve().parent / ".tg_bot_secret.json"

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        LOGGER.error(f"Failed to load config.json: {e}")
        return {}


def _load_monitor_settings(config: dict) -> dict:
    """Return delta_monitor settings with defaults."""
    settings = config.get("delta_monitor", {})
    return {
        "threshold": settings.get("threshold", 0.65),
        "poll_interval_sec": settings.get("poll_interval_sec", 30),
        "cooldown_min": settings.get("cooldown_min", 30),
        "client_id": settings.get("client_id", 10),
    }


# ---------------------------------------------------------------------------
# Telegram notifier (stdlib-only, no requests dependency)
# ---------------------------------------------------------------------------
class TelegramNotifier:
    """Sends plain-text messages to a Telegram chat via the Bot API.

    Uses urllib (stdlib) — no external dependencies required.
    If the secret file is missing or malformed, the notifier silently
    disables itself.
    """

    def __init__(self, secrets_path: Path | None = None):
        self._token: str | None = None
        self._chat_id: str | None = None
        self._enabled: bool = False

        path = secrets_path or _TG_SECRET_PATH
        try:
            with open(path, "r") as f:
                data = json.load(f)
            self._token = data["telegram_bot_token"]
            self._chat_id = str(data["telegram_chat_id"])
            self._enabled = True
            LOGGER.info("[TG] Telegram notifier enabled.")
        except FileNotFoundError:
            LOGGER.info(
                f"[TG] Secret file not found at {path}. Notifications disabled."
            )
        except (json.JSONDecodeError, KeyError) as e:
            LOGGER.error(
                f"[TG] Failed to parse secret file: {e}. Notifications disabled."
            )

    @property
    def enabled(self) -> bool:
        return self._enabled

    def send_message(self, text: str) -> bool:
        """Send an HTML message to the configured Telegram chat."""
        if not self._enabled:
            return False

        url = _TELEGRAM_API.format(token=self._token)
        payload = json.dumps(
            {
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read())
                if body.get("ok"):
                    LOGGER.info("[TG] Message sent successfully.")
                    return True
                else:
                    LOGGER.warning(f"[TG] Telegram API response not ok: {body}")
                    return False
        except (urllib.error.URLError, Exception) as e:
            LOGGER.error(f"[TG] Failed to send message: {e}")
            return False


# ---------------------------------------------------------------------------
# Delta Monitor per IBKR environment
# ---------------------------------------------------------------------------
class DeltaMonitorEnv:
    """Monitors short option deltas for a single IBKR environment (paper/real)."""

    def __init__(
        self,
        env_name: str,
        host: str,
        port: int,
        client_id: int,
        account: Optional[str],
        threshold: float,
        cooldown_min: float,
        poll_interval_sec: float,
        notifier: TelegramNotifier,
    ):
        self.env_name = env_name
        self.label = env_name.upper()  # "PAPER" or "REAL"
        self.host = host
        self.port = port
        self.client_id = client_id
        self.account = account
        self.threshold = threshold
        self.cooldown_sec = cooldown_min * 60
        self.poll_interval = poll_interval_sec
        self.notifier = notifier

        self.ib = IB()
        # conId -> last alert timestamp
        self._alert_cooldowns: Dict[int, float] = {}
        # conId -> Ticker
        self._tickers: Dict[int, Any] = {}
        # conId -> Contract
        self._contracts: Dict[int, Any] = {}

    async def run(self):
        """Main loop: connect → subscribe → poll deltas forever."""
        while True:
            try:
                if not self.ib.isConnected():
                    LOGGER.info(
                        f"[{self.label}] Connecting to {self.host}:{self.port} "
                        f"clientId={self.client_id}"
                    )
                    await self.ib.connectAsync(
                        self.host,
                        self.port,
                        clientId=self.client_id,
                        account=self.account or "",
                    )
                    self.ib.reqMarketDataType(3)
                    LOGGER.info(f"[{self.label}] Connected and synchronized.")
                    self._tickers.clear()
                    self._contracts.clear()

                await self._check_deltas()

            except Exception as e:
                LOGGER.error(f"[{self.label}] Error: {e}", exc_info=True)

            await asyncio.sleep(self.poll_interval)

    async def _check_deltas(self):
        """Scan portfolio for short options, subscribe to greeks, check deltas."""
        items = self.ib.portfolio()

        short_options = []
        for item in items:
            if self.account and item.account != self.account:
                continue
            contract = item.contract
            if contract.secType not in ("OPT", "FOP"):
                continue
            if float(item.position) >= 0:
                continue  # only short legs
            short_options.append(item)

        if not short_options:
            return

        active_con_ids = set()
        # Subscribe to market data for any new short options
        for item in short_options:
            contract = item.contract
            con_id = contract.conId
            active_con_ids.add(con_id)

            if con_id not in self._tickers:
                if not contract.exchange:
                    contract.exchange = contract.primaryExchange or "SMART"
                try:
                    ticker = self.ib.reqMktData(contract, "106", False, False)
                    self._tickers[con_id] = ticker
                    self._contracts[con_id] = contract
                    LOGGER.info(
                        f"[{self.label}] Subscribed to greeks for "
                        f"{contract.localSymbol} (conId={con_id})"
                    )
                except Exception as e:
                    LOGGER.warning(
                        f"[{self.label}] Failed to subscribe mkt data for "
                        f"{contract.localSymbol}: {e}"
                    )

        # Clean up closed or expired short option subscriptions
        closed_con_ids = set(self._tickers.keys()) - active_con_ids
        for con_id in closed_con_ids:
            old_contract = self._contracts.pop(con_id, None)
            if old_contract:
                try:
                    self.ib.cancelMktData(old_contract)
                except Exception:
                    pass
            self._tickers.pop(con_id, None)
            self._alert_cooldowns.pop(con_id, None)

        # Check deltas
        now = time.time()
        for item in short_options:
            contract = item.contract
            con_id = contract.conId
            ticker = self._tickers.get(con_id) or self.ib.ticker(contract)
            if not ticker:
                continue

            delta = None
            greeks = (
                getattr(ticker, "modelGreeks", None)
                or getattr(ticker, "lastGreeks", None)
                or getattr(ticker, "bidGreeks", None)
                or getattr(ticker, "askGreeks", None)
            )
            if greeks and greeks.delta is not None:
                delta = greeks.delta

            if delta is None:
                continue

            abs_delta = abs(delta)
            position = float(item.position)
            symbol = contract.localSymbol or contract.symbol

            LOGGER.debug(f"[{self.label}] {symbol}: delta={delta:.4f}, pos={position}")

            if abs_delta >= self.threshold:
                # Check cooldown
                last_alert = self._alert_cooldowns.get(con_id, 0)
                if now - last_alert < self.cooldown_sec:
                    continue

                self._alert_cooldowns[con_id] = now
                self._send_alert(contract, delta, position)

    def _send_alert(self, contract, delta: float, position: float):
        """Log and send Telegram alert for a breached delta threshold."""
        symbol = contract.localSymbol or contract.symbol
        strike = getattr(contract, "strike", "?")
        right = getattr(contract, "right", "?")
        expiry = getattr(contract, "lastTradeDateOrContractMonth", "?")

        msg_lines = [
            f"⚠️ <b>Delta Alert [{self.label}]</b>",
            f"",
            f"<b>{contract.symbol}</b> {strike}{right} exp {expiry}",
            f"Delta: <b>{delta:.4f}</b>  (threshold: {self.threshold})",
            f"Position: {position:+.0f}",
            f"Local: {symbol}",
            f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ]
        msg = "\n".join(msg_lines)

        LOGGER.warning(
            f"[{self.label}] DELTA ALERT: {symbol} delta={delta:.4f} "
            f"pos={position} (threshold={self.threshold})"
        )
        self.notifier.send_message(msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    config = _load_config()
    settings = _load_monitor_settings(config)
    notifier = TelegramNotifier()

    LOGGER.info(
        f"Delta Monitor starting — threshold={settings['threshold']}, "
        f"poll={settings['poll_interval_sec']}s, "
        f"cooldown={settings['cooldown_min']}min"
    )

    ibkr_settings = config.get("ibkr", {})
    monitors = []

    for env_name in ["paper", "real"]:
        if env_name not in ibkr_settings:
            continue
        env_config = ibkr_settings[env_name]
        monitor = DeltaMonitorEnv(
            env_name=env_name,
            host=env_config.get("host", "127.0.0.1"),
            port=env_config.get("port", 4002 if env_name == "paper" else 4001),
            client_id=settings["client_id"] + (0 if env_name == "real" else 1),
            account=env_config.get("account"),
            threshold=settings["threshold"],
            cooldown_min=settings["cooldown_min"],
            poll_interval_sec=settings["poll_interval_sec"],
            notifier=notifier,
        )
        monitors.append(monitor)

    if not monitors:
        LOGGER.error("No IBKR environments configured. Exiting.")
        return

    tasks = [asyncio.create_task(m.run()) for m in monitors]
    LOGGER.info(
        f"Monitoring {len(monitors)} environment(s): "
        f"{[m.env_name for m in monitors]}"
    )

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        LOGGER.info("Shutting down...")
    finally:
        for m in monitors:
            if m.ib.isConnected():
                m.ib.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
