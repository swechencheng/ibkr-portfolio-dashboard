"""
ibkr_portfolio.py

Extracts portfolio-level data from Interactive Brokers via ib_async:
- Account summary (NLV, cash, margins, buying power, cushion)
- Portfolio positions with market values and P&L
- Recent executions/fills
- P&L summary (daily/total)

This module wraps an existing IB connection instance and provides
JSON-serializable dicts suitable for the portfolio frontend.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Set

from ib_async import IB, Contract, Stock
import ib_async.wrapper

LOGGER = logging.getLogger("ibkr_portfolio")

# Monkey patch ib_async to ignore delayed contract details when reqId is already cleared (due to timeouts or disconnects)
_orig_contractDetails = ib_async.wrapper.Wrapper.contractDetails


def _patched_contractDetails(self, reqId: int, contractDetails):
    if reqId not in self._results:
        return
    _orig_contractDetails(self, reqId, contractDetails)


ib_async.wrapper.Wrapper.contractDetails = _patched_contractDetails


class IbkrPortfolio:
    """
    Read-only portfolio data provider for an existing ib_async IB connection.

    Does NOT own or manage the IB connection — the caller (backend.py) is
    responsible for connecting/disconnecting.
    """

    def __init__(self, ib: IB, account: Optional[str] = None):
        self.ib = ib
        self.account = account
        self._pnl_subscribed = False
        self._pnl_single_subscribed: Dict[int, bool] = {}
        self._combo_symbol_cache: Dict[tuple, str] = {}
        self._prior_close_cache: Dict[int, float] = {}
        self._prior_close_fetching: Set[int] = set()
        self._prior_close_cache_date: Optional[str] = None

        # Subscribe to account and portfolio updates so that
        # ib.accountValues() and ib.portfolio() are populated.
        # This is a streaming subscription — data arrives asynchronously.
        import asyncio

        asyncio.create_task(self._subscribe_async())
        asyncio.create_task(self._poll_executions())

    async def _poll_executions(self):
        import asyncio

        while True:
            await asyncio.sleep(5)
            try:
                if self.ib.isConnected():
                    await self.ib.reqExecutionsAsync()
            except Exception as e:
                LOGGER.warning(f"Failed to poll executions: {e}")

    async def _subscribe_async(self):
        try:
            self.ib.reqMarketDataType(
                3
            )  # Use delayed market data if live is not available

            # Await the async variants so they do not block the active Uvicorn event loop
            await self.ib.reqAccountUpdatesAsync(self.account or "")

            # Await the async variants so they do not block the active Uvicorn event loop
            await self.ib.reqAccountSummaryAsync()

            # Fetch open orders and bind to new orders
            await self.ib.reqAllOpenOrdersAsync()
            self.ib.reqAutoOpenOrders(True)

            # Fetch recent executions
            await self.ib.reqExecutionsAsync()

            # Subscribe to market data for all portfolio positions
            self.subscribe_market_data()
            import asyncio

            asyncio.create_task(self._delayed_subscribe_market_data())

            LOGGER.info(
                "Subscribed to IBKR account updates, summary, open orders, and market data"
            )
        except Exception as e:
            LOGGER.warning(f"Failed to subscribe to account updates: {e}")

    async def _delayed_subscribe_market_data(self):
        import asyncio

        await asyncio.sleep(2)
        self.subscribe_market_data()

    async def _fetch_prior_close(self, contract: Contract) -> Optional[float]:
        con_id = contract.conId
        if not con_id:
            return None
        self._prior_close_fetching.add(con_id)
        try:
            req_c = Stock(
                conId=contract.conId,
                symbol=contract.symbol,
                exchange="SMART",
                currency=contract.currency or "USD",
            )
            bars = await self.ib.reqHistoricalDataAsync(
                req_c,
                endDateTime="",
                durationStr="5 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            if bars:
                today = date.today()
                prior_bars = [
                    b
                    for b in bars
                    if (isinstance(b.date, date) and b.date < today)
                    or (isinstance(b.date, datetime) and b.date.date() < today)
                ]
                if prior_bars:
                    prior_close = prior_bars[-1].close
                elif len(bars) >= 2:
                    prior_close = bars[-2].close
                else:
                    prior_close = bars[0].close

                if prior_close and prior_close > 0:
                    self._prior_close_cache[con_id] = float(prior_close)
                    LOGGER.info(
                        f"Cached prior close for {contract.symbol} ({con_id}): {prior_close}"
                    )
                    return float(prior_close)
        except Exception as e:
            LOGGER.warning(f"Failed to fetch prior close for {contract.symbol}: {e}")
        finally:
            self._prior_close_fetching.discard(con_id)
        return None

    async def _prefetch_all_prior_closes(self):
        import asyncio

        today_str = date.today().isoformat()
        if self._prior_close_cache_date != today_str:
            self._prior_close_cache.clear()
            self._prior_close_cache_date = today_str

        tasks = []
        for item in self.ib.portfolio():
            if self.account and item.account != self.account:
                continue
            c = item.contract
            if (
                c.secType == "STK"
                and c.conId
                and c.conId not in self._prior_close_cache
            ):
                if c.conId not in self._prior_close_fetching:
                    tasks.append(self._fetch_prior_close(c))
        if tasks:
            LOGGER.info(f"Prefetching prior close for {len(tasks)} stock positions...")
            await asyncio.gather(*tasks, return_exceptions=True)
            LOGGER.info(
                f"Finished prefetching prior closes. Cache size: {len(self._prior_close_cache)}"
            )

    def subscribe_market_data(self):
        """Ensure market data is subscribed for all portfolio positions using SMART routing."""
        import asyncio

        for item in self.ib.portfolio():
            if self.account and item.account != self.account:
                continue
            contract = item.contract
            if contract.secType == "CASH":
                continue
            if contract.secType == "STK" or not contract.exchange:
                contract.exchange = "SMART"

            ticker = self.ib.ticker(contract)
            if not ticker:
                try:
                    generic_ticks = "106" if contract.secType in ["OPT", "FOP"] else ""
                    self.ib.reqMktData(contract, generic_ticks, False, False)
                except Exception as e:
                    LOGGER.warning(
                        f"Failed to subscribe market data for {contract.symbol}: {e}"
                    )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._prefetch_all_prior_closes())
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # Account summary
    # ------------------------------------------------------------------

    def get_account_summary(self) -> Dict[str, Any]:
        """
        Return key account metrics from IBKR.

        Uses ib.accountValues() which returns a list of AccountValue objects:
            AccountValue(account, tag, value, currency, modelCode)

        IBKR returns multiple entries per tag — one per currency ("USD",
        "SEK", etc.) plus optionally "BASE" for consolidated values.
        We prefer "BASE" or empty-currency entries; if none exist for a
        tag we fall back to the first currency-specific entry.
        """
        # Merge accountValues and accountSummary to support IB Gateway (Paper Trading)
        # where some values (CashBalance, PnL) only appear in accountSummary as $LEDGER- tags.
        # We access the wrapper dictionaries directly to avoid calling blocking methods
        # like ib.accountSummary() which would raise RuntimeError in the Uvicorn asyncio loop.
        account_values = list(self.ib.wrapper.accountValues.values())
        account_summary = list(self.ib.wrapper.acctSummary.values())
        values = account_values + account_summary
        if not values:
            return {}

        # Tags we care about (tag -> display label)
        WANTED_TAGS = {
            "NetLiquidation": "netLiquidation",
            "TotalCashValue": "totalCash",
            "BuyingPower": "buyingPower",
            "GrossPositionValue": "grossPositionValue",
            "MaintMarginReq": "maintMargin",
            "AvailableFunds": "availableFunds",
            "ExcessLiquidity": "excessLiquidity",
            "Cushion": "cushion",
            "UnrealizedPnL": "unrealizedPnL",
            "RealizedPnL": "realizedPnL",
            "FullMaintMarginReq": "fullMaintMargin",
            "FullInitMarginReq": "fullInitMargin",
            "InitMarginReq": "initMargin",
            "EquityWithLoanValue": "equityWithLoan",
        }

        # Collect all values per tag, grouped by priority:
        #   priority 0: currency == "" or "BASE" (consolidated)
        #   priority 1: any specific currency (fallback)
        # tag -> { priority: (value_str, currency) }
        tag_values: Dict[str, Dict[int, tuple]] = {}
        account_id = None
        base_currency = None

        for av in values:
            if self.account and av.account != self.account:
                continue

            if account_id is None:
                account_id = av.account

            tag = av.tag
            if tag.startswith("$LEDGER-"):
                tag = tag.replace("$LEDGER-", "")

            # Detect the account's base currency from NetLiquidation
            if tag == "NetLiquidation" and av.currency not in ("", "BASE"):
                if base_currency is None:
                    base_currency = av.currency

            if tag not in WANTED_TAGS:
                continue

            if tag not in tag_values:
                tag_values[tag] = {}

            if av.currency in ("", "BASE"):
                tag_values[tag][0] = (av.value, av.currency)
            elif base_currency and av.currency == base_currency:
                # Prefer the base currency over other currencies
                if 1 not in tag_values[tag]:
                    tag_values[tag][1] = (av.value, av.currency)
            else:
                if 2 not in tag_values[tag]:
                    tag_values[tag][2] = (av.value, av.currency)

        summary: Dict[str, Any] = {}

        for tag, label in WANTED_TAGS.items():
            entry = tag_values.get(tag)
            if not entry:
                continue
            # Pick best priority: 0 (BASE) > 1 (base_currency) > 2 (any)
            val_str, currency = None, None
            for prio in (0, 1, 2):
                if prio in entry:
                    val_str, currency = entry[prio]
                    break
            if val_str is None:
                continue
            try:
                if tag == "Cushion":
                    summary[label] = float(val_str) * 100  # -> percentage
                else:
                    summary[label] = float(val_str)
            except (ValueError, TypeError):
                summary[label] = val_str

        summary["accountId"] = account_id
        summary["baseCurrency"] = base_currency
        summary["timestamp"] = datetime.now(timezone.utc).isoformat()
        return summary

    # ------------------------------------------------------------------
    # Portfolio positions
    # ------------------------------------------------------------------

    def _group_option_strategies(
        self, positions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        import collections

        options = [p for p in positions if p.get("secType") in ("OPT", "FOP")]
        others = [p for p in positions if p.get("secType") not in ("OPT", "FOP")]

        groups = collections.defaultdict(list)
        for o in options:
            sym = o.get("symbol", "")
            exp = o.get("expiry", "")
            groups[(sym, exp)].append(o)

        final_positions = others

        for (sym, exp), legs in groups.items():
            legs = [l for l in legs if l["position"] != 0]

            while len(legs) > 1:
                legs.sort(key=lambda x: x.get("strike", 0))
                matched = False
                match_indices = []
                name = ""

                # 1. Iron Condor
                lp = sp = sc = lc = -1
                for i, l in enumerate(legs):
                    if l["right"] == "P" and l["position"] > 0 and lp == -1:
                        lp = i
                    elif l["right"] == "P" and l["position"] < 0 and sp == -1:
                        sp = i
                    elif l["right"] == "C" and l["position"] < 0 and sc == -1:
                        sc = i
                    elif l["right"] == "C" and l["position"] > 0 and lc == -1:
                        lc = i

                if lp != -1 and sp != -1 and sc != -1 and lc != -1:
                    if (
                        legs[lp]["strike"]
                        <= legs[sp]["strike"]
                        <= legs[sc]["strike"]
                        <= legs[lc]["strike"]
                    ):
                        match_indices = [lp, sp, sc, lc]
                        name = (
                            "Iron Butterfly"
                            if legs[sp]["strike"] == legs[sc]["strike"]
                            else "Iron Condor"
                        )

                # 2. Straddle / Strangle
                if not match_indices:
                    lc = sc = lp = sp = -1
                    for i, l in enumerate(legs):
                        if l["right"] == "C" and l["position"] > 0 and lc == -1:
                            lc = i
                        elif l["right"] == "P" and l["position"] > 0 and lp == -1:
                            lp = i
                        elif l["right"] == "C" and l["position"] < 0 and sc == -1:
                            sc = i
                        elif l["right"] == "P" and l["position"] < 0 and sp == -1:
                            sp = i

                    if lc != -1 and lp != -1:
                        match_indices = [lc, lp]
                        name = (
                            "Straddle"
                            if legs[lc]["strike"] == legs[lp]["strike"]
                            else "Strangle"
                        )
                    elif sc != -1 and sp != -1:
                        match_indices = [sc, sp]
                        name = (
                            "Short Straddle"
                            if legs[sc]["strike"] == legs[sp]["strike"]
                            else "Short Strangle"
                        )

                # 3. Vertical Spreads
                if not match_indices:
                    lc = sc = lp = sp = -1
                    for i, l in enumerate(legs):
                        if l["right"] == "C" and l["position"] > 0 and lc == -1:
                            lc = i
                        elif l["right"] == "C" and l["position"] < 0 and sc == -1:
                            sc = i
                    if lc != -1 and sc != -1:
                        match_indices = [lc, sc]
                        name = (
                            "Bull Call Spread"
                            if legs[lc]["strike"] < legs[sc]["strike"]
                            else "Bear Call Spread"
                        )
                    else:
                        for i, l in enumerate(legs):
                            if l["right"] == "P" and l["position"] > 0 and lp == -1:
                                lp = i
                            elif l["right"] == "P" and l["position"] < 0 and sp == -1:
                                sp = i
                        if lp != -1 and sp != -1:
                            match_indices = [lp, sp]
                            name = (
                                "Bull Put Spread"
                                if legs[lp]["strike"] < legs[sp]["strike"]
                                else "Bear Put Spread"
                            )

                if match_indices:
                    extracted = [legs[i] for i in match_indices]
                    min_abs_pos = min(abs(e["position"]) for e in extracted)

                    combo_legs = []
                    for i in sorted(match_indices, reverse=True):
                        orig = legs.pop(i)
                        sign = 1 if orig["position"] > 0 else -1
                        ratio = min_abs_pos / abs(orig["position"])

                        leg = dict(orig)
                        leg["position"] = min_abs_pos * sign
                        leg["marketValue"] = orig["marketValue"] * ratio
                        leg["avgCost"] = orig["avgCost"] * ratio
                        leg["unrealizedPnL"] = orig["unrealizedPnL"] * ratio
                        leg["realizedPnL"] = orig["realizedPnL"] * ratio
                        combo_legs.append(leg)

                        if abs(orig["position"]) > min_abs_pos:
                            rem = dict(orig)
                            rem["position"] = orig["position"] - (min_abs_pos * sign)
                            rem_ratio = abs(rem["position"]) / abs(orig["position"])
                            rem["marketValue"] = orig["marketValue"] * rem_ratio
                            rem["avgCost"] = orig["avgCost"] * rem_ratio
                            rem["unrealizedPnL"] = orig["unrealizedPnL"] * rem_ratio
                            rem["realizedPnL"] = orig["realizedPnL"] * rem_ratio
                            legs.append(rem)

                    combo_legs.sort(key=lambda x: x.get("strike", 0))

                    combo_prev_close = 0.0
                    combo_current_price = 0.0
                    has_prev_close = True
                    combo_market_price = 0.0
                    combo_avg_price = 0.0
                    combo_delta = 0.0
                    has_delta = False
                    multiplier_val = combo_legs[0].get("multiplier", 100)

                    for c in combo_legs:
                        rel_pos = c["position"] / min_abs_pos

                        combo_market_price += (c.get("marketPrice") or 0.0) * rel_pos
                        combo_avg_price += (c.get("avgPrice") or 0.0) * rel_pos

                        leg_delta = c.get("delta")
                        if leg_delta is not None:
                            has_delta = True
                            combo_delta += leg_delta * rel_pos

                        chg_pct = c.get("changePercent")
                        mkt_px = c.get("marketPrice")
                        if chg_pct is not None and mkt_px is not None:
                            prev_close = mkt_px / (1 + chg_pct / 100)
                            combo_prev_close += prev_close * rel_pos
                            combo_current_price += mkt_px * rel_pos
                        else:
                            has_prev_close = False

                    combo_change_pct = None
                    if has_prev_close and combo_prev_close != 0:
                        combo_change_pct = round(
                            (combo_current_price - combo_prev_close)
                            / abs(combo_prev_close)
                            * 100,
                            2,
                        )

                    local_sym = f"{sym} {name} {exp}"
                    combo = {
                        "conId": "-".join(str(c["conId"]) for c in combo_legs),
                        "symbol": sym,
                        "localSymbol": local_sym,
                        "secType": "COMBO",
                        "exchange": combo_legs[0]["exchange"],
                        "currency": combo_legs[0]["currency"],
                        "multiplier": multiplier_val,
                        "position": min_abs_pos,
                        "marketPrice": round(combo_market_price, 4),
                        "marketValue": sum(c["marketValue"] for c in combo_legs),
                        "avgCost": sum(c["avgCost"] for c in combo_legs),
                        "avgPrice": round(combo_avg_price, 4),
                        "unrealizedPnL": sum(c["unrealizedPnL"] for c in combo_legs),
                        "realizedPnL": sum(c["realizedPnL"] for c in combo_legs),
                        "changePercent": combo_change_pct,
                        "pnlPercent": 0.0,
                        "delta": round(combo_delta, 3) if has_delta else None,
                        "legs": combo_legs,
                    }
                    if combo["avgCost"] != 0:
                        combo["pnlPercent"] = (
                            combo["unrealizedPnL"] / abs(combo["avgCost"]) * 100
                        )

                    final_positions.append(combo)
                    matched = True

                if not matched:
                    break

            final_positions.extend(legs)

        return final_positions

    def get_portfolio_positions(self) -> List[Dict[str, Any]]:
        """
        Return all portfolio positions with market values and P&L.

        Uses ib.portfolio() which returns PortfolioItem objects:
            PortfolioItem(contract, position, marketPrice, marketValue,
                          averageCost, unrealizedPNL, realizedPNL, account)
        """
        items = self.ib.portfolio()
        positions = []

        for item in items:
            if self.account and item.account != self.account:
                continue

            contract = item.contract
            position = float(item.position)
            market_price = float(item.marketPrice) if item.marketPrice else 0.0
            market_value = float(item.marketValue) if item.marketValue else 0.0
            avg_cost = float(item.averageCost) if item.averageCost else 0.0
            unrealized_pnl = float(item.unrealizedPNL) if item.unrealizedPNL else 0.0
            realized_pnl = float(item.realizedPNL) if item.realizedPNL else 0.0

            # Compute per-unit average cost (for futures, divide by multiplier)
            multiplier = 1.0
            try:
                if contract.multiplier:
                    multiplier = float(contract.multiplier)
                    if multiplier <= 0:
                        multiplier = 1.0
            except (ValueError, TypeError):
                multiplier = 1.0

            avg_price = avg_cost / multiplier if avg_cost else 0.0

            # Compute P&L percentage
            cost_basis = abs(position) * avg_price * multiplier
            pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis else 0.0

            change_pct = None
            delta = None
            if contract.secType != "CASH":
                if contract.secType == "STK" or not contract.exchange:
                    contract.exchange = "SMART"

                ticker = self.ib.ticker(contract)
                if not ticker:
                    try:
                        generic_ticks = (
                            "106" if contract.secType in ["OPT", "FOP"] else ""
                        )
                        self.ib.reqMktData(contract, generic_ticks, False, False)
                        ticker = self.ib.ticker(contract)
                    except Exception:
                        pass

                if (
                    ticker
                    and getattr(ticker, "modelGreeks", None)
                    and ticker.modelGreeks.delta is not None
                ):
                    delta = ticker.modelGreeks.delta

                close_price = None
                if (
                    ticker
                    and getattr(ticker, "close", None) is not None
                    and ticker.close == ticker.close
                    and ticker.close > 0
                ):
                    close_price = ticker.close
                    if contract.conId:
                        self._prior_close_cache[contract.conId] = float(ticker.close)
                elif contract.conId in self._prior_close_cache:
                    close_price = self._prior_close_cache[contract.conId]
                elif contract.secType == "STK" and contract.conId:
                    if contract.conId not in self._prior_close_fetching:
                        import asyncio

                        try:
                            loop = asyncio.get_running_loop()
                            loop.create_task(self._fetch_prior_close(contract))
                        except RuntimeError:
                            pass

                if close_price and close_price > 0:
                    current_price = ticker.marketPrice() if ticker else 0.0
                    if current_price != current_price or current_price == 0:
                        current_price = market_price

                    if current_price and current_price > 0:
                        change_pct = (current_price - close_price) / close_price * 100

            pos_dict = {
                "conId": contract.conId,
                "symbol": contract.symbol or "",
                "localSymbol": contract.localSymbol or "",
                "secType": contract.secType or "",
                "exchange": contract.exchange or "",
                "currency": contract.currency or "",
                "multiplier": multiplier,
                "position": position,
                "marketPrice": round(market_price, 4),
                "marketValue": round(market_value, 2),
                "avgCost": round(avg_cost, 2),
                "avgPrice": round(avg_price, 4),
                "changePercent": (
                    round(change_pct, 2) if change_pct is not None else None
                ),
                "unrealizedPnL": round(unrealized_pnl, 2),
                "realizedPnL": round(realized_pnl, 2),
                "pnlPercent": round(pnl_pct, 2),
                "account": item.account or "",
                "delta": round(delta, 3) if delta is not None else None,
            }

            if contract.secType == "OPT" or contract.secType == "FOP":
                pos_dict["strike"] = contract.strike
                pos_dict["right"] = contract.right
                pos_dict["expiry"] = contract.lastTradeDateOrContractMonth

            positions.append(pos_dict)

        # Apply grouping logic
        positions = self._group_option_strategies(positions)

        # Also include Cash balances as positions (like IBKR TWS)
        account_values = list(self.ib.wrapper.accountValues.values())
        account_summary = list(self.ib.wrapper.acctSummary.values())
        merged_values = account_values + account_summary

        exchange_rates = {}
        for av in merged_values:
            if av.account == "All":
                continue
            if self.account and av.account != self.account:
                continue

            tag = (
                av.tag.replace("$LEDGER-", "")
                if av.tag.startswith("$LEDGER-")
                else av.tag
            )
            if tag == "ExchangeRate" and av.currency not in ("", "BASE"):
                try:
                    exchange_rates[av.currency] = float(av.value)
                except (ValueError, TypeError):
                    pass

        seen_cash_balances = set()
        for av in merged_values:
            if av.account == "All":
                continue
            if self.account and av.account != self.account:
                continue

            tag = (
                av.tag.replace("$LEDGER-", "")
                if av.tag.startswith("$LEDGER-")
                else av.tag
            )
            if tag == "CashBalance" and av.currency not in ("", "BASE"):
                cache_key = (av.account, av.currency)
                if cache_key in seen_cash_balances:
                    continue
                seen_cash_balances.add(cache_key)

                try:
                    cash_val = float(av.value)
                    if cash_val != 0:
                        rate = exchange_rates.get(av.currency, 1.0)
                        market_value = cash_val * rate
                        positions.append(
                            {
                                "conId": 0,
                                "symbol": av.currency,
                                "localSymbol": f"{av.currency}.CASH",
                                "secType": "CASH",
                                "exchange": "",
                                "currency": av.currency,
                                "multiplier": 1.0,
                                "position": cash_val,
                                "marketPrice": round(rate, 4),
                                "marketValue": round(market_value, 2),
                                "avgCost": 0.0,
                                "avgPrice": 0.0,
                                "changePercent": None,
                                "unrealizedPnL": 0.0,
                                "realizedPnL": 0.0,
                                "pnlPercent": 0.0,
                                "account": av.account or "",
                            }
                        )
                except (ValueError, TypeError):
                    pass

        # Sort by absolute market value descending
        positions.sort(key=lambda p: abs(p["marketValue"]), reverse=True)
        return positions

    # ------------------------------------------------------------------
    # Open orders
    # ------------------------------------------------------------------

    async def _resolve_contract_symbol_async(self, contract) -> tuple[str, str]:
        symbol_resolved = contract.symbol or ""
        local_symbol_resolved = contract.localSymbol or ""

        if contract.secType == "BAG" and contract.comboLegs:
            cache_key = tuple(leg.conId for leg in contract.comboLegs)
            if cache_key in self._combo_symbol_cache:
                local_symbol_resolved = self._combo_symbol_cache[cache_key]
            else:
                from ib_async import Contract

                leg_contracts = [
                    Contract(conId=leg.conId) for leg in contract.comboLegs
                ]
                try:
                    await self.ib.qualifyContractsAsync(*leg_contracts)
                    desc_parts = []
                    for leg, c in zip(contract.comboLegs, leg_contracts):
                        if c.strike:
                            desc_parts.append(
                                f"{leg.action} {leg.ratio}x {c.strike}{c.right}"
                            )
                        else:
                            desc_parts.append(
                                f"{leg.action} {leg.ratio}x {c.localSymbol}"
                            )

                    if desc_parts:
                        local_symbol_resolved = (
                            f"{symbol_resolved} (" + ", ".join(desc_parts) + ")"
                        )
                        self._combo_symbol_cache[cache_key] = local_symbol_resolved
                    else:
                        local_symbol_resolved = f"{symbol_resolved} COMBO"
                except Exception as e:
                    import logging

                    LOGGER = logging.getLogger("ibkr_portfolio")
                    LOGGER.warning(
                        f"Failed to qualify combo legs for {symbol_resolved}: {e}"
                    )
                    local_symbol_resolved = f"{symbol_resolved} COMBO"

        return symbol_resolved, local_symbol_resolved

    async def get_open_orders_async(self) -> List[Dict[str, Any]]:
        """
        Return all open/active orders across all contracts.

        Returns a list of dicts with order details suitable for frontend rendering.
        """
        orders = []
        for trade in self.ib.openTrades():
            if not trade.isActive():
                continue

            order = trade.order
            if self.account and order.account != self.account:
                continue

            contract = trade.contract

            price = None
            if order.orderType == "STP":
                price = order.auxPrice
            elif order.orderType == "LMT":
                price = order.lmtPrice
            elif order.orderType == "STP LMT":
                price = order.auxPrice
            elif order.orderType == "MKT":
                price = None  # Market orders have no fixed price

            placed_time = None
            if trade.log:
                placed_time = trade.log[0].time
            if not placed_time:
                placed_time = datetime.now(timezone.utc)

            parent_id = getattr(order, "parentId", 0)
            oca_group = getattr(order, "ocaGroup", "")

            symbol_resolved, local_symbol_resolved = (
                await self._resolve_contract_symbol_async(contract)
            )

            orders.append(
                {
                    "orderId": order.orderId,
                    "permId": order.permId,
                    "symbol": symbol_resolved,
                    "localSymbol": local_symbol_resolved,
                    "secType": contract.secType or "",
                    "action": order.action,
                    "orderType": order.orderType,
                    "totalQuantity": int(order.totalQuantity),
                    "price": price,
                    "status": trade.orderStatus.status,
                    "parentId": parent_id if parent_id else None,
                    "ocaGroup": oca_group if oca_group else None,
                    "tif": order.tif or "",
                    "placedTime": placed_time.isoformat() if placed_time else None,
                    "filledQuantity": int(trade.orderStatus.filled),
                    "remaining": int(trade.orderStatus.remaining),
                    "avgFillPrice": (
                        float(trade.orderStatus.avgFillPrice)
                        if trade.orderStatus.avgFillPrice
                        else None
                    ),
                }
            )

        return orders

    # ------------------------------------------------------------------
    # Executions / fills
    # ------------------------------------------------------------------

    async def get_executions_async(self) -> List[Dict[str, Any]]:
        """
        Return recent executions from the current IBKR session.

        Uses ib.fills() which returns Fill objects:
            Fill(contract, execution, commissionReport, time)
        """
        fills = self.ib.fills()
        executions = []

        for fill in fills:
            exec_ = fill.execution
            if self.account and exec_.acctNumber != self.account:
                continue

            contract = fill.contract
            comm = fill.commissionReport

            symbol_resolved, local_symbol_resolved = (
                await self._resolve_contract_symbol_async(contract)
            )

            executions.append(
                {
                    "execId": exec_.execId,
                    "symbol": symbol_resolved,
                    "localSymbol": local_symbol_resolved,
                    "secType": contract.secType or "",
                    "side": exec_.side,  # "BOT" or "SLD"
                    "quantity": int(exec_.shares),
                    "price": float(exec_.price),
                    "time": exec_.time.isoformat() if exec_.time else None,
                    "exchange": exec_.exchange or "",
                    "orderId": exec_.orderId,
                    "commission": (
                        float(comm.commission) if comm and comm.commission else 0.0
                    ),
                    "realizedPnL": (
                        float(comm.realizedPNL) if comm and comm.realizedPNL else None
                    ),
                    "currency": contract.currency or "",
                }
            )

        # Sort by time descending (most recent first)
        executions.sort(key=lambda e: e["time"] or "", reverse=True)
        return executions

    # ------------------------------------------------------------------
    # P&L summary
    # ------------------------------------------------------------------

    def get_pnl_summary(self) -> Dict[str, Any]:
        """
        Return aggregated P&L information.

        Combines data from ib.pnl() (account-level) and positions.
        """
        positions = self.get_portfolio_positions()
        total_unrealized = sum(p["unrealizedPnL"] for p in positions)
        total_realized = sum(p["realizedPnL"] for p in positions)
        total_market_value = sum(p["marketValue"] for p in positions)

        # Count winning/losing positions
        winning = sum(1 for p in positions if p["unrealizedPnL"] > 0)
        losing = sum(1 for p in positions if p["unrealizedPnL"] < 0)
        flat = sum(1 for p in positions if p["unrealizedPnL"] == 0)

        return {
            "totalUnrealizedPnL": round(total_unrealized, 2),
            "totalRealizedPnL": round(total_realized, 2),
            "totalPnL": round(total_unrealized + total_realized, 2),
            "totalMarketValue": round(total_market_value, 2),
            "positionCount": len(positions),
            "winningPositions": winning,
            "losingPositions": losing,
            "flatPositions": flat,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
