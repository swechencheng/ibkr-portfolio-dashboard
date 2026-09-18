import json
import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
import uvicorn

from ib_async import IB, util
from ibkr_portfolio import IbkrPortfolio

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)
LOGGER = logging.getLogger("backend")

# Load config
try:
    with open("config.json", "r") as f:
        config = json.load(f)
except Exception as e:
    LOGGER.error(f"Failed to load config.json: {e}")
    config = {}

server_port = config.get("server", {}).get("port", 6001)


# WebSocket Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                pass


class IBKREnvironment:
    def __init__(
        self,
        env_name: str,
        host: str,
        port: int,
        client_id: int,
        account: Optional[str] = None,
    ):
        self.env_name = env_name
        self.host = host
        self.port = port
        self.client_id = client_id
        self.account = account

        self.ib = IB()
        self.portfolio: Optional[IbkrPortfolio] = None
        self.manager = ConnectionManager()

        self._portfolio_update_task: Optional[asyncio.Task] = None
        self._portfolio_dirty: bool = False
        self._order_update_task: Optional[asyncio.Task] = None
        self._exec_update_task: Optional[asyncio.Task] = None

        self.ib.openOrderEvent += self.on_order_update
        self.ib.orderStatusEvent += self.on_order_update
        self.ib.execDetailsEvent += self.on_exec_update
        self.ib.commissionReportEvent += self.on_commission_update
        self.ib.updatePortfolioEvent += self.on_update_portfolio
        self.ib.accountValueEvent += self.on_update_account_value
        self.ib.errorEvent += self.on_error

    def on_order_update(self, trade):
        if not self.portfolio:
            return
        if self._order_update_task is None or self._order_update_task.done():

            async def debounced_orders():
                try:
                    await asyncio.sleep(0.3)
                    orders = await self.portfolio.get_open_orders_async()
                    await self.manager.broadcast(
                        {"type": "order_update", "orders": orders}
                    )
                except Exception as e:
                    LOGGER.error(f"[{self.env_name}] Error broadcasting orders: {e}")

            self._order_update_task = asyncio.create_task(debounced_orders())

    def _schedule_exec_broadcast(self):
        if not self.portfolio:
            return
        if self._exec_update_task is None or self._exec_update_task.done():

            async def debounced_execs():
                try:
                    await asyncio.sleep(0.3)
                    executions = await self.portfolio.get_executions_async()
                    await self.manager.broadcast(
                        {"type": "execution_update", "executions": executions}
                    )
                except Exception as e:
                    LOGGER.error(
                        f"[{self.env_name}] Error broadcasting executions: {e}"
                    )

            self._exec_update_task = asyncio.create_task(debounced_execs())

    def on_exec_update(self, trade, fill):
        self._schedule_exec_broadcast()
        self.on_order_update(trade)

    def on_commission_update(self, trade, fill, report):
        self._schedule_exec_broadcast()

    def _schedule_portfolio_broadcast(self):
        if not self.portfolio:
            return
        self._portfolio_dirty = True
        if self._portfolio_update_task is None or self._portfolio_update_task.done():
            self._portfolio_update_task = asyncio.create_task(
                self._debounced_portfolio_broadcast()
            )

    async def _debounced_portfolio_broadcast(self):
        try:
            # Throttle portfolio broadcast to max once per ~0.8s to avoid UI thrashing
            await asyncio.sleep(0.8)
            if self.portfolio and self._portfolio_dirty:
                self._portfolio_dirty = False
                summary = self.portfolio.get_account_summary()
                positions = self.portfolio.get_portfolio_positions()
                await self.manager.broadcast(
                    {
                        "type": "portfolio_update",
                        "summary": summary,
                        "positions": positions,
                    }
                )
        except Exception as e:
            LOGGER.error(
                f"[{self.env_name}] Error in debounced portfolio broadcast: {e}"
            )

    def on_update_portfolio(self, item):
        self._schedule_portfolio_broadcast()

    def on_error(self, reqId, errorCode, errorString):
        if errorCode == 1102 and self.portfolio:
            LOGGER.info(
                f"[{self.env_name}] Connection restored (1102). Refreshing all portfolio data..."
            )
            asyncio.create_task(self.portfolio._subscribe_async())

    def on_update_account_value(self, value):
        self._schedule_portfolio_broadcast()

    async def connect_loop(self):
        while True:
            try:
                if not self.ib.isConnected():
                    LOGGER.info(
                        f"[{self.env_name}] Connecting to IBKR {self.host}:{self.port} clientId={self.client_id}"
                    )
                    await self.ib.connectAsync(
                        self.host, self.port, clientId=self.client_id
                    )
                    LOGGER.info(f"[{self.env_name}] Connected to IBKR")
                    self.portfolio = IbkrPortfolio(self.ib, account=self.account)
                    await self.manager.broadcast(
                        {"type": "ibkr_status", "connected": True}
                    )
                await asyncio.sleep(5)
            except Exception as e:
                LOGGER.error(f"[{self.env_name}] IBKR Connection error: {e}")
                await self.manager.broadcast(
                    {"type": "ibkr_status", "connected": False}
                )
                await asyncio.sleep(5)


envs = {}
ibkr_settings = config.get("ibkr", {})
for env_name in ["paper", "real"]:
    if env_name in ibkr_settings:
        env_config = ibkr_settings[env_name]
        host = env_config.get("host", "127.0.0.1")
        port = env_config.get("port", 4002)
        client_id = env_config.get("client_id", 0)
        account = env_config.get("account")
        envs[env_name] = IBKREnvironment(env_name, host, port, client_id, account)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for env in envs.values():
        asyncio.create_task(env.connect_loop())

    yield

    for env in envs.values():
        if env.ib.isConnected():
            env.ib.disconnect()


app = FastAPI(lifespan=lifespan)


# REST Endpoints
@app.get("/api/{env_name}/portfolio/summary")
async def get_summary(env_name: str):
    env = envs.get(env_name)
    if env and env.portfolio and env.ib.isConnected():
        return env.portfolio.get_account_summary()
    return {}


@app.get("/api/{env_name}/portfolio/positions")
async def get_positions(env_name: str):
    env = envs.get(env_name)
    if env and env.portfolio and env.ib.isConnected():
        try:
            return {
                "positions": env.portfolio.get_portfolio_positions(),
                "pnl": env.portfolio.get_pnl_summary(),
            }
        except Exception as e:
            LOGGER.error(
                f"[{env_name}] Error getting portfolio positions: {e}", exc_info=True
            )
            return {"positions": [], "pnl": {}, "error": str(e)}
    return {"positions": [], "pnl": {}}


@app.get("/api/{env_name}/portfolio/orders")
async def get_orders(env_name: str):
    env = envs.get(env_name)
    if env and env.portfolio and env.ib.isConnected():
        return {"orders": await env.portfolio.get_open_orders_async()}
    return {"orders": []}


@app.get("/api/{env_name}/portfolio/executions")
async def get_executions(env_name: str):
    env = envs.get(env_name)
    if env and env.portfolio and env.ib.isConnected():
        return {"executions": await env.portfolio.get_executions_async()}
    return {"executions": []}


class CancelOrderReq(BaseModel):
    orderId: int


@app.post("/api/{env_name}/cancel_order")
async def cancel_order(env_name: str, req: CancelOrderReq):
    env = envs.get(env_name)
    if env and env.ib.isConnected():
        for trade in env.ib.openTrades():
            if trade.order.orderId == req.orderId or (
                req.orderId and getattr(trade.order, "permId", None) == req.orderId
            ):
                env.ib.cancelOrder(trade.order)
                return {"status": "success"}
    return {"status": "error", "detail": "Order not found or IB disconnected"}


# WebSockets
@app.websocket("/ws/{env_name}")
async def websocket_endpoint(websocket: WebSocket, env_name: str):
    env = envs.get(env_name)
    if not env:
        await websocket.close()
        return

    await env.manager.connect(websocket)
    try:
        await websocket.send_json(
            {"type": "ibkr_status", "connected": env.ib.isConnected()}
        )
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        env.manager.disconnect(websocket)


# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def get_index_redirect():
    return RedirectResponse(url="/paper")


@app.get("/paper")
async def get_paper_index():
    return FileResponse("static/index.html")


@app.get("/real")
async def get_real_index():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    uvicorn.run(
        "main:app", host="0.0.0.0", port=server_port, reload=True, log_level="warning"
    )
