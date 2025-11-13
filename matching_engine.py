#new #!/usr/bin/env python3
"""
Name:-Ektedar Ahmad


matching_engine.py

High-Performance Cryptocurrency Matching Engine (Optimized & Refactored)
- Implements per-symbol locking for concurrent order processing.
- Uses heaps for efficient stop-order triggering (O(log N) instead of O(N)).
- Fixes a critical concurrency deadlock bug related to triggered orders.
- Optimizes market data snapshots using heapq.nsmallest.
- Replaces insecure pickle with JSON for robust state persistence.
"""
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import json # OPTIMIZATION: Replaced pickle with json
import os
from collections import deque, defaultdict # OPTIMIZATION: Imported defaultdict
import heapq
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from pydantic import BaseModel, Field, field_validator
import uvicorn

# Set decimal precision
getcontext().prec = 18

# --- Comprehensive Logging Setup (Unchanged) ---
def setup_logging():
    """Configures logging for both a general audit trail and a dedicated latency log."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # 1. Main Application Logger (for general info, warnings, errors)
    app_logger = logging.getLogger("matching-engine")
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False

    console_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    
    file_handler = RotatingFileHandler('logs/engine.log', maxBytes=5*1024*1024, backupCount=5)
    file_handler.setFormatter(file_formatter)

    if not app_logger.handlers:
        app_logger.addHandler(console_handler)
        app_logger.addHandler(file_handler)

    # 2. Dedicated Latency Logger (for performance metrics only)
    latency_logger = logging.getLogger("latency-logger")
    latency_logger.setLevel(logging.INFO)
    latency_logger.propagate = False

    latency_handler = RotatingFileHandler('logs/latency.log', maxBytes=5*1024*1024, backupCount=5)
    latency_formatter = logging.Formatter('%(asctime)s - %(message)s') # Simple format for latency
    latency_handler.setFormatter(latency_formatter)

    if not latency_logger.handlers:
        latency_logger.addHandler(latency_handler)

    return app_logger

logger = setup_logging()
# --- End of Logging Setup ---


####################
# Domain models (Unchanged)
####################
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def generate_id(prefix="o"):
    return f"{prefix}_{uuid.uuid4().hex}"

@dataclass(order=True)
class Order:
    timestamp: datetime = field(compare=True)
    order_id: str = field(compare=False)
    symbol: str = field(compare=False)
    side: str = field(compare=False)
    order_type: str = field(compare=False)
    quantity: Decimal = field(compare=False)
    price: Optional[Decimal] = field(compare=False)
    remaining: Decimal = field(compare=False)
    meta: dict = field(default_factory=dict, compare=False)

class OrderIn(BaseModel):
    symbol: str = Field(..., json_schema_extra={"example": "BTC-USDT"})
    order_type: str = Field(..., json_schema_extra={"example": "limit"})
    side: str = Field(..., json_schema_extra={"example": "buy"})
    quantity: Decimal = Field(..., gt=Decimal("0"))
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None

    # ... (Validators are unchanged)
    @field_validator("order_type")
    @classmethod
    def check_type(cls, v: str) -> str:
        allowed_types = {"market", "limit", "ioc", "fok", "stop_loss", "stop_limit", "take_profit"}
        if v not in allowed_types:
            raise ValueError(f"order_type must be one of {allowed_types}")
        return v

    @field_validator("side")
    @classmethod
    def check_side(cls, v: str) -> str:
        if v not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        return v

    @field_validator("price")
    @classmethod
    def price_required_for_limit_types(cls, v: Optional[Decimal], values) -> Optional[Decimal]:
        if 'order_type' in values.data:
            order_type = values.data['order_type']
            if order_type in {"limit", "stop_limit", "take_profit"} and v is None:
                raise ValueError(f"{order_type} orders require a price")
        if v is not None and v <= 0:
            raise ValueError("price must be positive")
        return v

    @field_validator("stop_price")
    @classmethod
    def stop_price_required_for_stop_types(cls, v: Optional[Decimal], values) -> Optional[Decimal]:
        if 'order_type' in values.data:
            order_type = values.data['order_type']
            if order_type in {"stop_loss", "stop_limit", "take_profit"} and v is None:
                raise ValueError(f"stop_price required for {order_type} orders")
        if v is not None and v <= 0:
            raise ValueError("stop_price must be positive")
        return v


####################
# OrderBook
####################
class PriceLevel:
    def __init__(self, price: Decimal):
        self.price = price
        self.orders: deque[Order] = deque()
        self.total_qty = Decimal("0")

    def add(self, order: Order):
        self.orders.append(order)
        self.total_qty += order.remaining

    def remove_empty_front(self):
        while self.orders and self.orders[0].remaining == 0:
            self.orders.popleft()

    def pop_best(self) -> Optional[Order]:
        self.remove_empty_front()
        return self.orders[0] if self.orders else None

class OrderBookSide:
    def __init__(self, side: str):
        assert side in {"buy", "sell"}
        self.side = side
        self.levels: Dict[Decimal, PriceLevel] = {}
        self.price_heap: List[Decimal] = []

    def best_price(self) -> Optional[Decimal]:
        self._cleanup_heap()
        if not self.price_heap:
            return None
        return -self.price_heap[0] if self.side == "buy" else self.price_heap[0]

    def _cleanup_heap(self):
        while self.price_heap:
            price_key = self.price_heap[0]
            price = -price_key if self.side == "buy" else price_key
            if price in self.levels and self.levels[price].total_qty > 0:
                break
            heapq.heappop(self.price_heap)

    def add_order(self, order: Order):
        price = order.price
        if price not in self.levels:
            self.levels[price] = PriceLevel(price)
            key = -price if self.side == "buy" else price
            heapq.heappush(self.price_heap, key)
        self.levels[price].add(order)

    def remove_level_if_empty(self, price: Decimal):
        if price in self.levels and self.levels[price].total_qty == 0:
            del self.levels[price]

    def iter_top_n(self, n=10) -> List[Tuple[Decimal, Decimal]]:
        self._cleanup_heap()
        # OPTIMIZATION: Use heapq.nsmallest for O(k log N) instead of sorted() for O(N log N).
        if self.side == "buy":
            top_keys = heapq.nsmallest(n, self.price_heap)
            prices = [-p for p in top_keys]
        else:
            top_keys = heapq.nsmallest(n, self.price_heap)
            prices = top_keys
        
        out = []
        for price in prices:
            if price in self.levels:
                qty = self.levels[price].total_qty
                if qty > 0:
                    out.append((price, qty))
        return out


####################
# Matching Engine
####################
class MatchEngine:
    def __init__(self, state_file: str = "orderbook_state.json"): # OPTIMIZATION: Default to .json
        self.books: Dict[str, Dict[str, OrderBookSide]] = {}
        self.order_map: Dict[str, Order] = {}
        # OPTIMIZATION: Use heaps for stop orders for efficient O(log N) checks.
        # Structure: {symbol: {"buy": [min_heap_of_stop_prices], "sell": [max_heap_of_stop_prices]}}
        self.stop_orders: Dict[str, Dict[str, list]] = defaultdict(lambda: {"buy": [], "sell": []})
        self.last_trade_price: Dict[str, Decimal] = {}
        self.state_file = state_file
        self.trade_queue: asyncio.Queue = asyncio.Queue()
        self.md_queue: asyncio.Queue = asyncio.Queue()
        # OPTIMIZATION: Per-symbol locking for true concurrency.
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.trade_subscribers: List[WebSocket] = []
        self.md_subscribers: List[WebSocket] = []
        self.fee_model = {"maker": Decimal("0.001"), "taker": Decimal("0.002")}
        logger.info("Engine initialized.")

    def ensure_book(self, symbol: str):
        if symbol not in self.books:
            self.books[symbol] = {"buy": OrderBookSide("buy"), "sell": OrderBookSide("sell")}

    async def submit_order(self, order_in: OrderIn) -> Dict:
        timestamp = datetime.now(timezone.utc)
        order_id = generate_id("o")
        order = Order(
            timestamp=timestamp, order_id=order_id, symbol=order_in.symbol,
            side=order_in.side, order_type=order_in.order_type, quantity=order_in.quantity,
            price=order_in.price, remaining=order_in.quantity,
            meta={"stop_price": order_in.stop_price} if order_in.stop_price else {}
        )
        
        # FIX: Process triggered orders outside the main lock to prevent deadlocks.
        triggered_orders_to_process = []
        
        # OPTIMIZATION: Acquire lock for a specific symbol.
        async with self._locks[order.symbol]:
            if order.order_type in {"stop_loss", "stop_limit", "take_profit"}:
                self._add_stop_order(order)
                self.order_map[order.order_id] = order
                logger.info(f"Stop/TP order {order.order_id} accepted, pending trigger.")
                return {"accepted": True, "order_id": order.order_id, "status": "stop/trigger pending"}
            
            result, triggered_orders = await self._process_order(order)
            triggered_orders_to_process.extend(triggered_orders)

        # FIX: Process any newly triggered orders asynchronously.
        if triggered_orders_to_process:
            for triggered_order in triggered_orders_to_process:
                # Re-create OrderIn model to submit through the main path
                triggered_order_in = OrderIn(
                    symbol=triggered_order.symbol,
                    order_type=triggered_order.order_type,
                    side=triggered_order.side,
                    quantity=triggered_order.remaining,
                    price=triggered_order.price
                )
                asyncio.create_task(self.submit_order(triggered_order_in))

        return result

    def _add_stop_order(self, order: Order):
        """Adds a stop order to the appropriate heap."""
        symbol, side, stop_price = order.symbol, order.side, order.meta.get("stop_price")
        if not stop_price: return
        
        # For sell-side stops (stop-loss sell, take-profit sell), we trigger as price falls.
        # We want to check the highest stop price first, so we use a max-heap (negated values in min-heap).
        if side == "sell":
            heapq.heappush(self.stop_orders[symbol]["sell"], (-stop_price, order))
        # For buy-side stops (stop-loss buy, take-profit buy), we trigger as price rises.
        # We want to check the lowest stop price first, so we use a min-heap.
        else: # side == "buy"
            heapq.heappush(self.stop_orders[symbol]["buy"], (stop_price, order))

    async def _process_order(self, order: Order) -> Tuple[Dict, List[Order]]:
        symbol = order.symbol
        self.ensure_book(symbol)
        logger.info(f"Processing order {order.order_id}: {order.side} {order.order_type} {order.quantity} @ {order.price or 'market'}")
        fills = []
        opposite_book = self.books[symbol]["sell"] if order.side == "buy" else self.books[symbol]["buy"]

        if order.order_type == "fok":
            available_qty = self._get_available_fok_qty(order, opposite_book)
            if available_qty < order.quantity:
                logger.warning(f"FOK order {order.order_id} rejected: insufficient liquidity.")
                return {"accepted": False, "order_id": order.order_id, "reason": "FOK cannot be fully matched", "fills": []}, []

        while order.remaining > 0:
            best_price = opposite_book.best_price()
            if not self._is_matchable(order, best_price): break
            lvl = opposite_book.levels.get(best_price)
            if not lvl: break
            maker_order = lvl.pop_best()
            if not maker_order:
                opposite_book.remove_level_if_empty(best_price)
                continue
            
            exec_price, exec_qty = maker_order.price, min(order.remaining, maker_order.remaining)
            order.remaining -= exec_qty
            maker_order.remaining -= exec_qty
            lvl.total_qty -= exec_qty
            
            self.order_map.setdefault(order.order_id, order)
            self.order_map.setdefault(maker_order.order_id, maker_order)
            
            trade = self._create_trade_report(order, maker_order, exec_price, exec_qty)
            fills.append(trade)
            await self.trade_queue.put(trade)
            self.last_trade_price[symbol] = exec_price
            
            if lvl.total_qty == 0: opposite_book.remove_level_if_empty(best_price)

        # --- FIX for IOC: cancel remainder ---
        if order.order_type == "ioc" and order.remaining > 0:
            logger.info(f"IOC order {order.order_id} partially filled, cancelling remaining {order.remaining}")
            order.remaining = Decimal("0")
        # --- end fix ---
        if order.remaining > 0 and order.order_type == "limit":
            self.books[symbol][order.side].add_order(order)
            self.order_map[order.order_id] = order
            logger.info(f"Limit order {order.order_id} resting on book with remaining {order.remaining}")
        
        await self._publish_md_update(symbol)
        # FIX: Return triggered orders to be processed outside the lock.
        triggered_orders = self._check_stop_triggers(symbol, self.last_trade_price.get(symbol))
        
        result = {"accepted": True, "order_id": order.order_id, "fills": fills, "remaining": str(order.remaining)}
        return result, triggered_orders
    
    # ... (_is_matchable, _get_available_fok_qty, _create_trade_report are unchanged)
    def _is_matchable(self, order: Order, best_price: Optional[Decimal]) -> bool:
        if best_price is None: return False
        if order.order_type == "market": return True
        return order.price >= best_price if order.side == "buy" else order.price <= best_price

    def _get_available_fok_qty(self, order: Order, opposite_book: OrderBookSide) -> Decimal:
        total_available = Decimal("0")
        prices = sorted(opposite_book.levels.keys(), reverse=(order.side == "buy"))
        for price in prices:
            if self._is_matchable(order, price):
                total_available += opposite_book.levels[price].total_qty
        return total_available

    def _create_trade_report(self, taker: Order, maker: Order, price: Decimal, qty: Decimal) -> Dict:
        end_time = datetime.now(timezone.utc)
        latency_ms = (end_time - taker.timestamp).total_seconds() * 1000
        latency_logger = logging.getLogger("latency-logger")
        maker_side = "buy" if taker.side == "sell" else "sell"
        log_message = (
            f"{taker.side.capitalize()} order id {taker.order_id} matched with "
            f"{maker_side.capitalize()} order id {maker.order_id} in {latency_ms:.4f} milliseconds"
        )
        latency_logger.info(log_message)
        trade = {"timestamp": now_iso(), "symbol": taker.symbol, "trade_id": generate_id("t"),
                 "price": str(price), "quantity": str(qty), "aggressor_side": taker.side,
                 "maker_order_id": maker.order_id, "taker_order_id": taker.order_id}
        trade.update(self.calculate_fees(trade))
        logger.info(f"TRADE: {qty} {taker.symbol} @ {price} (Taker: {taker.order_id}, Maker: {maker.order_id})")
        return trade

    async def _publish_md_update(self, symbol: str):
        self.ensure_book(symbol)
        bids = [(str(p), str(q)) for p, q in self.books[symbol]["buy"].iter_top_n(10)]
        asks = [(str(p), str(q)) for p, q in self.books[symbol]["sell"].iter_top_n(10)]
        await self.md_queue.put({"timestamp": now_iso(), "symbol": symbol, "bids": bids, "asks": asks})

    def _check_stop_triggers(self, symbol: str, last_price: Optional[Decimal]) -> List[Order]:
        if last_price is None: return []
        
        triggered_orders = []
        
        # Check buy-side stops (trigger when price rises)
        buy_heap = self.stop_orders[symbol]["buy"]
        while buy_heap and buy_heap[0][0] <= last_price:
            _stop_price, order = heapq.heappop(buy_heap)
            logger.info(f"TRIGGERED: Buy Stop {order.order_id} at last_price={last_price}")
            order.order_type = "market" if order.order_type in ["stop_loss", "take_profit"] else "limit"
            triggered_orders.append(order)

        # Check sell-side stops (trigger when price falls)
        sell_heap = self.stop_orders[symbol]["sell"]
        while sell_heap and -sell_heap[0][0] >= last_price:
            _stop_price, order = heapq.heappop(sell_heap)
            logger.info(f"TRIGGERED: Sell Stop {order.order_id} at last_price={last_price}")
            order.order_type = "market" if order.order_type in ["stop_loss", "take_profit"] else "limit"
            triggered_orders.append(order)
            
        # After triggering
        if not self.stop_orders[symbol]["buy"] and not self.stop_orders[symbol]["sell"]:
            del self.stop_orders[symbol]
        return triggered_orders

    def calculate_fees(self, trade: Dict) -> Dict:
        notional = Decimal(trade["quantity"]) * Decimal(trade["price"])
        return {"maker_fee": str(notional * self.fee_model["maker"]), "taker_fee": str(notional * self.fee_model["taker"])}

    # OPTIMIZATION: Switched from pickle to JSON for state persistence.
    def save_state(self):
        try:
            state = {
                "order_map": {oid: o.__dict__ for oid, o in self.order_map.items()},
                "last_trade_price": {s: str(p) for s, p in self.last_trade_price.items()}
            }
            with open(self.state_file, "w") as f:
                json.dump(state, f, default=str, indent=2)
            logger.info(f"Engine state saved to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def load_state(self):
        if not os.path.exists(self.state_file):
            logger.warning("State file not found. Starting clean.")
            return
        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
            
            for oid, o_data in state.get("order_map", {}).items():
                order = Order(**{
                    **o_data,
                    'timestamp': datetime.fromisoformat(o_data['timestamp']),
                    'quantity': Decimal(o_data['quantity']),
                    'price': Decimal(o_data['price']) if o_data['price'] else None,
                    'remaining': Decimal(o_data['remaining']),
                    'meta': {'stop_price': Decimal(o_data['meta']['stop_price'])} if o_data.get('meta') and o_data['meta'].get('stop_price') else {}
                })
                self.order_map[oid] = order
                if order.order_type in {"stop_loss", "stop_limit", "take_profit"}:
                    self._add_stop_order(order)
                elif order.order_type == "limit" and order.remaining > 0:
                    self.ensure_book(order.symbol)
                    self.books[order.symbol][order.side].add_order(order)

            self.last_trade_price = {s: Decimal(p) for s, p in state.get("last_trade_price", {}).items()}
            logger.info(f"Engine state loaded from {self.state_file}")
        except Exception as e:
            logger.error(f"Error loading state: {e}. Starting fresh.")

    # ... (Broadcaster and WebSocket logic is unchanged)
    async def _broadcaster(self, queue: asyncio.Queue, subscribers: List[WebSocket], name: str):
        while True:
            message = await queue.get()
            dead_sockets = [ws for ws in subscribers if not await self._send_to_ws(ws, message)]
            if dead_sockets:
                logger.warning(f"Removing {len(dead_sockets)} dead subscribers from {name}.")
                for ws in dead_sockets: subscribers.remove(ws)

    async def _send_to_ws(self, ws: WebSocket, message: dict) -> bool:
        try:
            await ws.send_json(message)
            return True
        except Exception:
            return False

    async def register_ws(self, ws: WebSocket, subscribers: list):
        await ws.accept()
        subscribers.append(ws)
        feed_name = 'trades' if subscribers is self.trade_subscribers else 'marketdata'
        logger.info(f"New client connected to {feed_name} feed.")

# --- FastAPI App Setup (Unchanged) ---
engine = MatchEngine()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Matching engine starting up...")
    engine.load_state()
    trade_task = asyncio.create_task(engine._broadcaster(engine.trade_queue, engine.trade_subscribers, "trades"))
    md_task = asyncio.create_task(engine._broadcaster(engine.md_queue, engine.md_subscribers, "marketdata"))
    logger.info("Matching engine startup complete.")
    yield
    logger.info("Matching engine shutting down...")
    trade_task.cancel()
    md_task.cancel()
    engine.save_state()
    logger.info("Matching engine shutdown complete.")

app = FastAPI(title="Cryptocurrency Matching Engine", lifespan=lifespan)

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"status": "ok"}

@app.post("/order", status_code=status.HTTP_202_ACCEPTED)
async def submit_order_endpoint(order_in: OrderIn):
    try:
        result = await engine.submit_order(order_in)
        if not result.get("accepted", True): # FOK orders might not have 'accepted' key on failure
            reason = result.get('reason', 'Order rejected')
            logger.warning(f"Order rejected: {reason}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=reason)
        return result
    except ValueError as e:
        logger.error(f"Invalid order submission: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.critical(f"Unexpected server error processing order: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

@app.websocket("/ws/trades")
async def trades_websocket(websocket: WebSocket):
    await engine.register_ws(websocket, engine.trade_subscribers)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        engine.trade_subscribers.remove(websocket)
        logger.info("Trade websocket client disconnected.")

@app.websocket("/ws/marketdata")
async def marketdata_websocket(websocket: WebSocket):
    await engine.register_ws(websocket, engine.md_subscribers)
    try:
        if "BTC-USDT" in engine.books:
              await engine._publish_md_update("BTC-USDT")
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        engine.md_subscribers.remove(websocket)
        logger.info("MD websocket client disconnected.")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)