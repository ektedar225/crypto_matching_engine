import pytest
from decimal import Decimal
import asyncio
import json


from matching_engine import MatchEngine, OrderIn


pytestmark = pytest.mark.asyncio


def print_test_step(description: str, order_in: OrderIn, result: dict = None):
    """Prints a formatted block showing the input and output of an engine action."""
    print(f"\n--- {description} ---")
    
    print(f"INPUT  ==> {json.dumps(order_in.model_dump(mode='json'), indent=2)}")
    if result:
       
        print(f"OUTPUT ==> {json.dumps(result, indent=2)}")
    print("--------------------" + "-" * len(description))



@pytest.fixture
def engine():
    """Provides a clean instance of the MatchEngine for each test, disabling state file."""
    return MatchEngine(state_file=None)

async def test_simple_limit_order_match(engine: MatchEngine):
    print("\n\n#################### Testing: Simple Limit Order Match ####################")
    # Step 1: Place a resting sell order
    sell_order_in = OrderIn(symbol="BTC-USDT", order_type="limit", side="sell", quantity="0.5", price="50000")
    sell_result = await engine.submit_order(sell_order_in)
    print_test_step("1. Place Resting Sell Order (Maker)", sell_order_in, sell_result)

    # Step 2: Place a matching buy order to create a trade
    buy_order_in = OrderIn(symbol="BTC-USDT", order_type="limit", side="buy", quantity="0.5", price="50000")
    buy_result = await engine.submit_order(buy_order_in)
    print_test_step("2. Place Matching Buy Order (Taker)", buy_order_in, buy_result)

    assert buy_result["accepted"] is True
    assert len(buy_result["fills"]) == 1
    fill = buy_result["fills"][0]
    assert Decimal(fill["price"]) == Decimal("50000")
    assert Decimal(fill["quantity"]) == Decimal("0.5")
    assert fill["aggressor_side"] == "buy"
    assert engine.books["BTC-USDT"]["sell"].best_price() is None

async def test_partial_fill_and_resting_order(engine: MatchEngine):
    print("\n\n#################### Testing: Partial Fill and Resting Order ####################")
    # Step 1: Place a large resting sell order
    sell_order_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="1.0", price="50000")
    sell_result = await engine.submit_order(sell_order_in)
    print_test_step("1. Place Large Resting Sell Order", sell_order_in, sell_result)

    # Step 2: Partially fill it with a smaller buy order
    buy_order_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="limit", quantity="0.2", price="50000")
    buy_result = await engine.submit_order(buy_order_in)
    print_test_step("2. Place Smaller Buy Order to Partially Fill", buy_order_in, buy_result)

    sell_side = engine.books["BTC-USDT"]["sell"]
    assert sell_side.best_price() == Decimal("50000")
    assert sell_side.levels[Decimal("50000")].total_qty == Decimal("0.8")

async def test_price_time_priority(engine: MatchEngine):
    print("\n\n#################### Testing: Price-Time Priority (FIFO) ####################")
    # Step 1 & 2: Place two sell orders at the same price
    sell1_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="1", price="50000")
    sell1_result = await engine.submit_order(sell1_in)
    print_test_step("1. Place First Sell Order (Should be filled first)", sell1_in, sell1_result)

    await asyncio.sleep(0.01) # Ensure different timestamp

    sell2_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="1", price="50000")
    sell2_result = await engine.submit_order(sell2_in)
    print_test_step("2. Place Second Sell Order", sell2_in, sell2_result)

    # Step 3: Match one of the orders
    buy_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="limit", quantity="1", price="50000")
    buy_result = await engine.submit_order(buy_in)
    print_test_step("3. Place Buy Order to Match First Sell Order", buy_in, buy_result)

    assert buy_result["fills"][0]["maker_order_id"] == sell1_result["order_id"]
    assert buy_result["fills"][0]["maker_order_id"] != sell2_result["order_id"]

async def test_market_order_sweeps_book(engine: MatchEngine):
    print("\n\n#################### Testing: Market Order Sweeping Multiple Levels ####################")
    # Step 1 & 2: Populate the book with two price levels
    sell1_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="0.5", price="50000")
    sell1_result = await engine.submit_order(sell1_in)
    print_test_step("1. Place Sell Order at Best Price", sell1_in, sell1_result)

    sell2_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="0.5", price="50001")
    sell2_result = await engine.submit_order(sell2_in)
    print_test_step("2. Place Sell Order at Second Best Price", sell2_in, sell2_result)

    # Step 3: A market order should fill both
    market_buy_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="market", quantity="1.0")
    market_buy_result = await engine.submit_order(market_buy_in)
    print_test_step("3. Market Buy Order Sweeps Both Levels", market_buy_in, market_buy_result)

    assert len(market_buy_result["fills"]) == 2
    assert {Decimal(f["price"]) for f in market_buy_result["fills"]} == {Decimal("50000"), Decimal("50001")}
    assert engine.books["BTC-USDT"]["sell"].best_price() is None

async def test_ioc_order(engine: MatchEngine):
    print("\n\n#################### Testing: Immediate-Or-Cancel (IOC) Order ####################")
    # Step 1: Add liquidity to the book
    sell_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="0.5", price="50000")
    sell_result = await engine.submit_order(sell_in)
    print_test_step("1. Place Resting Sell Order", sell_in, sell_result)

    # Step 2: IOC order is larger than available liquidity, should partially fill and cancel remainder
    ioc_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="ioc", quantity="1.0", price="50000")
    ioc_result = await engine.submit_order(ioc_in)
    print_test_step("2. IOC Buy Partially Fills and Cancels Remainder", ioc_in, ioc_result)

    assert len(ioc_result["fills"]) == 1
    assert Decimal(ioc_result["fills"][0]["quantity"]) == Decimal("0.5")
    assert Decimal(ioc_result["remaining"]) == Decimal("0")
    assert engine.books["BTC-USDT"]["buy"].best_price() is None

async def test_fok_order_success(engine: MatchEngine):
    print("\n\n#################### Testing: Fill-Or-Kill (FOK) Order - Success ####################")
    # Step 1: Add enough liquidity to fill the FOK order
    sell_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="1.0", price="50000")
    sell_result = await engine.submit_order(sell_in)
    print_test_step("1. Place Resting Order with Sufficient Quantity", sell_in, sell_result)

    # Step 2: FOK order can be fully filled
    fok_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="fok", quantity="1.0", price="50000")
    fok_result = await engine.submit_order(fok_in)
    print_test_step("2. FOK Buy Order is Successfully Filled", fok_in, fok_result)

    assert fok_result["accepted"] is True
    assert len(fok_result["fills"]) == 1

async def test_fok_order_failure(engine: MatchEngine):
    print("\n\n#################### Testing: Fill-Or-Kill (FOK) Order - Failure ####################")
    # Step 1: Add insufficient liquidity
    sell_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="0.5", price="50000")
    sell_result = await engine.submit_order(sell_in)
    print_test_step("1. Place Resting Order with Insufficient Quantity", sell_in, sell_result)

    # Step 2: FOK order cannot be fully filled, should be rejected
    fok_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="fok", quantity="1.0", price="50000")
    fok_result = await engine.submit_order(fok_in)
    print_test_step("2. FOK Buy Order is Rejected", fok_in, fok_result)

    assert fok_result["accepted"] is False
    assert engine.books["BTC-USDT"]["sell"].levels[Decimal("50000")].total_qty == Decimal("0.5")

async def test_stop_loss_trigger(engine: MatchEngine):
    print("\n\n#################### BONUS Testing: Stop-Loss Order Trigger ####################")
    # Step 1: Place the stop-loss order. It sits pending.
    stop_order_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="stop_loss", quantity="1.0", stop_price="49000")
    submission_result = await engine.submit_order(stop_order_in)
    print_test_step("1. Place PENDING Stop-Loss Sell Order", stop_order_in, submission_result)
    stop_order_id = submission_result["order_id"]

    assert len(engine.stop_orders) == 1
    
    # Step 2: Simulate a trade below the stop price to trigger it.
    # First, a resting buy order is needed to provide liquidity for the trigger trade.
    resting_buy_in = OrderIn(symbol="BTC-USDT", side="buy", order_type="limit", quantity="0.1", price="48999")
    resting_buy_result = await engine.submit_order(resting_buy_in)
    print_test_step("2. Place Resting Buy (Liquidity for trigger)", resting_buy_in, resting_buy_result)

    # Now, a sell order hits that bid, creating a trade at 48999. This is the trigger.
    trigger_sell_in = OrderIn(symbol="BTC-USDT", side="sell", order_type="limit", quantity="0.1", price="48999")
    trigger_sell_result = await engine.submit_order(trigger_sell_in)
    print_test_step("3. Place Aggressor Sell to Create Trigger Trade at 48999", trigger_sell_in, trigger_sell_result)
    
    print("\n--- After trigger, the stop order was converted to a Market Order and processed. ---")
    print(f"--- The engine's final output for the trigger trade shows its own fill AND any fills from the triggered stop-loss order. ---")
    print(f"OUTPUT of Triggering Action ==> {json.dumps(trigger_sell_result, indent=2)}")

    # The trade at 48999 should have triggered the stop loss.
    assert len(engine.stop_orders) == 0
    
    # Verify the order was processed and its type was changed in the main order map.
    triggered_order = engine.order_map.get(stop_order_id)
    assert triggered_order is not None
    assert triggered_order.order_type == "market"