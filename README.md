# High-Performance Cryptocurrency Matching Engine

A production-grade order matching engine for cryptocurrency trading with support for multiple order types, concurrent processing, and real-time market data streaming.

## Overview

This project implements a sophisticated financial matching engine that efficiently matches buy and sell orders for cryptocurrency trading pairs. It handles complex order types, manages concurrent order processing with per-symbol locking, and provides high-throughput order matching with sub-millisecond latency.

## Features

- **Multiple Order Types**: Support for limit, market, IOC (Immediate or Cancel), FOK (Fill or Kill), stop-loss, stop-limit, and take-profit orders
- **Concurrent Order Processing**: Per-symbol locking mechanism for safe concurrent order handling without deadlocks
- **High-Performance Matching**: Heap-based data structures for O(log N) stop-order triggering and efficient price level management
- **Real-Time API**: FastAPI-based REST endpoints with WebSocket support for live order updates and market data streaming
- **Robust State Persistence**: JSON-based serialization for secure and reliable state management
- **Comprehensive Logging**: Multi-level logging with rotating file handlers and dedicated latency tracking
- **Stress Testing**: Built-in stress testing framework for simulating high-volume trading scenarios

## Architecture

### Core Components

**MatchEngine**: Main engine managing order lifecycle, matching logic, and state persistence
- Order submission and validation
- Price-time priority matching algorithm
- Stop-order triggering using heap-based approach
- Multi-symbol order book management

**OrderBook**: Per-symbol order book implementation
- Separate buy and sell side order books
- Price levels with queue-based order management
- Best bid/ask tracking for market snapshots

**Order Types**:
- **Limit**: Orders executed at specified price or better
- **Market**: Immediate execution at best available price
- **IOC (Immediate or Cancel)**: Fill immediately or cancel remaining
- **FOK (Fill or Kill)**: Execute entire order or cancel
- **Stop-Loss**: Triggered when price falls below stop price
- **Stop-Limit**: Triggered stop with limit price execution
- **Take-Profit**: Triggered when price rises above stop price

### API Endpoints

- `POST /order` - Submit a new order
- `POST /order/{order_id}/cancel` - Cancel an existing order
- `GET /book/{symbol}` - Get current order book snapshot
- `GET /trades/{symbol}` - Get recent trades for a symbol
- `WebSocket /ws` - Real-time order updates and market data stream

## Technical Stack

- **Language**: Python 3.8+
- **Web Framework**: FastAPI
- **Async Runtime**: asyncio
- **Data Validation**: Pydantic
- **Testing**: pytest
- **Server**: Uvicorn

## Installation

### Prerequisites

- Python 3.8 or higher
- pip or conda

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd matching-engine
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Engine

Start the matching engine server:

```bash
python matching_engine.py
```

The server will start on `http://127.0.0.1:8000`

### API Documentation

Once running, visit `http://127.0.0.1:8000/docs` for interactive API documentation (Swagger UI)

### Submitting Orders

Example: Place a limit buy order

```bash
curl -X POST "http://127.0.0.1:8000/order" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTC-USDT",
    "order_type": "limit",
    "side": "buy",
    "quantity": 0.5,
    "price": 50000
  }'
```

Example: Place a stop-loss order

```bash
curl -X POST "http://127.0.0.1:8000/order" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTC-USDT",
    "order_type": "stop_loss",
    "side": "sell",
    "quantity": 1.0,
    "stop_price": 45000
  }'
```

### Running Tests

Execute the comprehensive test suite:

```bash
pytest test_matching_engine.py -v
```

Run with detailed output:

```bash
pytest test_matching_engine.py -v -s
```

### Stress Testing

Run stress tests to evaluate performance under high load:

```bash
python stress_test.py
```

This will send 1000 concurrent orders and report throughput metrics.

## Performance Characteristics

- Order Matching: O(log N) average case using binary search on price levels
- Stop-Order Triggering: O(log N) using min-heap for triggered stop orders
- Best Bid/Ask: O(1) constant-time lookup using heap operations
- Concurrent Safety: Per-symbol locks ensure thread-safe concurrent processing

## Logging

The engine produces two types of logs:

- **Application Log** (`logs/engine.log`): General application events, warnings, and errors
- **Latency Log** (`logs/latency.log`): Performance metrics and order processing times

Logs are rotated automatically at 5MB with 5 backup files retained.

## State Management

The engine persists state to `orderbook_state.json` using JSON serialization. This enables:
- Recovery from restarts
- State auditing
- External system integration

## Key Optimizations

1. **Per-Symbol Locking**: Reduces contention by isolating locks to individual trading pairs instead of global locks
2. **Heap-Based Stop Orders**: Achieves O(log N) triggered stop-order management instead of O(N) linear scans
3. **Efficient Market Snapshots**: Uses `heapq.nsmallest` for quick price level retrieval
4. **Secure Serialization**: Replaced pickle with JSON to prevent code injection vulnerabilities
5. **Async/Await Processing**: Non-blocking I/O for handling thousands of concurrent connections

## Bug Fixes

- Fixed critical deadlock issue in triggered stop-order processing
- Corrected edge cases in partial order fills
- Resolved concurrency issues in order cancellation

## Project Structure

```
matching-engine/
├── matching_engine.py          # Main engine implementation
├── test_matching_engine.py     # Comprehensive test suite
├── stress_test.py              # Performance stress testing
├── orderbook_state.json        # Persisted engine state
├── requirements.txt            # Python dependencies
├── logs/                       # Application and latency logs
└── README.md                   # This file
```

## Future Enhancements

- Multi-exchange aggregation
- Advanced order types (trailing stops, iceberg orders)
- Market data feed integration
- Performance optimization for ultra-low latency
- Distributed order book architecture for scalability

## Testing Coverage

The test suite covers:
- Simple limit order matching
- Partial fills and remaining orders
- Market order execution
- IOC (Immediate or Cancel) behavior
- FOK (Fill or Kill) behavior
- Stop-loss order triggering
- Order cancellation edge cases
- Concurrent order processing
- Order book state consistency

## Contributing

Contributions are welcome. Please ensure:
- All tests pass: `pytest test_matching_engine.py`
- Code follows the existing style
- New features include corresponding tests
- Logging is appropriate for production use

## License

This project is provided as-is for educational and professional use.

## Author

Ektedar Ahmad

## Contact

For questions or issues, please open an issue in the repository or contact the maintainers.

---

## Performance Benchmarks

When running stress tests on typical hardware:
- Throughput: 1000+ orders processed in 2-3 seconds
- Average latency: Single-digit milliseconds per order
- Memory usage: Efficient O(N) space for N active orders
- Concurrent connections: Handles 100+ simultaneous WebSocket connections

## Disclaimer

This is a demonstration of matching engine concepts. For production use in real trading systems, additional features should be implemented:
- Risk management and position limits
- Comprehensive audit trails
- High-availability and disaster recovery
- Market surveillance and compliance
- Integration with external liquidity sources
#   c r y p t o _ m a t c h i n g _ e n g i n e  
 