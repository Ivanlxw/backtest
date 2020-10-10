# Simple Backtesting 

A simple backtester to test trading algorithms and portfolio optimization strategies.

## Setup
`python -m pip install -r requirements.txt && python -m pip install -e .`

## Usage
`python loop.py`

`loop.py` - runs the backtester. 

This repo is meant to be as low-level as possible to get greater control of the backtesting environment. Edit the various scripts explained below and import them to `loop.py` to test your strategies. 


## Details
This event-driven backtester consists of the following: 
- DataHandler (Defines trading universe, reads in necessary data)
- Execution Enironment (Details about the exchange that might affect backtesting strategies)
- Portfolio (Executes `OrderEvent` based on `SignalEvent`, updates portfolio as necessary)
- Strategy (Looks for `SignalEvent` to be routed to portfolio)

### DataHandler
`HistoricCSVDataHandler`

**Arguments:**

* `event_queue` - An event queue, created as `queue.LifoQueue()` 
* `csv_dir` - directory where csv files are kept
* `symbol_list` - List of symbols as stock universe. Ensure that symbols have the same name as the CSV files in `csv_dir`
* `start_date` - YYYY-MM-DD. Has to be a trading day else `KeyError` will be returned. 
* `end_date` (OPTIONAL) - YYYY-MM-DD. Has to be a trading day else `KeyError` will be returned. 


NOTE: Add the above to a github wiki page when more information is available

