# Simple Backtesting 

A simple backtester to test trading algorithms and portfolio optimization strategies.

## Installation
- To be updated with requirements.txt

## Usage
`python loop.py`

`loop.py` - runs the backtester. 

This repo is meant to be as low-level as possible to get greater control of the backtesting environment. Edit the various scripts explained below and import them to `loop.py` to test your strategies. 


## Wiki
This event-driven backtester consists of the following: 
- DataHandler (Defines trading universe, reads in necessary data)
- Execution Enironment (Details about the exchange that might affect backtesting strategies)
- Portfolio (Executes `OrderEvent` based on `SignalEvent`, updates portfolio as necessary)
- Strategy (Looks for `SignalEvent` to be routed to portfolio)


NOTE: Add the above  to a github wiki page when more information is available

