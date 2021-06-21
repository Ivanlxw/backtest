import time
import queue
import logging
from trading_common.plots.plot import Plot, PlotTradePrices


def _backtest_loop(bars, event_queue, order_queue, strategy, port, broker, loop_live: bool = False) -> Plot:
    start = time.time()
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        if bars.continue_backtest == True:
            bars.update_bars()
        else:
            while not event_queue.empty():
                event_queue.get()
            break
        while True:
            try:
                event = event_queue.get(block=False)
            except queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'MARKET':
                        port.update_timeindex(event)
                        signal_list = strategy.calculate_signals(event)
                        for signal in signal_list:
                            if signal is not None:
                                event_queue.put(signal)
                        while not order_queue.empty():
                            event_queue.put(order_queue.get())

                    elif event.type == 'SIGNAL':
                        port.update_signal(event)

                    elif event.type == 'ORDER':
                        if broker.execute_order(event):
                            logging.info(event.print_order())

                    elif event.type == 'FILL':
                        port.update_fill(event)

    print(f"Backtest finished in {time.time() - start}. Getting summary stats")
    port.create_equity_curve_df()
    logging.log(32, port.output_summary_stats())

    plotter = Plot(port)
    plotter.plot()
    return plotter


def _life_loop(bars, event_queue, order_queue, strategy, port, broker) -> Plot:
    while True:
        # Update the bars (specific backtest code, as opposed to live trading)
        now = pd.Timestamp.now(tz=NY)
        if now.hour == 10:  # and now.minute >= 35:
            # Update the bars (specific backtest code, as opposed to live trading)
            if now.dayofweek > 4:
                logging.info("saving info")
                port.create_equity_curve_df()
                results_dir = os.path.join(backtest_basepath, "results")
                if not os.path.exists(results_dir):
                    os.mkdir(results_dir)

                port.equity_curve.to_csv(os.path.join(
                    results_dir, f"{port.name}.json"))
                break

            bars.update_bars()
            while True:
                try:
                    event = event_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            logging.info(f"{now}: MarketEvent")
                            port.update_timeindex(event)
                            if (signal_list := strategy.calculate_signals(event)) is not None:
                                for signal in signal_list:
                                    event_queue.put(signal)
                            while not order_queue.empty():
                                event_queue.put(order_queue.get())

                        elif event.type == 'SIGNAL':
                            port.update_signal(event)

                        elif event.type == 'ORDER':
                            if broker.execute_order(event):
                                logging.info(event.print_order())

                        elif event.type == 'FILL':
                            port.update_fill(event)

            # write the day's portfolio status before sleeping
            logging.info(f"{pd.Timestamp.now(tz=NY)}: sleeping")
            time.sleep(2 * 60 * 60)  # 12 hrs
