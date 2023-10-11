from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.data import Bar


class PrintDataStrategy(Strategy):
    """simple strategy to print the bars and
    instrumentstatus objects that are received during
    startegy execution. This can be used for
    debugging and testing.
    """

    def __init__(self, instrument, bar_type):
        super().__init__()
        self.instrument = instrument
        self.bar_type = bar_type

    def on_start(self):
        self.log.info("subscribing to bars and instrument status updates")
        self.subscribe_bars(self.bar_type)
        self.subscribe_instrument_status(self.instrument.id)

    def on_bar(self, bar: Bar) -> None:
        self.log.info(repr(bar))

    def on_instrument_status(self, status):
        self.log.info(repr(status))
