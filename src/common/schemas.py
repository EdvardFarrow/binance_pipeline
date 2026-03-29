import hashlib
from pydantic import BaseModel, Field, computed_field

class BinanceTrade(BaseModel):
    """
    Binance Raw Event Mapping:
    { "e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 12345, "p": "0.001", "q": "100" ... }
    """
    event_time_ms: int = Field(alias="E")
    symbol: str = Field(alias="s")
    trade_id: int = Field(alias="t")
    price: str = Field(alias="p")
    quantity: str = Field(alias="q")
    
    @computed_field
    @property
    def event_id(self) -> str:
        """
        Deterministic ID for exactly-once semantics.
        Even if we receive this trade twice, the event_id will be identical.
        ClickHouse will easily collapse duplicates for this key
        """
        unique_string = f"{self.symbol}_{self.trade_id}_{self.event_time_ms}"
        return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

    @computed_field
    @property
    def ingested_at(self) -> int:
        import time
        return int(time.time() * 1000)