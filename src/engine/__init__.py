"""FarmIA Engine Module: Spark processors for unified ingestion"""

from src.engine.processors import (
    UnifiedMemoryCoreProcessor,
    CircuitBreakerState,
    CircuitBreakerStatus,
)

__all__ = [
    "UnifiedMemoryCoreProcessor",
    "CircuitBreakerState",
    "CircuitBreakerStatus",
]
