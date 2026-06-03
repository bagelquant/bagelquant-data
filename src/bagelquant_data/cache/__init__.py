"""Cache interfaces."""

from bagelquant_data.cache.distributed import DistributedCache
from bagelquant_data.cache.memory import MemoryCache
from bagelquant_data.cache.policy import CachePolicy

__all__ = ["CachePolicy", "DistributedCache", "MemoryCache"]
