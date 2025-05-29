
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

# -------------------------------
# **技术指标配置**
# -------------------------------
@dataclass
class TechnicalParams:
    """技术指标参数配置"""
    ma_periods: Dict[str, int]
    rsi_period: int
    bollinger_period: int
    bollinger_std: int
    volume_ma_period: int
    atr_period: int

    @classmethod
    def default(cls) -> 'TechnicalParams':
        """返回默认的技术指标参数"""
        return cls(
            ma_periods={'short': 5, 'medium': 20, 'long': 60},
            rsi_period=14,
            bollinger_period=20,
            bollinger_std=2,
            volume_ma_period=20,
            atr_period=14
        )
