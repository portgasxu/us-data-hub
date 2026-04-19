"""
dayup_logger.py — Dayup 复盘日志系统

统一日志模块，将系统运行时的日志自动路由到 dayup/ 对应子目录。
所有日志带时间戳，按模块分类，便于后续复盘与系统优化。

用法:
    from dayup_logger import get_logger
    logger = get_logger('trades')      # 交易记录
    logger = get_logger('decisions')   # 决策记录
    logger = get_logger('market')      # 市场信号
    logger = get_logger('risk')        # 风控记录
    logger = get_logger('system')      # 系统性能
    logger = get_logger('events')      # 外部事件
    logger = get_logger('errors')      # 错误日志
    logger = get_logger('performance') # 绩效指标
    logger = get_logger('strategy')    # 策略变更

    # 便捷函数
    from dayup_logger import log_trade, log_decision, log_error, log_risk, log_market
    log_trade(symbol='AAPL', action='BUY', price=150.0, ...)
    log_decision(decision_type='参数调整', content='...', ...)
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from functools import wraps
from typing import Dict

# ── 基础配置 ──────────────────────────────────────────────

DAYUP_BASE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'dayup'
)

_DAYUP_DIRS = ['errors', 'decisions', 'trades', 'positions', 'market',
               'performance', 'strategy', 'risk', 'system', 'events', 'reviews']
for d in _DAYUP_DIRS:
    os.makedirs(os.path.join(DAYUP_BASE, d), exist_ok=True)

_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
_DATEFMT = '%Y-%m-%d %H:%M:%S'

_loggers = {}


def get_logger(module_name, log_file=None):
    """
    获取 logger，自动路由到 dayup 对应目录。

    Args:
        module_name: 模块名，对应 dayup 子目录
        log_file: 自定义文件名，默认 module_name.log

    Returns:
        logging.Logger
    """
    if module_name in _loggers:
        return _loggers[module_name]

    if log_file is None:
        log_file = module_name + '.log'

    log_dir = os.path.join(DAYUP_BASE, module_name)
    if not os.path.exists(log_dir):
        log_dir = os.path.join(DAYUP_BASE, 'system')

    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger('dayup.' + module_name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        _loggers[module_name] = logger
        return logger

    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    logger.addHandler(ch)

    _loggers[module_name] = logger
    return logger


def _fmt(**kw):
    """格式化: k1=v1 | k2=v2"""
    parts = []
    for k, v in kw.items():
        if isinstance(v, str) and '\n' in v:
            v = v.replace('\n', '\\n')
        parts.append(str(k) + '=' + str(v))
    return ' | '.join(parts)


def _ts():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ── 便捷函数 ──────────────────────────────────────────────

def log_trade(symbol, action, price, quantity=0, fee=0,
              pnl=None, signal='', note='', **kw):
    logger = get_logger('trades')
    logger.info('[TRADE] ' + _fmt(
        time=_ts(), symbol=symbol, action=action,
        price=price, quantity=quantity, fee=fee,
        pnl=pnl or 'N/A', signal=signal, note=note, **kw))


def log_decision(decision_type, content, basis='',
                 expected='', result='', **kw):
    logger = get_logger('decisions')
    logger.info('[DECISION] ' + _fmt(
        time=_ts(), type=decision_type, content=content,
        basis=basis, expected=expected, result=result, **kw))


def log_error(error_type, module, description,
              stack='', status='待处理', **kw):
    logger = get_logger('errors')
    logger.error('[ERROR] ' + _fmt(
        time=_ts(), type=error_type, module=module,
        description=description, stack=stack, status=status, **kw))


# Risk log dedup: same risk_type+trigger within N seconds is skipped
_risk_log_cache: Dict[str, datetime] = {}
RISK_LOG_DEDUP_SECONDS = 600  # 10 minutes


def log_risk(risk_type, trigger, current,
             threshold, action, result='', **kw):
    # Fix #16: Dedup — same risk_type+trigger within 10min is skipped
    cache_key = f"{risk_type}:{trigger}"
    now = datetime.now()
    if cache_key in _risk_log_cache:
        elapsed = (now - _risk_log_cache[cache_key]).total_seconds()
        if elapsed < RISK_LOG_DEDUP_SECONDS:
            return  # Skip duplicate
    _risk_log_cache[cache_key] = now

    logger = get_logger('risk')
    logger.warning('[RISK] ' + _fmt(
        time=_ts(), type=risk_type, trigger=trigger,
        current=current, threshold=threshold,
        action=action, result=result, **kw))


def log_market(indicator_type, symbol, value,
               signal='', strength='', **kw):
    logger = get_logger('market')
    logger.info('[MARKET] ' + _fmt(
        time=_ts(), type=indicator_type, symbol=symbol,
        value=value, signal=signal, strength=strength, **kw))


def log_performance(period, total_return='', annual_return='',
                    max_drawdown='', sharpe='', win_rate='',
                    trades=0, **kw):
    logger = get_logger('performance')
    logger.info('[PERF] ' + _fmt(
        time=_ts(), period=period, total_return=total_return,
        annual_return=annual_return, max_drawdown=max_drawdown,
        sharpe=sharpe, win_rate=win_rate, trades=trades, **kw))


def log_strategy(strategy_name, change_type, params='',
                 reason='', expected_impact='', **kw):
    logger = get_logger('strategy')
    logger.info('[STRATEGY] ' + _fmt(
        time=_ts(), name=strategy_name, change=change_type,
        params=params, reason=reason,
        expected=expected_impact, **kw))


def log_system(metric_type, value, status='正常', **kw):
    logger = get_logger('system')
    logger.info('[SYSTEM] ' + _fmt(
        time=_ts(), type=metric_type, value=value,
        status=status, **kw))


def log_event(event_type, description, affected='',
              impact='中性', reaction='', **kw):
    logger = get_logger('events')
    logger.info('[EVENT] ' + _fmt(
        time=_ts(), type=event_type, description=description,
        affected=affected, impact=impact,
        reaction=reaction, **kw))


def log_position(symbol, quantity, cost_price,
                 current_price=None, holding_days=0,
                 weight=0, **kw):
    logger = get_logger('positions')
    fpnl = 'N/A'
    if current_price is not None:
        fpnl = '{:+.2f}'.format((current_price - cost_price) * quantity)
    logger.info('[POSITION] ' + _fmt(
        time=_ts(), symbol=symbol, quantity=quantity,
        cost_price=cost_price, current_price=current_price or 'N/A',
        floating_pnl=fpnl, weight='{:.1%}'.format(weight),
        days=holding_days, **kw))


# ── 自动错误捕获装饰器 ─────────────────────────────────────

def capture_errors(module_name='system'):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_error(
                    error_type=type(e).__name__,
                    module=module_name,
                    description=str(e),
                    stack=traceback.format_exc(),
                    status='待处理',
                    function=func.__name__)
                raise
        return wrapper
    return decorator


# ── 兼容旧版: 重定向根 logger 到 dayup ─────────────────────

def setup_root_logger(level=logging.INFO, log_file='system.log'):
    """
    配置根 logger，将未分类日志路由到 dayup/system/。
    应在程序入口调用，替代 logging.basicConfig。
    """
    root = logging.getLogger()
    root.setLevel(level)

    for h in root.handlers[:]:
        root.removeHandler(h)

    log_path = os.path.join(DAYUP_BASE, 'system', log_file)
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(ch)

    return root
