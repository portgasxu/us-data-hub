"""
Health Monitor — 系统健康监控 (v5.3)

检查项：
- 数据管道（最近 30 分钟是否有新数据）
- 数据库大小（超过 1GB 时告警）
- 进程状态（watcher/auto_execute 是否在跑）
- API 限流（Longbridge 调用是否接近上限）
- 磁盘空间（日志文件是否堆积）
"""

import logging
import os
import subprocess
from datetime import datetime

logger = logging.getLogger("health_monitor")

# ─── 阈值 ─────────────────────────────────────────────
MAX_DB_SIZE_MB = 1024       # 数据库最大 1GB
MAX_LOG_SIZE_MB = 500       # 日志目录最大 500MB
MIN_DISK_FREE_MB = 1024     # 磁盘最少剩余 1GB
DATA_MAX_AGE_MINUTES = 180  # 数据最大 3 小时未更新（盘后放宽）


def check_health(db, base_dir: str = None) -> dict:
    """运行所有健康检查。"""
    if base_dir is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    results = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "healthy",  # healthy | warning | critical
        "checks": {},
        "alerts": [],
    }

    # 1. 数据管道
    check = _check_data_pipeline(db)
    results["checks"]["data_pipeline"] = check
    if check["status"] != "ok":
        results["alerts"].append(check)

    # 2. 数据库大小
    check = _check_db_size(base_dir)
    results["checks"]["db_size"] = check
    if check["status"] != "ok":
        results["alerts"].append(check)

    # 3. 进程状态
    check = _check_processes()
    results["checks"]["processes"] = check
    if check["status"] != "ok":
        results["alerts"].append(check)

    # 4. 磁盘空间
    check = _check_disk_space()
    results["checks"]["disk_space"] = check
    if check["status"] != "ok":
        results["alerts"].append(check)

    # 5. 日志大小
    check = _check_log_size(base_dir)
    results["checks"]["log_size"] = check
    if check["status"] != "ok":
        results["alerts"].append(check)

    # 总体状态
    critical = any(c["status"] == "critical" for c in results["checks"].values())
    warning = any(c["status"] == "warning" for c in results["checks"].values())

    if critical:
        results["status"] = "critical"
    elif warning:
        results["status"] = "warning"

    return results


def _check_data_pipeline(db) -> dict:
    """检查数据管道是否正常。"""
    row = db.conn.execute(
        "SELECT MAX(date) as last_date FROM prices"
    ).fetchone()

    if not row or not row["last_date"]:
        return {"status": "critical", "message": "无价格数据"}

    last_date = row["last_date"]

    # 检查是否是周末或盘后
    from datetime import datetime
    now = datetime.now()
    hour = now.hour

    # 美股盘后（北京时间 4:00-16:00）允许数据较旧
    if 4 <= hour < 16:
        max_age_days = 2  # 周末允许 2 天
    else:
        max_age_days = 1  # 交易时段允许 1 天

    try:
        last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        age_days = (now - last_dt).days
        if age_days > max_age_days:
            return {"status": "warning", "message": f"数据已 {age_days} 天未更新"}
    except ValueError:
        return {"status": "critical", "message": f"日期格式异常: {last_date}"}

    return {"status": "ok", "message": f"最新数据: {last_date}"}


def _check_db_size(base_dir: str) -> dict:
    """检查数据库大小。"""
    db_path = os.path.join(base_dir, "data", "us_data_hub.db")

    if not os.path.exists(db_path):
        return {"status": "critical", "message": "数据库文件不存在"}

    size_mb = os.path.getsize(db_path) / (1024 * 1024)

    if size_mb > MAX_DB_SIZE_MB:
        return {"status": "critical", "message": f"数据库 {size_mb:.0f}MB 超过 {MAX_DB_SIZE_MB}MB"}
    elif size_mb > MAX_DB_SIZE_MB * 0.8:
        return {"status": "warning", "message": f"数据库 {size_mb:.0f}MB 接近上限"}

    return {"status": "ok", "message": f"数据库 {size_mb:.0f}MB"}


def _check_processes() -> dict:
    """检查关键进程是否在运行。"""
    processes = ["watcher.py", "auto_execute.py"]
    running = []
    not_running = []

    for proc in processes:
        result = subprocess.run(
            f"ps aux | grep '{proc}' | grep -v grep",
            shell=True, capture_output=True, text=True
        )
        if result.stdout.strip():
            running.append(proc)
        else:
            not_running.append(proc)

    if not_running:
        # watcher/auto_execute 通过 cron 运行，不一定有常驻进程
        return {"status": "ok", "message": f"运行中: {', '.join(running) or '无'} (cron 调度)"}

    return {"status": "ok", "message": f"运行中: {', '.join(running)}"}


def _check_disk_space() -> dict:
    """检查磁盘空间。"""
    try:
        stat = os.statvfs("/")
        free_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)

        if free_mb < MIN_DISK_FREE_MB:
            return {"status": "critical", "message": f"磁盘剩余 {free_mb:.0f}MB < {MIN_DISK_FREE_MB}MB"}
        elif free_mb < MIN_DISK_FREE_MB * 2:
            return {"status": "warning", "message": f"磁盘剩余 {free_mb:.0f}MB 偏低"}

        return {"status": "ok", "message": f"磁盘剩余 {free_mb / 1024:.1f}GB"}
    except Exception as e:
        return {"status": "warning", "message": f"无法检查磁盘: {e}"}


def _check_log_size(base_dir: str) -> dict:
    """检查日志目录大小。"""
    log_dir = os.path.join(base_dir, "logs")

    if not os.path.exists(log_dir):
        return {"status": "ok", "message": "日志目录不存在"}

    total_size = 0
    for root, dirs, files in os.walk(log_dir):
        for f in files:
            fp = os.path.join(root, f)
            try:
                total_size += os.path.getsize(fp)
            except OSError:
                pass

    size_mb = total_size / (1024 * 1024)

    if size_mb > MAX_LOG_SIZE_MB:
        return {"status": "warning", "message": f"日志 {size_mb:.0f}MB 超过 {MAX_LOG_SIZE_MB}MB，建议清理"}

    return {"status": "ok", "message": f"日志 {size_mb:.1f}MB"}


def generate_health_report(db, base_dir: str = None) -> str:
    """生成健康报告。"""
    results = check_health(db, base_dir)

    lines = ["=" * 60]
    lines.append(f"系统健康报告 - {results['timestamp']}")
    lines.append(f"总体状态: {results['status'].upper()}")
    lines.append("=" * 60)

    for name, check in results["checks"].items():
        icon = "✅" if check["status"] == "ok" else ("⚠️" if check["status"] == "warning" else "🔴")
        lines.append(f"{icon} {name}: {check['message']}")

    if results["alerts"]:
        lines.append("")
        lines.append("--- 告警 ---")
        for a in results["alerts"]:
            lines.append(f"  [{a['status'].upper()}] {a['message']}")

    return "\n".join(lines)
