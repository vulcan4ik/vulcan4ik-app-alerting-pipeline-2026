import os
import pandas as pd

from src.alerts_demo import run_alerts_demo
from src.app_report_demo import make_app_report_and_charts


DATA_DIR = "data"
OUT_DIR = "output"


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1) Отчёт по приложению (app_report.md + app_charts.png)
    make_app_report_and_charts(DATA_DIR, OUT_DIR)

    # 2) Алерты (alert_*.md + alert_*.png)
    feed_15m = pd.read_csv(os.path.join(DATA_DIR, "feed_15m.csv"), parse_dates=["ts", "date"])
    msg_15m = pd.read_csv(os.path.join(DATA_DIR, "msg_15m.csv"), parse_dates=["ts", "date"])

    results = run_alerts_demo(feed_15m, msg_15m, OUT_DIR)

    print("OK, generated:")
    for key, metric, is_alert, plot_path, md_path in results:
        status = "TRIGGERED" if is_alert == 1 else "no alert"
        print(f"- alert {key}:{metric} -> {status}")
        print(f"  - {md_path}")
        print(f"  - {plot_path}")

    print("- output/app_report.md")
    print("- output/app_charts.png")


if __name__ == "__main__":
    main()
