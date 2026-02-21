import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

def fmt_int(x):
    """Форматирование целых чисел с пробелами"""
    try:
        return f"{int(x):,}".replace(",", " ")
    except Exception:
        return "0"


def make_app_report_and_charts(data_dir: str, out_dir: str):
    """
    Демо отчет работающий только от CSV (без ClickHouse/Airflow/Telegram).

    Читает из data/:
      - app_dau_yesterday.csv (1 строка)
      - app_dau_14d.csv (14 дней * 2 продукта)
      - app_segments_14d.csv (сегменты пересечения)
      - app_messages_details_yesterday.csv (1 строка)
      - app_weekly_avg.csv (1 строка; здесь просто читаем, чтобы показать, что файл есть)

    Пишет в output/:
      - app_report.md
      - app_charts.png
    """
    os.makedirs(out_dir, exist_ok=True)

    dau_y = pd.read_csv(os.path.join(data_dir, "app_dau_yesterday.csv"), parse_dates=["date"])
    dau_14 = pd.read_csv(os.path.join(data_dir, "app_dau_14d.csv"), parse_dates=["date"])
    seg = pd.read_csv(os.path.join(data_dir, "app_segments_14d.csv"), parse_dates=["__timestamp"])
    msg_det = pd.read_csv(os.path.join(data_dir, "app_messages_details_yesterday.csv"))
    weekly = pd.read_csv(os.path.join(data_dir, "app_weekly_avg.csv"))

    # Берём "вчера" как базовую дату отчёта
    row = dau_y.iloc[0]
    report_date = pd.to_datetime(row["date"]).date()

    # Сегменты: приводим timestamp к дню и берём сегменты за report_date (или последний доступный день)
    seg2 = seg.copy()
    seg2["day"] = seg2["__timestamp"].dt.date

    seg_day = seg2[seg2["day"] == report_date].copy()
    if seg_day.empty and not seg2.empty:
        seg_day = seg2[seg2["day"] == seg2["day"].max()].copy()

    seg_dict = {r["type_user"]: r["SUM(user_count)"] for _, r in seg_day.iterrows()}
    both_users = int(seg_dict.get("Лента и сообщения", 0))
    only_feed = int(seg_dict.get("Только лента", 0))
    only_msg = int(seg_dict.get("Только сообщения", 0))

    total_seg = both_users + only_feed + only_msg
    overlap = (both_users / total_seg * 100) if total_seg else 0.0

    # Метрики мессенджера за вчера
    md_row = msg_det.iloc[0] if not msg_det.empty else {}
    total_messages = md_row.get("total_messages", 0)
    unique_conversations = md_row.get("unique_conversations", 0)

  
    report_text = (
        f"📅 Дата: {report_date.strftime('%d.%m.%Y')}\n\n"
        f"👥 Общий DAU приложения: {fmt_int(row['app_dau_total'])}\n"
        f"📱 DAU Ленты: {fmt_int(row['feed_dau'])}\n"
        f"  👁️ Просмотры: {fmt_int(row['feed_views'])}\n"
        f"  ❤️ Лайки: {fmt_int(row['feed_likes'])}\n"
        f"  📊 CTR: {float(row['ctr']):.2%}\n"
        f"💬 DAU Мессенджера: {fmt_int(row['msg_dau'])}\n"
        f"📨 Сообщений за вчера: {fmt_int(total_messages)}\n"
        f"💬 Уникальных диалогов: {fmt_int(unique_conversations)}\n\n"
        f"🎯 Сегменты (за день):\n"
        f"  🔄 Оба сервиса: {fmt_int(both_users)}\n"
        f"  📱 Только Лента: {fmt_int(only_feed)}\n"
        f"  💬 Только Мессенджер: {fmt_int(only_msg)}\n"
        f"  ➡️ Пересечение: {overlap:.1f}%\n"
    )


    md_path = os.path.join(out_dir, "app_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# App report demo\n\n")
        f.write("```text\n")
        f.write(report_text)
        f.write("```\n")

    # Графики: DAU (feed vs messenger), CTR feed, сегменты 
    dau_14 = dau_14.copy()
    feed = dau_14[dau_14["product"] == "feed"].sort_values("date")
    msg = dau_14[dau_14["product"] == "messenger"].sort_values("date")

    sns.set_style("whitegrid")  # ✅ ИЛИ это
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.1])

    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(feed["date"], feed["dau"], label="Лента", linewidth=2.3)
    ax1.plot(msg["date"], msg["dau"], label="Мессенджер", linewidth=2.3)
    ax1.set_title("DAU (14 дней)")
    ax1.legend()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    ax1.tick_params(axis="x", rotation=25)

    ax2 = fig.add_subplot(gs[0, 1])
    if "ctr" in feed.columns and not feed.empty:
        ax2.plot(feed["date"], feed["ctr"] * 100, linewidth=2.3, color="green")
    ax2.set_title("CTR Ленты (%) (14 дней)")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    ax2.tick_params(axis="x", rotation=25)

    ax3 = fig.add_subplot(gs[1, :])
    pivot = seg2.pivot_table(index="day", columns="type_user", values="SUM(user_count)", aggfunc="sum").fillna(0)
    pivot = pivot.sort_index()
    cols = [c for c in ["Только сообщения", "Только лента", "Лента и сообщения"] if c in pivot.columns]
    pivot = pivot[cols]
    pivot.plot(kind="bar", stacked=True, ax=ax3, width=0.85)
    ax3.legend(
    title='Сегмент',
    loc='center left',
    bbox_to_anchor=(1.05, 0.5),     
    frameon=False
)

    ax3.set_title("Структура аудитории (14 дней)")  
    ax3.set_xlabel("")
    ax3.tick_params(axis="x", rotation=25)


    fig.tight_layout(rect=[0, 0, 0.85, 0.965])
    png_path = os.path.join(out_dir, "app_charts.png")
    fig.savefig(png_path, dpi=160, bbox_inches="tight")
    plt.close(fig)

    return {"app_report_md": md_path, "app_charts_png": png_path}
