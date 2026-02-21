import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def check_anomaly_iqr(df: pd.DataFrame, metric: str, a: float = 3.5, n: int = 6):
    """
    Детект аномалий методом IQR (межквартильный размах), как в alerts DAG:
    - считаем q25/q75 по rolling окну на предыдущих точках (shift(1))
    - защищаемся от нулевого IQR
    - строим верхнюю/нижнюю границу и слегка сглаживаем
    Возвращает: (is_alert: 0/1, df )
    """
    df = df.copy()

    df["q25"] = df[metric].shift(1).rolling(n, min_periods=2).quantile(0.25)
    df["q75"] = df[metric].shift(1).rolling(n, min_periods=2).quantile(0.75)
    df["iqr"] = df["q75"] - df["q25"]
    
    # Если IQR нулевой (все значения одинаковые) — подставляем медианный IQR или 1

    if df["iqr"].median() > 0:
        df["iqr"] = df["iqr"].replace(0, df["iqr"].median())
    else:
        df["iqr"] = df["iqr"].replace(0, 1)

    df["up"] = df["q75"] + a * df["iqr"]
    df["low"] = df["q25"] - a * df["iqr"]

    df["up"] = df["up"].rolling(3, center=True, min_periods=1).mean()
    df["low"] = df["low"].rolling(3, center=True, min_periods=1).mean()

    low = df["low"].iloc[-1]
    up = df["up"].iloc[-1]
    x = df[metric].iloc[-1]
    
    # Если границы не рассчитались (NaN) — алерт не поднимаем
    if pd.isna(low) or pd.isna(up):
        return 0, df

    is_alert = 1 if (x < low or x > up) else 0
    return is_alert, df


def format_deviation_iqr(x: float, low: float, up: float):
    """
    Рассчитываем отклонение относительно нарушенной границы и направление.
    Возвращает: (deviation, direction)
    """
    if pd.isna(low) or pd.isna(up) or pd.isna(x):
        return float("nan"), "неизвестно"

    if x < low and low != 0:
        return abs(1 - x / low), "вниз"
    if x > up and up != 0:
        return abs(1 - x / up), "вверх"
    return 0.0, "норма"


def plot_metric_png(df: pd.DataFrame, metric: str, metric_name: str, source: str, out_path: str):
    """
    Строит график метрики + (если есть) коридор low/up.
    Сохраняет .png
    """
    sns.set_style("darkgrid")
    plt.figure(figsize=(14, 7))

    plt.plot(
        df["ts"], df[metric],
        label=metric_name,
        linewidth=2.5,
        color="#3498db",
        marker="o",
        markersize=3
    )

    if "low" in df.columns and "up" in df.columns:
        plt.fill_between(df["ts"], df["low"], df["up"], alpha=0.2, color="gray", label="Нормальный диапазон")
        plt.plot(df["ts"], df["up"], "r--", linewidth=1.2, alpha=0.85)
        plt.plot(df["ts"], df["low"], "r--", linewidth=1.2, alpha=0.85)

        last = df.iloc[-1]
        if (last[metric] > last["up"]) or (last[metric] < last["low"]):
            plt.scatter([last["ts"]], [last[metric]], color="red", s=120, zorder=5, label="🚨 Аномалия")

    plt.title(f"{source}: {metric_name}", fontsize=14, fontweight="bold", pad=12)
    plt.xlabel("Время")
    plt.ylabel(metric_name)
    plt.legend(loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close()


def create_alert_message(metric_name: str, current_value: float, deviation: float, direction: str,
                         low: float, up: float, source: str, timestamp: pd.Timestamp, method: str = "IQR") -> str:
    """
    текст алерта для демо- срабатывания (без Telegram).
    """
    def fmt(x):
        try:
            x = float(x)
        except Exception:
            return "nan"
        if abs(x) >= 1000:
            return f"{x:,.0f}".replace(",", " ")
        return f"{x:.4f}"

    return (
        f"🚨 АЛЕРТ: {metric_name}\n"      # ✅ Русский
        f"📍 Источник: {source}\n"        # ✅ Русский  
        f"🕐 Время: {pd.to_datetime(timestamp).strftime('%d.%m.%Y %H:%M')}\n"
        f"📊 Метод: {method}\n"
        f"📈 Текущее: {fmt(current_value)}\n"
        f"📉 Отклонение {direction}: {deviation:.1%}\n"
        f"🎯 Норма: [{fmt(low)} — {fmt(up)}]\n"
    )



def run_alerts_demo(feed_15m: pd.DataFrame, msg_15m: pd.DataFrame, out_dir: str):
    """
    Главная функция демо-алертов.
    Вход:
      - feed_15m.csv: ts,date,hm,users_feed,views,likes
      - msg_15m.csv:  ts,date,hm,users_msg,messages,users_received
    Выход (в output/):
      - alert_feed_ctr.md + alert_feed_ctr.png
      - alert_msg_messages.md + alert_msg_messages.png
    """
    os.makedirs(out_dir, exist_ok=True)

    feed = feed_15m.copy()
    msg = msg_15m.copy()

    # вычисление CTR по аналогии с DAG - логикой
    feed["ctr"] = 0.0
    m = feed["views"] > 0
    feed.loc[m, "ctr"] = feed.loc[m, "likes"] / feed.loc[m, "views"]

    feed = feed.sort_values("ts").reset_index(drop=True)
    msg = msg.sort_values("ts").reset_index(drop=True)
    
    # - CTR ленты (часто показывает аномалии/скачки)
    # - messages мессенджера 
    targets = [
        ("feed", feed, "ctr", "CTR", "Лента новостей"),
        ("msg", msg, "messages", "Сообщения", "Мессенджер"),
    ]

    results = []
    for key, df, metric, metric_name, source in targets:
        need_cols = ["ts", "hm", metric]
        missing = [c for c in need_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in {key} dataframe: {missing}")

        d = df[need_cols].copy()
        d = d.sort_values("ts").reset_index(drop=True)
        # Считаем коридор и проверяем последнюю точку
        is_alert, d_iqr = check_anomaly_iqr(d, metric, a=3.5, n=6)

        x = float(d_iqr[metric].iloc[-1])
        low = float(d_iqr["low"].iloc[-1])
        up = float(d_iqr["up"].iloc[-1])
        dev, direction = format_deviation_iqr(x, low, up)

        msg_text = create_alert_message(
            metric_name=metric_name,
            current_value=x,
            deviation=dev,
            direction=direction,
            low=low,
            up=up,
            source=source,
            timestamp=d_iqr["ts"].iloc[-1],
            method="IQR"
        )

        # График последних 24 часов (96 точек по 15 минут)
        plot_path = os.path.join(out_dir, f"alert_{key}_{metric}.png")
        plot_metric_png(d_iqr.tail(96), metric, metric_name, source, plot_path)

        # Сохраянем MD
        md_path = os.path.join(out_dir, f"alert_{key}_{metric}.md")
        status = "**🚨 АЛЕРТ СРАБОТАЛ**" if is_alert == 1 else "**✅ Норма** (последняя точка в коридоре)"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("# Демо алертов\n\n")           # ✅ Русский
            f.write(f"{status}\n\n")
            f.write("## Сообщение\n\n")             # ✅ Русский
            f.write("```text\n")
            f.write(msg_text)
            f.write("```\n\n")
            f.write("## График\n\n")                # ✅ Русский
            f.write(f"PNG: `{os.path.basename(plot_path)}`\n")


        results.append((key, metric, is_alert, plot_path, md_path))

    return results
