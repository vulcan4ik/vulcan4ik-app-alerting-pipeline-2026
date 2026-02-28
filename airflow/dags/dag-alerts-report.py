# alerts_feed_and_messages_final.py
import os
import io
from datetime import datetime, timedelta
import traceback

import matplotlib.pyplot as plt
import pandas as pd
import pandahouse as ph
import seaborn as sns
import telegram

from airflow.decorators import dag, task

# переменные и функции для работы с БД

def _env(name: str, required: bool = True, default: str | None = None) -> str:
    """
    Определяем функцию _env.

    name — имя переменной окружения (например, "TELEGRAM_TOKEN").

    required=True — по умолчанию считаем переменную обязательной.

    default — значение “на случай, если переменная не найдена”.

    """
    val = os.environ.get(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise ValueError(f"Missing env var: {name}")
    return val

connection = {
    "host": _env("CLICKHOUSE_HOST"),
    "database": _env("CLICKHOUSE_DB"),
    "user": _env("CLICKHOUSE_USER"),
    "password": _env("CLICKHOUSE_PASSWORD"),
}

TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN")
CHAT_ID = int(_env("TELEGRAM_CHAT_ID"))
SUPERSET_CHART_URL = "https://superset.lab.karpov.courses/superset/dashboard/7930/"

# Конфигурация метрик 
FEED_METRICS_CONFIG = {
    'users_feed': '👥 Активные пользователи ленты',
    'views': '👀 Просмотры',
    'likes': '❤️ Лайки',
    'ctr': '🎯 CTR'
}

MESSAGE_METRICS_CONFIG = {
    'users_msg': '💬 Активные пользователи мессенджера',
    'messages': '📩 Отправленные сообщения',
    'users_received': '👥 Получатели сообщений'
}


def select(sql) -> pd.DataFrame:
    """
    выполняет sql запрос к clickhouse и возвращает результат как dataframe
    """
    return ph.read_clickhouse(sql, connection=connection)


def check_anomaly_iqr(df: pd.DataFrame, metric: str, a: float = 3.5, n: int = 6):
    """
    1. a=3.5 
    2. n=6 
    3. Защита от нулевого IQR 
    4. min_periods для устойчивости
    """
    df = df.copy()
    
    # Улучшенный расчет квантилей
    df["q25"] = df[metric].shift(1).rolling(n, min_periods=2).quantile(0.25)
    df["q75"] = df[metric].shift(1).rolling(n, min_periods=2).quantile(0.75)
    df["iqr"] = df["q75"] - df["q25"]
    
    #  защита от нулевого IQR
    if df["iqr"].median() > 0:
        df["iqr"] = df["iqr"].replace(0, df["iqr"].median())
    else:
        df["iqr"] = df["iqr"].replace(0, 1)
    
    df["up"] = df["q75"] + a * df["iqr"]
    df["low"] = df["q25"] - a * df["iqr"]
    
    # Сглаживание границ 
    df["up"] = df["up"].rolling(3, center=True, min_periods=1).mean()
    df["low"] = df["low"].rolling(3, center=True, min_periods=1).mean()
    
    x = df[metric].iloc[-1]
    low = df["low"].iloc[-1]
    up = df["up"].iloc[-1]
    
    # проверка
    if pd.isna(low) or pd.isna(up):
        return 0, df
    
    is_alert = 1 if (x < low or x > up) else 0
    return is_alert, df


def check_anomaly_day_ago(df: pd.DataFrame, metric: str, threshold: float = 0.35):
    """
    Сравнение со значением сутки назад
    """
    if len(df) < 97:  # Нужно хотя бы сутки данных + текущая точка
        return 0, 0.0, "недостаточно данных"
    
    current_val = df[metric].iloc[-1]
    current_time = df['hm'].iloc[-1]
    
    # Ищем такое же время вчера
    day_ago_data = df[df['hm'] == current_time]
    if len(day_ago_data) < 2:
        return 0, 0.0, "нет данных за вчера"
    
    day_ago_val = day_ago_data[metric].iloc[-2]
    
    if day_ago_val == 0:
        return 0, 0.0, "нулевое значение вчера"
    
    deviation = abs(current_val - day_ago_val) / day_ago_val
    is_alert = 1 if deviation > threshold else 0
    direction = "вниз" if current_val < day_ago_val else "вверх"
    
    return is_alert, deviation, direction


def format_deviation_iqr(x: float, low: float, up: float) -> tuple:
    """
    отклонение относительно нарушенной границы коридора + направление
    """
    if pd.isna(low) or pd.isna(up) or pd.isna(x):
        return float("nan"), "неизвестно"
    
    if x < low and low != 0:
        return abs(1 - x / low), "вниз"
    if x > up and up != 0:
        return abs(1 - x / up), "вверх"
    
    return 0.0, "норма"


def plot_metric(df: pd.DataFrame, metric: str, metric_name: str, source: str):
    """
    Построение графика
    """
    sns.set_style("darkgrid")
    plt.figure(figsize=(14, 8))
    
    # Основной график с маркерами
    plt.plot(df["ts"], df[metric], label=metric_name, 
             linewidth=2.5, color='#3498db', marker='o', markersize=4)
    
    #  Заливка нормального диапазона
    if 'low' in df.columns and 'up' in df.columns:
        plt.fill_between(df["ts"], df["low"], df["up"], 
                        alpha=0.2, color='gray', label='Нормальный диапазон')
        plt.plot(df["ts"], df["up"], 'r--', linewidth=1.5, alpha=0.7)
        plt.plot(df["ts"], df["low"], 'r--', linewidth=1.5, alpha=0.7)
    
    # Аномальная точка с правильным синтаксисом
    last_point = df.iloc[-1]
    if ('up' in df.columns and 'low' in df.columns and
        (last_point[metric] > last_point["up"] or 
         last_point[metric] < last_point["low"])):
        plt.scatter([last_point["ts"]], [last_point[metric]], 
                   color='red', s=150, zorder=5, 
                   label=f'🚨 Аномалия: {last_point[metric]:.0f}')
    
    plt.title(f'{source}: {metric_name}', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Время', fontsize=12)
    plt.ylabel(metric_name, fontsize=12)
    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    plot_object = io.BytesIO()
    plt.savefig(plot_object, format="png", dpi=120, bbox_inches="tight")
    plot_object.seek(0)
    plot_object.name = f"{metric}.png"
    plt.close()

    return plot_object


def create_alert_message(metric_name: str, current_value: float, 
                        deviation: float, direction: str, 
                        low: float, up: float, source: str, 
                        timestamp: datetime, method: str = "IQR") -> str:
    """
    cообщение об алерте
    """
    superset_url = SUPERSET_CHART_URL
    # Emoji в зависимости от направления и величины отклонения
    if direction == "вверх":
        emoji = "🟡" if deviation < 0.5 else "🔴"
        recommendation = "Убедиться в корректности данных"
    else:
        emoji = "🔵" if deviation < 0.5 else "🔴"
        recommendation = "Проверить систему на сбои"
    
    # Форматирование чисел
    current_fmt = f"{current_value:,.0f}" if current_value >= 1000 else f"{current_value:.1f}"
    low_fmt = f"{low:,.0f}" if low >= 1000 else f"{low:.1f}"
    up_fmt = f"{up:,.0f}" if up >= 1000 else f"{up:.1f}"
    
    # Форматирование сообщения
    message = f"""🚨 *АЛЕРТ: {metric_name}*
📍 *Срез:* {source}
🕐 *Время:* {timestamp.strftime('%d.%m.%Y %H:%M')}
📊 *Метод детектирования:* {method}

📈 *Текущее значение:* {current_fmt}
📉 *Отклонение {direction}:* {deviation:.1%}
🎯 *Диапазон нормы:* [{low_fmt} — {up_fmt}]
    *Графики метрик:* {superset_url}
    *Рекомендация:* {recommendation}

"""
    
    return message


default_args = {
    "owner": "aleksej-harchenko-wpl4644",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="alerts_feed_and_messages_kharchenko",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 20),
    catchup=False,
    default_args=default_args,
    tags=["alerts", "telegram", "monitoring"]
)
def alerts_feed_and_messages_kharchenko():
    
    @task
    def run_alerts():
        chat_id = CHAT_ID
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        
        try:
           
            # FEED
            query_feed = """
                select
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(ts) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_feed
                    , countIf(action = 'view') as views
                    , countIf(action = 'like') as likes
                from simulator_20251220.feed_actions
                where time >= today() - 7  # ← 7 ДНЕЙ для стабильного IQR
                  and time < toStartOfFifteenMinutes(now())
                group by ts, date, hm
                order by ts
            """
            feed = select(query_feed)
            
            # Расчет CTR с защитой от деления на ноль
            if 'views' in feed.columns and 'likes' in feed.columns:
                feed["ctr"] = 0.0
                mask = feed["views"] > 0
                feed.loc[mask, "ctr"] = feed.loc[mask, "likes"] / feed.loc[mask, "views"]
            
            # Проверка всех метрик ленты
            for metric_key, metric_name in FEED_METRICS_CONFIG.items():
                if metric_key not in feed.columns or len(feed) < 10:
                    continue
                
                # Пропускаем CTR если нет просмотров
                if metric_key == "ctr" and feed["views"].iloc[-1] == 0:
                    continue
                
                df = feed[["ts", "date", "hm", metric_key]].copy()
                
                # 1. Основной метод: IQR
                is_alert_iqr, df_iqr = check_anomaly_iqr(df, metric_key, a=3.5, n=6)
                
                # 2. Дополнительная проверка: сравнение с днем назад
                is_alert_day, day_deviation, day_direction = check_anomaly_day_ago(
                    df, metric_key, threshold=0.35
                )
                
                # Логика алерта: IQR ИЛИ (IQR и день назад)
                if is_alert_iqr == 1:
                    x = df_iqr[metric_key].iloc[-1]
                    low = df_iqr["low"].iloc[-1]
                    up = df_iqr["up"].iloc[-1]
                    dev, direction = format_deviation_iqr(x, low, up)
                    
                    # Создаем  сообщение
                    msg = create_alert_message(
                        metric_name=metric_name,
                        current_value=x,
                        deviation=dev,
                        direction=direction,
                        low=low,
                        up=up,
                        source="Лента новостей",
                        timestamp=df["ts"].iloc[-1],
                        method="IQR" + (" + Day-over-Day" if is_alert_day == 1 else "")
                    )
                    
                    # График с улучшенным дизайном
                    plot_object = plot_metric(
                        df_iqr.tail(96),  # Последние 24 часа
                        metric_key, 
                        metric_name, 
                        "Лента"
                    )
                    
                    # Отправляем в Telegram
                    bot.sendMessage(
                        chat_id=chat_id, 
                        text=msg, 
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    )
                    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
            # МЕССЕНДЖЕР 
            query_msg = """
                select
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(ts) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_msg
                    , count() as messages
                    , uniqExact(receiver_id) as users_received
                from simulator_20251220.message_actions
                where time >= today() - 7  # ← 7 ДНЕЙ для стабильного IQR
                  and time < toStartOfFifteenMinutes(now())
                group by ts, date, hm
                order by ts
            """
            msg_df = select(query_msg)
            
            # Проверка всех метрик мессенджера
            for metric_key, metric_name in MESSAGE_METRICS_CONFIG.items():
                if metric_key not in msg_df.columns or len(msg_df) < 10:
                    continue
                
                df = msg_df[["ts", "date", "hm", metric_key]].copy()
                
                is_alert_iqr, df_iqr = check_anomaly_iqr(df, metric_key, a=3.5, n=6)
                
                if is_alert_iqr == 1:
                    x = df_iqr[metric_key].iloc[-1]
                    low = df_iqr["low"].iloc[-1]
                    up = df_iqr["up"].iloc[-1]
                    dev, direction = format_deviation_iqr(x, low, up)
                    
                    msg = create_alert_message(
                        metric_name=metric_name,
                        current_value=x,
                        deviation=dev,
                        direction=direction,
                        low=low,
                        up=up,
                        source="Мессенджер",
                        timestamp=df["ts"].iloc[-1],
                        method="IQR"
                    )
                    
                    plot_object = plot_metric(
                        df_iqr.tail(96),
                        metric_key, 
                        metric_name, 
                        "Мессенджер"
                    )
                    
                    bot.sendMessage(
                        chat_id=chat_id, 
                        text=msg, 
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    )
                    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
            print("✅ Проверка завершена успешно")
            
        except Exception as e:
            #  Обработка ошибок с отправкой в Telegram
            error_msg = (
                f"❌ *КРИТИЧЕСКАЯ ОШИБКА в алерт-системе*\n\n"
                f"Ошибка: {str(e)[:200]}\n\n"
                f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            print(f"Ошибка: {e}")
            print(traceback.format_exc())
            
            try:
                bot.sendMessage(
                    chat_id=chat_id,
                    text=error_msg,
                    parse_mode="Markdown"
                )
            except Exception as telegram_error:
                print(f"Не удалось отправить ошибку в Telegram: {telegram_error}")
    
    run_alerts()


alerts_feed_and_messages_dag = alerts_feed_and_messages_kharchenko()