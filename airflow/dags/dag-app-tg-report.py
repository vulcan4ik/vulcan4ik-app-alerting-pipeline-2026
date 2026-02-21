from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import numpy as np
import os
from tempfile import NamedTemporaryFile

from airflow.decorators import dag, task
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule



connection = {
    "host": _env("CLICKHOUSE_HOST"),
    "database": _env("CLICKHOUSE_DB"), 
    "user": _env("CLICKHOUSE_USER"),
    "password": _env("CLICKHOUSE_PASSWORD"),
}

TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN")
CHAT_ID = int(_env("TELEGRAM_CHAT_ID"))

def select(sql) -> pd.DataFrame:
    """
    Выполняет SQL запрос к ClickHouse и возвращает результат как DataFrame
 
    """
    return ph.read_clickhouse(sql, connection = connection)


# параметры DAG
default_args = {
    'owner': 'aleksej-harchenko-wpl4644',
    'depends_on_past': False, #запуск за текущий период не сможет выполниться, если запуск за предыдущий период был неуспешен = TRUE
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now() - timedelta(days=7)
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *' # Запуск в 11:00 каждый день
tags=["report", "app", "telegram", "test"]


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_app_report_kharchenko():
    # извлечение данных
    @task
    def extract_metrics():
        
        # таблица для вывода dau и ctr(для feed) за последний полный день 
        
        sql = """
                    WITH feed_stats AS (
            SELECT
                toDate(time) AS date,
                COUNT(DISTINCT user_id) AS feed_dau,
                COUNTIf(action = 'view') AS feed_views,
                COUNTIf(action = 'like') AS feed_likes,
                ROUND(COUNTIf(action = 'like') / NULLIF(COUNTIf(action = 'view'), 0), 4) AS ctr
            FROM simulator_20251220.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY date
        ),
        messenger_dau AS (
            SELECT
                toDate(time) AS date,
                COUNT(DISTINCT user_id) AS msg_dau
            FROM simulator_20251220.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY date
        ),
        app_dau_table AS (
            SELECT
                yesterday() AS date,
                COUNT(DISTINCT user_id) AS app_dau_total
            FROM (
                SELECT user_id 
                FROM simulator_20251220.feed_actions 
                WHERE toDate(time) = yesterday()

                UNION ALL

                SELECT user_id 
                FROM simulator_20251220.message_actions 
                WHERE toDate(time) = yesterday()
            )
        )
        SELECT
            COALESCE(f.date, m.date, a.date) AS date,
            COALESCE(f.feed_dau, 0) AS feed_dau,
            COALESCE(f.feed_views, 0) AS feed_views,
            COALESCE(f.feed_likes, 0) AS feed_likes,
            COALESCE(f.ctr, 0) AS ctr,
            COALESCE(m.msg_dau, 0) AS msg_dau,
            COALESCE(a.app_dau_total, 0) AS app_dau_total
        FROM feed_stats f
        FULL OUTER JOIN messenger_dau m ON f.date = m.date
        FULL OUTER JOIN app_dau_table a ON COALESCE(f.date, m.date) = a.date
        
            """
        dau_yesterday = select(sql)
        
         # таблица для вывода dau и ctr(для feed) за 30 дней 
        sql = """
 
            SELECT 
                toDate(time) AS date,
                'feed' AS product,
                COUNT(DISTINCT user_id) AS dau,
                COUNTIf(action = 'view') AS views,
                COUNTIf(action = 'like') AS likes,
                ROUND(COUNTIf(action = 'like') / NULLIF(COUNTIf(action = 'view'), 0), 4) AS ctr
            FROM simulator_20251220.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 30 AND yesterday()
            GROUP BY date

            UNION ALL

            SELECT 
                toDate(time) AS date,
                'messenger' AS product,
                COUNT(DISTINCT user_id) AS dau,
                0 AS views,
                0 AS likes,
                0 AS ctr
            FROM simulator_20251220.message_actions
            WHERE toDate(time) BETWEEN yesterday() - 30 AND yesterday()
            GROUP BY date
            ORDER BY date, product
            
            """
        
        
        dau_30d = select(sql)
        
        
        # Таблица для пересечения пользователей по сервисам за 30 дней ( для визуализации)
        
        sql = """
                           -- Основной запрос для подсчета пользователей по сегментам за последние 30 дней
                SELECT 
                toStartOfDay(toDateTime(time)) AS __timestamp,  -- Приводим время к началу дня
                type_user AS type_user,                         -- Тип пользователя (сегмент)
                sum(user_count) AS "SUM(user_count)" -- Суммируем количество пользователей
    
            FROM
              (
                -- создаем временную таблицу с ежедневной статистикой по типам пользователей
                WITH segments AS
                  (
                    SELECT 
                        time,
                        user,
                        type_user
                    FROM
                      (
                        -- Пользователи, которые были и в ленте, и в сообщениях В ОДИН И ТОТ ЖЕ ДЕНЬ
                        SELECT DISTINCT 
                            toDate(f.time) AS time,            -- Дата активности
                            f.user_id AS user,                  -- ID пользователя
                            'Лента и сообщения' AS type_user   -- Тип: оба действия
                        FROM 
                            simulator_20251220.feed_actions AS f
                        INNER JOIN 
                            simulator_20251220.message_actions AS m 
                            ON f.user_id = m.user_id           -- Связь по пользователю
                            AND toDate(f.time) = toDate(m.time) -- И по ДАТЕ (ключевое условие!)
                    
                        WHERE 
                            toDate(f.time) >= yesterday() - 30     -- Только данные за последние 30 дней

                        UNION ALL 

                        --  Пользователи, которые были ТОЛЬКО в ленте (без сообщений в этот день)
                        SELECT DISTINCT 
                            toDate(f.time) AS time,
                            f.user_id AS user,
                            'Только лента' AS type_user
                        FROM 
                            simulator_20251220.feed_actions AS f 
                        LEFT ANTI JOIN                           
                        
                            simulator_20251220.message_actions AS m 
                            ON f.user_id = m.user_id
                            AND toDate(f.time) = toDate(m.time)  -- В тот же день
                  
                        WHERE 
                            toDate(f.time) >= yesterday() - 30

                        UNION ALL 

                        -- Пользователи, которые были ТОЛЬКО в сообщениях (без ленты в этот день)
                        SELECT DISTINCT 
                            toDate(m.time) AS time,
                            m.user_id AS user,
                            'Только сообщения' AS type_user
                        FROM 
                            simulator_20251220.message_actions AS m 
                        LEFT ANTI JOIN                           -- Анти-джойн: исключаем тех, кто есть в ленте
                            simulator_20251220.feed_actions AS f 
                            ON m.user_id = f.user_id
                            AND toDate(m.time) = toDate(f.time)  -- В тот же день
                
                        WHERE 
                            toDate(m.time) >= yesterday() - 30
                      ) 
                  )

   
                SELECT 
                    time,
                    type_user,
                    COUNT(DISTINCT user) as user_count           -- Уникальные пользователи в день
                FROM 
                    segments
                GROUP BY 
                    time,
                    type_user
                ORDER BY 
                    time DESC                                    -- Сортируем по дате (новые первыми)
              ) AS virtual_table


            GROUP BY 
                type_user,
                toStartOfDay(toDateTime(time))                  -- Группировка по дню и сегменту

     
            ORDER BY 
                __timestamp DESC 
            """
        
        df_segments = select(sql)
        
        
        # метрики мессенджера
        sql = """
                SELECT
                    COUNT(*) as total_messages,
                    COUNT(DISTINCT user_id) as active_senders,
                    COUNT(DISTINCT receiver_id) as active_receivers,
                    COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0) as avg_messages_per_sender,
                    COUNT(DISTINCT 
                        CASE WHEN user_id != receiver_id 
                            THEN concat(least(user_id, receiver_id), '|', greatest(user_id, receiver_id))
                        END   -- УНИКАЛЬНЫЕ ДИАЛОГИ (пары пользователей)
                    ) as unique_conversations
                FROM simulator_20251220.message_actions
                WHERE toDate(time) = yesterday()
             """   
        messages_details = select(sql)
        
        # средние значения метрик за неделю (feed)
        sql = """
            SELECT
                AVG(dau)  AS avg_feed_dau_7d,
                AVG(likes) AS avg_likes_7d,
                AVG(views) AS avg_views_7d,
                AVG(ctr)  AS avg_ctr_7d
            FROM (
                SELECT
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS dau,
                    COUNTIf(action = 'like') AS likes,
                    COUNTIf(action = 'view') AS views,
                    ROUND(COUNTIf(action = 'like') / NULLIF(COUNTIf(action = 'view'), 0), 4) AS ctr
                FROM simulator_20251220.feed_actions
                WHERE toDate(time) BETWEEN yesterday() - 7 AND yesterday() - 1
                GROUP BY date
            )
        """
        weekly_avg_feed = select(sql)

        # среднее DAU мессенджера за неделю (messenger)
        sql = """
            SELECT
                AVG(dau) AS avg_msg_dau_7d
            FROM (
                SELECT
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS dau
                FROM simulator_20251220.message_actions
                WHERE toDate(time) BETWEEN yesterday() - 7 AND yesterday() - 1
                GROUP BY date
            )
        """
        weekly_avg_msg = select(sql)
        weekly_avg = pd.concat([weekly_avg_feed, weekly_avg_msg], axis=1)
        
        # retention пользователей 7го дня
        
        sql_feed_7d = """
                                    WITH user_starts AS (
                    SELECT
                        user_id,
                        min(toDate(time)) AS start_day
                    FROM simulator_20251220.feed_actions
                    GROUP BY user_id
                ),
                cohort AS (
                    SELECT user_id
                    FROM user_starts
                    WHERE start_day = yesterday() - 7
                ),
                day7_active AS (
                    SELECT DISTINCT user_id
                    FROM simulator_20251220.feed_actions
                    WHERE toDate(time) = yesterday()          -- это start_day + 7
                )
                SELECT
                    'feed' AS service,
                    yesterday() - 7 AS cohort_start_day,
                    countDistinct(c.user_id) AS cohort_size,
                    countDistinct(a.user_id) AS retained_d7,
                    round(retained_d7 * 100.0 / nullIf(cohort_size, 0), 2) AS retention_d7_pct
                FROM cohort c
                LEFT JOIN day7_active a USING (user_id);
                """
        feed_7d = select(sql_feed_7d)
        
        # retention пользователей message 7 дня
        sql_msg_7d = """
                            WITH events AS (
                SELECT user_id AS user_id, toDate(time) AS day
                FROM simulator_20251220.message_actions
                UNION ALL
                SELECT receiver_id AS user_id, toDate(time) AS day
                FROM simulator_20251220.message_actions
            ),
            user_starts AS (
                SELECT
                    user_id,
                    min(day) AS start_day
                FROM events
                GROUP BY user_id
            ),
            cohort AS (
                SELECT user_id
                FROM user_starts
                WHERE start_day = yesterday() - 7
            ),
            day7_active AS (
                SELECT DISTINCT user_id
                FROM events
                WHERE day = yesterday()
            )
            SELECT
                'messenger_any' AS service,
                yesterday() - 7 AS cohort_start_day,
                countDistinct(c.user_id) AS cohort_size,
                countDistinct(a.user_id) AS retained_d7,
                round(retained_d7 * 100.0 / nullIf(cohort_size, 0), 2) AS retention_d7_pct
            FROM cohort c
            LEFT JOIN day7_active a USING (user_id);
                    """
        msg_7d = select(sql_msg_7d)
        
        
        # средний retention 7го дня за последние 30 дней message
        
        slq_retention_avg_msg_7d = """
                    WITH events AS
                (
                    SELECT
                        user_id AS user_id,
                        toDate(time) AS day
                    FROM simulator_20251220.message_actions

                    UNION ALL

                    SELECT
                        receiver_id AS user_id,
                        toDate(time) AS day
                    FROM simulator_20251220.message_actions
                ),
                user_starts AS
                (
                    SELECT
                        user_id,
                        min(day) AS start_day
                    FROM events
                    GROUP BY user_id
                ),
                cohorts AS
                (
                    SELECT
                        start_day,
                        countDistinct(user_id) AS cohort_users
                    FROM user_starts
                    WHERE start_day BETWEEN yesterday() - 30 AND yesterday() - 7
                    GROUP BY start_day
                ),
                day7 AS
                (
                    SELECT
                        us.start_day,
                        countDistinct(e.user_id) AS users_d7
                    FROM user_starts us
                    INNER JOIN events e
                        ON e.user_id = us.user_id
                    WHERE us.start_day BETWEEN yesterday() - 30 AND yesterday() - 7
                      AND e.day = us.start_day + 7
                    GROUP BY us.start_day
                )
                SELECT
                    7 AS day_number,
                    round(avg(ifNull(users_d7, 0) * 100.0 / cohort_users), 2) AS avg_retention_pct,
                    count() AS cohorts_count
                FROM cohorts
                LEFT JOIN day7 USING (start_day);
                """
        
        retention_avg_msg_7d = select(slq_retention_avg_msg_7d)
        
        
        # средний retention 7го дня за последние 30 дней feed
        sql_retention_avg_feed_7d =     """
               WITH
            cohorts AS (
                SELECT
                    start_day,
                    COUNT(DISTINCT user_id) AS cohort_users
                FROM
                (
                    SELECT
                        user_id,
                        MIN(toDate(time)) AS start_day
                    FROM simulator_20251220.feed_actions
                    GROUP BY user_id
                )
                WHERE start_day BETWEEN yesterday() - 30 AND yesterday() - 7
                GROUP BY start_day
            ),
            day7 AS (
                SELECT
                    u.start_day,
                    COUNT(DISTINCT fa.user_id) AS users_d7
                FROM
                (
                    SELECT
                        user_id,
                        MIN(toDate(time)) AS start_day
                    FROM simulator_20251220.feed_actions
                    GROUP BY user_id
                ) u
                INNER JOIN simulator_20251220.feed_actions fa
                    ON fa.user_id = u.user_id
                WHERE u.start_day BETWEEN yesterday() - 30 AND yesterday() - 7
                  AND toDate(fa.time) = u.start_day + 7
                GROUP BY u.start_day
            )
            SELECT
                7 AS day_number,
                round(avg(ifNull(users_d7, 0) * 100.0 / cohort_users), 2) AS avg_retention_pct,
                count() AS cohorts_count
            FROM cohorts
            LEFT JOIN day7 USING (start_day);
            """
        retention_avg_feed_7d = select(sql_retention_avg_feed_7d)
        
        
        
        
        return {
            'dau_yesterday': dau_yesterday,
            'dau_30d': dau_30d,
            'df_segments': df_segments,
            'messages_details': messages_details,
            'weekly_avg': weekly_avg,
            'feed_7d': feed_7d,
            'msg_7d': msg_7d,
            'retention_avg_feed_7d': retention_avg_feed_7d,
            'retention_avg_msg_7d': retention_avg_msg_7d,
        }

    
    @task
    def create_report(data):
        """Создание отчета на основе всех собранных данных"""

        try:
            # извлекаем данные из словаря
            dau_yesterday = data['dau_yesterday']
            dau_30d = data['dau_30d']
            df_segments = data['df_segments']
            messages_details = data['messages_details']
            weekly_avg = data['weekly_avg']
            
            
             # retention данные
            feed_7d = data['feed_7d']
            msg_7d = data['msg_7d']
            retention_avg_feed_7d = data['retention_avg_feed_7d']
            retention_avg_msg_7d = data['retention_avg_msg_7d']
            
            #  безопасные преобразования типов (боремся с numpy.uint64 переполнениями)
            def as_int(x):
                try:
                    return int(x)
                except Exception:
                    return 0

            def as_float(x):
                try:
                    return float(x)
                except Exception:
                    return 0.0
            # форматируем числа для читаемого вывода
            def fmt(num):
                return f"{as_int(num):,}".replace(",", " ")

            # дата отчета
            report_date = pd.to_datetime(dau_yesterday['date'].iloc[0]).date()  
            report_date_str = report_date.strftime('%d.%m.%Y')
            cohort_date = report_date - timedelta(days=7)

            # данные за вчера
            feed_dau = as_int(dau_yesterday['feed_dau'].iloc[0])
            feed_views = as_int(dau_yesterday['feed_views'].iloc[0])
            feed_likes = as_int(dau_yesterday['feed_likes'].iloc[0])
            feed_ctr = as_float(dau_yesterday['ctr'].iloc[0])
            msg_dau = as_int(dau_yesterday['msg_dau'].iloc[0])
            app_dau_total = as_int(dau_yesterday['app_dau_total'].iloc[0])


            # детали мессенджера
            msg_details = messages_details.iloc[0] if not messages_details.empty else {}
            total_messages = as_int(msg_details.get('total_messages', 0))
            active_senders = as_int(msg_details.get('active_senders', 0))
            active_receivers = as_int(msg_details.get('active_receivers', 0))
            avg_messages_per_sender = as_float(msg_details.get('avg_messages_per_sender', 0))
            unique_conversations = as_int(msg_details.get('unique_conversations', 0))
            
            # дополнительные метрики мессенджера
            messages_per_conversation = (total_messages / unique_conversations) if unique_conversations > 0 else 0.0

            # средние за неделю
            week_avg_data = weekly_avg.iloc[0] if not weekly_avg.empty else pd.Series(dtype=float)
            
            #  функция сравнения с неделей (в одинаковых единицах)
            def week_comparison(current, avg_key):
                if week_avg_data is None or len(week_avg_data) == 0:
                    return ""
                avg = as_float(week_avg_data.get(avg_key, 0))
                if avg <= 0:
                    return ""
                change = (as_float(current) - avg) / avg * 100
                arrow = "↗" if change > 0 else "↘" if change < 0 else "→"
                return f"{arrow} {change:+.1f}%"
            
            # просмотры и лайки на юзера
            views_per_user = (feed_views / feed_dau) if feed_dau > 0 else 0.0
            likes_per_user = (feed_likes / feed_dau) if feed_dau > 0 else 0.0
            
            # cообщения на пользователя
            messages_per_sender = (total_messages / active_senders) if active_senders > 0 else 0.0
            messages_per_receiver = (total_messages / active_receivers) if active_receivers > 0 else 0.0

            # динамика dau за 30 дней
            feed_30d = dau_30d[dau_30d['product'] == 'feed'].copy()
            msg_30d = dau_30d[dau_30d['product'] == 'messenger'].copy()
            
            
            # сортируем по дате
            feed_30d['date'] = pd.to_datetime(feed_30d['date'])
            msg_30d['date'] = pd.to_datetime(msg_30d['date'])
            feed_30d = feed_30d.sort_values('date').reset_index(drop=True)
            msg_30d = msg_30d.sort_values('date').reset_index(drop=True)
            

            # расчет изменений за 30 дней
            if len(feed_30d) >= 2:
                feed_first = as_int(feed_30d.iloc[0]['dau'])
                feed_last = as_int(feed_30d.iloc[-1]['dau'])
                feed_change_30d = ((feed_last - feed_first) / feed_first * 100) if feed_first > 0 else 0.0
                feed_dau_index_30d = (feed_last / feed_first * 100) if feed_first > 0 else 0.0  
            else:
                feed_first, feed_last = 0, 0
                feed_change_30d = 0.0
                feed_dau_index_30d = 0.0  

            if len(msg_30d) >= 2:
                msg_first = as_int(msg_30d.iloc[0]['dau'])
                msg_last = as_int(msg_30d.iloc[-1]['dau'])
                msg_change_30d = ((msg_last - msg_first) / msg_first * 100) if msg_first > 0 else 0.0
                msg_dau_index_30d = (msg_last / msg_first * 100) if msg_first > 0 else 0.0  
            else:
                msg_first, msg_last = 0, 0
                msg_change_30d = 0.0
                msg_dau_index_30d = 0.0

            # CЕГМЕНТАЦИЯ 
            #  приводим __timestamp к типу date, чтобы сравнивать с report_date
            df_segments = df_segments.copy()
            df_segments['__timestamp'] = pd.to_datetime(df_segments['__timestamp']).dt.date  

            # сегменты за дату отчёта (вчера)
            segments_for_date = df_segments[df_segments['__timestamp'] == report_date]

            #  если за вчера строк нет, берём ближайший день ДО или РАВНЫЙ report_date
            if segments_for_date.empty:
                available_dates = df_segments.loc[df_segments['__timestamp'] <= report_date, '__timestamp']
                if available_dates.empty:
                    seg_date = report_date
                    segments_for_date = df_segments.iloc[0:0] 
                else:
                    seg_date = available_dates.max()  
                    segments_for_date = df_segments[df_segments['__timestamp'] == seg_date]  
            else:
                seg_date = report_date

            # cобираем сегменты в словарь 
            segments_dict = {}
            for _, row in segments_for_date.iterrows():
                segments_dict[row['type_user']] = as_int(row['SUM(user_count)'])

            both_users = as_int(segments_dict.get('Лента и сообщения', 0))
            only_feed = as_int(segments_dict.get('Только лента', 0))
            only_msg = as_int(segments_dict.get('Только сообщения', 0))

            #  метрики кросс-использования
            total_from_segments = both_users + only_feed + only_msg
            overlap_rate_app = (both_users / total_from_segments * 100) if total_from_segments > 0 else 0.0
            msg_penetration_in_feed = (both_users / feed_dau * 100) if feed_dau > 0 else 0.0
            feed_penetration_in_msg = (both_users / msg_dau * 100) if msg_dau > 0 else 0.0
            
            # Обработка retention данных
            def get_retention_value(df, prefix='', is_avg=False):
                """Извлекает retention данные из DataFrame"""
                if df is None or df.empty:
                    result = {f'{prefix}retention_rate': 0}
                    if not is_avg:
                        result[f'{prefix}cohort_size'] = 0
                        result[f'{prefix}retained'] = 0
                    return result

                result = {}

                # Retention rate
                if 'retention_d7_pct' in df.columns:
                    result[f'{prefix}retention_rate'] = as_float(df['retention_d7_pct'].iloc[0])
                elif 'avg_retention_pct' in df.columns:
                    result[f'{prefix}retention_rate'] = as_float(df['avg_retention_pct'].iloc[0])
                else:
                    result[f'{prefix}retention_rate'] = 0

                # только для текущей когорты
                if not is_avg:
                    result[f'{prefix}cohort_size'] = as_int(df['cohort_size'].iloc[0]) if 'cohort_size' in df.columns else 0
                    result[f'{prefix}retained'] = as_int(df['retained_d7'].iloc[0]) if 'retained_d7' in df.columns else 0


                return result

            def format_diff(diff):
                """Форматирует разницу в процентных пунктах"""
                if diff > 0:
                    return f"+{diff:.1f} п.п."
                elif diff < 0:
                    return f"{diff:.1f} п.п."
                else:
                    return "±0.0 п.п."

            # retention 7d: текущая когорта vs среднее за 30 дней ---
            feed_curr = get_retention_value(feed_7d, 'feed_', is_avg=False)
            msg_curr = get_retention_value(msg_7d, 'msg_', is_avg=False)
            feed_avg = get_retention_value(retention_avg_feed_7d, 'feed_avg_', is_avg=True)
            msg_avg = get_retention_value(retention_avg_msg_7d, 'msg_avg_', is_avg=True)

            feed_retention = feed_curr.get('feed_retention_rate', 0)
            msg_retention = msg_curr.get('msg_retention_rate', 0)
            feed_cohort_size = feed_curr.get('feed_cohort_size', 0)
            msg_cohort_size = msg_curr.get('msg_cohort_size', 0)
            feed_retained = feed_curr.get('feed_retained', 0)  
            msg_retained = msg_curr.get('msg_retained', 0)    

            feed_avg_retention = feed_avg.get('feed_avg_retention_rate', 0)
            msg_avg_retention = msg_avg.get('msg_avg_retention_rate', 0)

            feed_diff_formatted = format_diff(feed_retention - feed_avg_retention)
            msg_diff_formatted = format_diff(msg_retention - msg_avg_retention)
            

            window_note = "Окно динамики: BETWEEN yesterday()-30 AND yesterday() (31 календарный день)"


            # формирование отчета
            report = f"""
                📊 КОМПЛЕКСНЫЙ ОТЧЕТ ПО ПРИЛОЖЕНИЮ
                Дата: {report_date_str}
                Период анализа: 31 день | Сравнение со средним за неделю: 7 дней
                {window_note}

                👥 АУДИТОРИЯ ПРИЛОЖЕНИЯ (вчера):
                • App DAU (уникальные пользователи): {fmt(app_dau_total)}
                • Feed DAU: {fmt(feed_dau)} {week_comparison(feed_dau, 'avg_feed_dau_7d')}
                • Msg DAU: {fmt(msg_dau)} {week_comparison(msg_dau, 'avg_msg_dau_7d')}
                • Всего событий: {fmt(feed_views + feed_likes + total_messages)}

                🎯 ЛЕНТА НОВОСТЕЙ (вчера):
                • Просмотры: {fmt(feed_views)} {week_comparison(feed_views, 'avg_views_7d')}
                • Лайки: {fmt(feed_likes)} {week_comparison(feed_likes, 'avg_likes_7d')}
                • CTR (likes/views): {feed_ctr:.2%} {week_comparison(feed_ctr, 'avg_ctr_7d')}
                • Views per user: {views_per_user:.1f}
                • Likes per user: {likes_per_user:.2f}

                💬 МЕССЕНДЖЕР (вчера):
                • Всего сообщений: {fmt(total_messages)}
                • Отправили (уник. отправители): {fmt(active_senders)}
                • Получили (уник. получатели): {fmt(active_receivers)}
                • Уникальных диалогов: {fmt(unique_conversations)}
                • Сообщений на отправителя: {avg_messages_per_sender:.1f}
                • Сообщений на диалог: {messages_per_conversation:.1f}

                👤 СЕГМЕНТАЦИЯ ПОЛЬЗОВАТЕЛЕЙ ({seg_date.strftime('%d.%m.%Y')}):
                • Используют оба сервиса: {fmt(both_users)}
                • Только лента: {fmt(only_feed)}
                • Только мессенджер: {fmt(only_msg)}

                🔗 КРОСС-ИСПОЛЬЗОВАНИЕ (за день):
                • Overlap rate (оба сервиса / все активные): {overlap_rate_app:.1f}%
                • Проникновение мессенджера среди DAU ленты: {msg_penetration_in_feed:.1f}%
                • Проникновение ленты среди DAU мессенджера: {feed_penetration_in_msg:.1f}%

                📈 ДИНАМИКА (по DAU):
                🎯 ЛЕНТА:
                • Начало: {fmt(feed_first)}
                • Конец: {fmt(feed_last)}
                • Изменение: {feed_change_30d:+.1f}%
                • DAU index (конец/начало): {feed_dau_index_30d:.1f}%

                💬 МЕССЕНДЖЕР:
                • Начало: {fmt(msg_first)}
                • Конец: {fmt(msg_last)}
                • Изменение: {msg_change_30d:+.1f}%
                • DAU index (конец/начало): {msg_dau_index_30d:.1f}%
                
                
                
                🔄 RETENTION D7 (когорта {cohort_date.strftime('%d.%m.%Y')}, сравнение со средним за 30д):
               🎯 Лента: {feed_retention:.1f}% ({fmt(feed_retained)}/{fmt(feed_cohort_size)}) {feed_diff_formatted}
                💬 Мессенджер: {msg_retention:.1f}% ({fmt(msg_retained)}/{fmt(msg_cohort_size)}) {msg_diff_formatted}

                
                """

            print("✅ Отчет сгенерирован")
            return report.strip()

        except Exception as e:
            error_msg = f"❌ Ошибка при генерации отчета: {e}"
            print(error_msg)
            return error_msg
        
        
    # Подготовка графиков с сохранением в файл
    @task
    def create_charts(data) -> str:
        """
        5 графиков в одном PNG:
        DAU | CTR (с Avg 30d)
        Views | Likes (раздельно)
        Segments (stacked bar на всю ширину, легенда снаружи)
        """
        try:
            import matplotlib.dates as mdates

            dau_30d = data['dau_30d'].copy()
            df_segments = data['df_segments'].copy()

            if dau_30d.empty:
                return ""

            # --- подготовка данных ---
            feed_30d = dau_30d[dau_30d['product'] == 'feed'].copy()
            msg_30d = dau_30d[dau_30d['product'] == 'messenger'].copy()

            feed_30d['date'] = pd.to_datetime(feed_30d['date'])
            msg_30d['date'] = pd.to_datetime(msg_30d['date'])
            feed_30d = feed_30d.sort_values('date')
            msg_30d = msg_30d.sort_values('date')

            recent = feed_30d.tail(14).copy()

            # --- фигура (компактнее, но читаемо для Telegram) ---
            plt.style.use('seaborn-darkgrid')
            fig = plt.figure(figsize=(16, 18))
            fig.suptitle('📊 Аналитика приложения (30 дней)', fontsize=18, y=0.98)

            gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 1.2])

            # === 1) DAU ===
            ax1 = fig.add_subplot(gs[0, 0])
            if not feed_30d.empty:
                ax1.plot(feed_30d['date'], feed_30d['dau'], label='Лента', linewidth=2.5, color='#1f77b4')
            if not msg_30d.empty:
                ax1.plot(msg_30d['date'], msg_30d['dau'], label='Мессенджер', linewidth=2.5, color='#ff7f0e')
            ax1.set_title('DAU: Лента vs Мессенджер', fontsize=13)
            ax1.set_ylabel('DAU')
            ax1.legend(loc='upper left', frameon=True, framealpha=0.9)
            ax1.grid(True, axis='y', alpha=0.25)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
            ax1.tick_params(axis='x', rotation=25)

            # === 2) CTR ===
            ax2 = fig.add_subplot(gs[0, 1])
            if not feed_30d.empty and 'ctr' in feed_30d.columns:
                ax2.plot(feed_30d['date'], feed_30d['ctr'] * 100, linewidth=2.0, color='#2ca02c', label='Daily CTR')
                avg_ctr = float(feed_30d['ctr'].mean() * 100)
                ax2.axhline(y=avg_ctr, color='darkgreen', linestyle='--', linewidth=2.0,
                            label=f'Avg 30d ({avg_ctr:.2f}%)')
                ax2.legend(loc='upper left', frameon=True, framealpha=0.9)
            else:
                ax2.text(0.5, 0.5, 'Нет данных CTR', ha='center', va='center', transform=ax2.transAxes)
            ax2.set_title('CTR ленты (%)', fontsize=13)
            ax2.set_ylabel('CTR, %')
            ax2.grid(True, axis='y', alpha=0.25)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
            ax2.tick_params(axis='x', rotation=25)

            # === 3) Views (14 дней) ===
            ax3 = fig.add_subplot(gs[1, 0])
            if not recent.empty and 'views' in recent.columns:
                ax3.bar(recent['date'], recent['views'], color='#aec7e8', width=0.75)
                ax3.set_title('Просмотры (последние 14 дней)', fontsize=13, color='#1f77b4')
                ax3.set_ylabel('Views')
                ax3.grid(True, axis='y', alpha=0.25)
                ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
                ax3.tick_params(axis='x', rotation=25)
            else:
                ax3.text(0.5, 0.5, 'Нет данных views', ha='center', va='center', transform=ax3.transAxes)
                ax3.set_axis_off()

            # === 4) Likes (14 дней) ===
            ax4 = fig.add_subplot(gs[1, 1])
            if not recent.empty and 'likes' in recent.columns:
                ax4.plot(recent['date'], recent['likes'], color='#d62728', marker='o',
                         linewidth=2.5, markersize=6)
                ax4.set_title('Лайки (последние 14 дней)', fontsize=13, color='#d62728')
                ax4.set_ylabel('Likes')
                ax4.grid(True, axis='y', alpha=0.25)
                ax4.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
                ax4.tick_params(axis='x', rotation=25)
            else:
                ax4.text(0.5, 0.5, 'Нет данных likes', ha='center', va='center', transform=ax4.transAxes)
                ax4.set_axis_off()

            # === 5) Segments (14 дней, stacked) ===
            ax5 = fig.add_subplot(gs[2, :])
            if not df_segments.empty:
                df_segments['__timestamp'] = pd.to_datetime(df_segments['__timestamp'])
                recent_seg = df_segments[
                    df_segments['__timestamp'] >= (df_segments['__timestamp'].max() - pd.Timedelta(days=13))
                ].copy()

                if not recent_seg.empty:
                    pivot = recent_seg.pivot(
                        index='__timestamp',
                        columns='type_user',
                        values='SUM(user_count)'
                    ).fillna(0)

                    cols = ['Только сообщения', 'Только лента', 'Лента и сообщения']
                    cols = [c for c in cols if c in pivot.columns]
                    pivot = pivot[cols]

                    pivot.plot(
                        kind='bar',
                        stacked=True,
                        ax=ax5,
                        width=0.8,
                        color=['#ff9999', '#66b3ff', '#99ff99'],
                        rot=0
                    )

                    ax5.set_title('Структура аудитории: пересечение сервисов (14 дней)', fontsize=13)
                    ax5.set_xlabel('')
                    ax5.grid(True, axis='y', alpha=0.25)

                    # Легенда С НАРУЖИ справа — чтобы не залезала на бары
                    ax5.legend(
                        title='Сегмент',
                        loc='center left',
                        bbox_to_anchor=(1.01, 0.5),
                        frameon=False
                    )

                    # красивее подписи дат
                    labels = [pd.to_datetime(d).strftime('%d.%m') for d in pivot.index]
                    ax5.set_xticklabels(labels, rotation=25)
                else:
                    ax5.text(0.5, 0.5, 'Нет свежих данных сегментов', ha='center', va='center', transform=ax5.transAxes)
            else:
                ax5.text(0.5, 0.5, 'Нет данных сегментов', ha='center', va='center', transform=ax5.transAxes)

            # tight_layout с запасом справа под легенду сегментов
            fig.tight_layout(rect=[0, 0, 0.86, 0.965], h_pad=2.0, w_pad=2.0)

            # --- сохранение ---
            tmp = NamedTemporaryFile(suffix=".png", delete=False)
            tmp.close()
            fig.savefig(tmp.name, dpi=150, bbox_inches='tight')
            plt.close(fig)

            print(f"✅ Charts saved: {tmp.name}")
            return tmp.name

        except Exception as e:
            print(f"❌ Ошибка при создании графиков: {e}")
            import traceback
            traceback.print_exc()
            return ""




    @task
    def test_report(report_text):
        """Тестовая задача для проверки отчета без отправки"""
        print("=" * 50)
        print("🧪 ТЕСТИРУЮ ОТЧЕТ (без отправки):")
        print("=" * 50)
        print(report_text)
        print("=" * 50)
        print("✅ Тест завершен успешно!")
        return "Test passed"
    
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_temp_files(chart_path):
        """Очистка временных файлов после выполнения"""
        print(f"🧹 Cleanup started for: {chart_path}")

        if not chart_path:
            print("⚠️ chart_path is empty or None, nothing to clean")
            return

        if os.path.exists(chart_path):
            file_size = os.path.getsize(chart_path) / 1024  # KB
            print(f"📁 File exists, size: {file_size:.1f} KB")
            try:
                os.remove(chart_path)
                print(f"✅ Successfully deleted: {chart_path}")
            except Exception as e:
                print(f"❌ Failed to delete {chart_path}: {e}")
        else:
            print(f"ℹ️ File already deleted or doesn't exist: {chart_path}")
                
                
    @task
    def send_final_report(report_text, chart_path, chat_id=None):
        """Отправка финального отчета с задержкой между сообщениями"""
        try:
            bot = telegram.Bot(token=TELEGRAM_TOKEN)
            chat_id = chat_id or CHAT_ID


            print(f"📤 Отправляю отчет в Telegram...")

            import time

            # 1. Текстовый отчет
            print("📝 Шаг 1/2: Отправка текстового отчета...")
            bot.sendMessage(
                chat_id=chat_id, 
                text=report_text, 
                parse_mode=telegram.ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            print("✅ Текст отправлен")

            # Небольшая задержка для надежности
            time.sleep(1)

            # 2. Графики
            if chart_path and os.path.exists(chart_path):
                print("🖼️ Шаг 2/2: Отправка графиков...")
                try:
                    with open(chart_path, 'rb') as photo:
                        bot.sendPhoto(
                            chat_id=chat_id,
                            photo=photo,
                            caption='📊 Визуализация метрик за 30 дней'
                        )
                    print("✅ Графики отправлены")
                except Exception as photo_error:
                    print(f"⚠️ Не удалось отправить графики: {photo_error}")
                    # Отправляем сообщение об ошибке с графиками
                    bot.sendMessage(
                        chat_id=chat_id,
                        text="⚠️ Не удалось отправить графики, но текстовый отчет доставлен.",
                        parse_mode=telegram.ParseMode.MARKDOWN
                    )
            else:
                print("ℹ️ Графики отсутствуют")

            print("🎉 Отправка завершена успешно!")
            return "Report sent successfully"

        except Exception as e:  # ← ЭТА СТРОКА ДОЛЖНА БЫТЬ С ОТСТУПОМ ОТ try
            error_msg = f"❌ Критическая ошибка при отправке отчета: {e}"
            print(error_msg)

            # Пытаемся отправить хотя бы сообщение об ошибке
            try:
                bot = telegram.Bot(token=TELEGRAM_TOKEN)
                bot.sendMessage(
                    chat_id=chat_id,
                    text=f"❌ Ошибка при отправке отчета: {str(e)[:150]}...",
                    parse_mode=telegram.ParseMode.MARKDOWN
                )
            except:
                pass

            return error_msg
    
    # Выполнение DAG
        
    # 1. Собираем данные
    data = extract_metrics()
    
    # 2. Генерируем отчет
    report_text = create_report(data)
    
    # 3. Генерируем графики
    chart_path = create_charts(data)
    
    # 4. Тестируем отчет (параллельно)
    test_result = test_report(report_text)
    
    # 5. Отправляем в Telegram (сначала текст, потом графики)
    send_result = send_final_report(report_text, chart_path)
    
     # 6. Очищаем временный файл ПОСЛЕ отправки
    cleanup = cleanup_temp_files(chart_path)

    send_result >> cleanup

# Создаем DAG
dag_app_report_kharchenko = dag_app_report_kharchenko()
        
        
        
        
        