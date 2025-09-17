WITH account_stats AS(
    SELECT ses.date,
        sep.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
        COUNT(DISTINCT acc.id) AS account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg
    FROM `DA.account` AS acc
    JOIN `DA.account_session` AS acs
      ON acc.id = acs.account_id
    JOIN `DA.session` AS ses
      ON acs.ga_session_id = ses.ga_session_id
    JOIN `DA.session_params` AS sep
      ON ses.ga_session_id = sep.ga_session_id
    GROUP BY ses.date, sep.country, acc.send_interval, acc.is_verified, acc.is_unsubscribed
),
email_stats AS(
    SELECT
        DATE_ADD(ses.date, INTERVAL es.sent_date DAY) AS date,
        sep.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
        0 AS account_cnt,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
      FROM `DA.account` acc
      JOIN `DA.account_session` acs ON acc.id = acs.account_id
      JOIN `DA.session` ses ON acs.ga_session_id = ses.ga_session_id
      JOIN `DA.session_params` sep ON ses.ga_session_id = sep.ga_session_id


      JOIN `DA.email_sent` es ON acc.id = es.id_account
      LEFT JOIN `DA.email_open` eo ON acc.id = eo.id_account
      LEFT JOIN `DA.email_visit` ev ON acc.id = ev.id_account
      WHERE es.id_message IS NOT NULL
      GROUP BY 1, 2, 3, 4, 5
),




union_email_and_account AS(
  SELECT date,
      country,
      send_interval,
      is_verified,
      is_unsubscribed,
      SUM(account_cnt) AS account_cnt,
      SUM(sent_msg) AS sent_msg,
      SUM(open_msg) AS open_msg,
      SUM(visit_msg) AS visit_msg
  FROM(
        SELECT *
        FROM account_stats
        UNION ALL
        SELECT *
        FROM email_stats
  ) as unt
  GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),
total_for_country AS(
  SELECT *,
      SUM(account_cnt) OVER(PARTITION BY country) AS total_country_account_cnt,
      SUM(sent_msg) OVER(PARTITION BY country) AS total_country_sent_cnt,
  FROM union_email_and_account
),
rank_country AS(
  SELECT *,
      DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
      DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
  FROM total_for_country
)
SELECT date, country, send_interval, is_verified, is_unsubscribed,
    account_cnt, sent_msg, open_msg, visit_msg,
    total_country_account_cnt,total_country_sent_cnt, rank_total_country_account_cnt, rank_total_country_sent_cnt
FROM rank_country
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10