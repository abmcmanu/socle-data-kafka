-- Contrôles de cohérence entre le temps réel et le data lake (à lancer chaque jour sur J-1).
-- 1. Doublons : doit renvoyer 0 ligne (exactly-once de bout en bout).
SELECT orderId, COUNT(*) AS n
FROM raw.orders_enriched
WHERE dt = date_format(date_sub(current_date, 1), 'yyyy-MM-dd')
GROUP BY orderId
HAVING COUNT(*) > 1;

-- 2. Rapprochement : CA recalculé depuis les commandes = somme des agrégats minute.
SELECT d.currency, d.ca_commandes, a.ca_agregats, d.ca_commandes - a.ca_agregats AS ecart
FROM (SELECT currency, SUM(totalAmount) AS ca_commandes
      FROM raw.orders_enriched
      WHERE dt = date_format(date_sub(current_date, 1), 'yyyy-MM-dd')
      GROUP BY currency) d
JOIN (SELECT currency, SUM(revenue) AS ca_agregats
      FROM raw.revenue_by_country_1m
      WHERE dt = date_format(date_sub(current_date, 1), 'yyyy-MM-dd')
      GROUP BY currency) a
ON d.currency = a.currency;

-- 3. Part des commandes sans client connu (qualité du CRM).
SELECT ROUND(100 * SUM(CASE WHEN customerSegment IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_client_inconnu
FROM raw.orders_enriched
WHERE dt = date_format(date_sub(current_date, 1), 'yyyy-MM-dd');
