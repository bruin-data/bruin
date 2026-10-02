-- An alert with a delivered attempt is never sent again.
SELECT alert_key, COUNT(*) AS deliveries
FROM operations.alert_delivery_attempt
WHERE record_kind = 'attempt' AND status = 'delivered'
GROUP BY alert_key
HAVING COUNT(*) > 1
