-- alert_key = hash(content_id, destination, routing_policy_version); each combination routes once.
SELECT content_id, destination, routing_policy_version, COUNT(*) AS alerts
FROM operations.fct_alert_decision
GROUP BY content_id, destination, routing_policy_version
HAVING COUNT(*) > 1
