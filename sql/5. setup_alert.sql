USE ROLE KAFKA_OPENFLOW_ROLE;
USE DATABASE KAFKA_OPENFLOW_DB;

-- Alert if error rate exceeds threshold
CREATE OR REPLACE ALERT PUBLIC.HIGH_ERROR_RATE_ALERT
  WAREHOUSE = KAFKA_OPENFLOW_WH
  SCHEDULE = '5 MINUTE'
  IF( EXISTS(
    SELECT 
      SUM(CASE WHEN LEVEL = 'ERROR' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) as cnt
      FROM "APPLICATION-LOGS"
      WHERE TO_TIMESTAMP(TIMESTAMP) >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
      AND cnt > 15
    ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'DataOps Team',
    'ops@example.com',
    'Kafka Pipeline Alert: Error rate exceeds 15% in last 5 minutes'
  );

-- Alert on critical service errors
CREATE OR REPLACE ALERT PUBLIC.CRITICAL_SERVICE_ALERT
  WAREHOUSE = KAFKA_OPENFLOW_WH
  SCHEDULE = '5 MINUTE'
  IF( EXISTS(
    SELECT COUNT(*) as cnt
    FROM "APPLICATION-LOGS"
    WHERE LEVEL = 'ERROR'
      AND SERVICE IN ('payment-service', 'auth-service')
      AND TO_TIMESTAMP(TIMESTAMP) >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
      AND cnt > 10

))
THEN CALL SYSTEM$SEND_EMAIL(
    'DataOps Team',
    'ops@example.com',
    'Kafka Pipeline Alert: Critical service experiencing multiple errors'
  );
