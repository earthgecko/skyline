/*
This is the SQL script to update Skyline from v1.3.0 to v2.0.0
*/

USE skyline;

/* @added 20191031 - Feature #3306: Record anomaly_end_timestamp
                     Branch #3262: py3
# Added anomaly_end_timestamp */
ALTER TABLE `anomalies` ADD COLUMN `anomaly_end_timestamp` INT(11) DEFAULT NULL COMMENT 'end of the anomaly unix timestamp' AFTER `anomaly_timestamp`;
COMMIT;
