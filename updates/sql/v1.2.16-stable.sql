/*
This is the SQL script to update Skyline from ionosphere (v1.2.12 to v1.2.16)
*/

USE skyline;

# @added 20190501 - Branch #2646: slack
ALTER TABLE `anomalies` ADD COLUMN `slack_thread_ts` DECIMAL(17,6) DEFAULT 0 COMMENT 'the slack thread ts' AFTER `created_timestamp`;

# @added 20190501 - Bug #2638: anomalies db table - anomalous_datapoint greater than DECIMAL
# Changed DECIMAL(18,6) to DECIMAL(65,6)
ALTER TABLE `ionosphere_layers_matched` MODIFY `anomalous_datapoint` DECIMAL(65,6);

# @added 20190501 - Task #2980: Change DB defaults from NULL
ALTER TABLE `ionosphere` MODIFY `enabled` tinyint(1) DEFAULT 1;
ALTER TABLE `metrics` MODIFY `ionosphere_enabled` tinyint(1) DEFAULT 0;
COMMIT;
UPDATE `metrics` SET ionosphere_enabled=0 WHERE ionosphere_enabled IS NULL;
