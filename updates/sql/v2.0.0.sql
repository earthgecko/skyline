/*
This is the SQL script to update Skyline from v1.3.0 to v2.0.0
*/

USE skyline;

/* @added 20191031 - Feature #3306: Record anomaly_end_timestamp
                     Branch #3262: py3
# Added anomaly_end_timestamp */
ALTER TABLE `anomalies` ADD COLUMN `anomaly_end_timestamp` INT(11) DEFAULT NULL COMMENT 'end of the anomaly unix timestamp' AFTER `anomaly_timestamp`;
COMMIT;

/* @added 20191231 - Feature #3370: Add additional indices to DB
                     Branch #3262: py3
# For better query performance */
CREATE INDEX metric_id ON ionosphere (metric_id);
CREATE INDEX anomaly_timestamp ON anomalies (anomaly_timestamp);

/*
# @added 20200411 - Feature #3478: sql_versions table
#                   Branch #3262: py3
# Added a versions table to the DB as a method to track what version of the DB schema is being run.
# This eases tracking and skipping versions by know what DB updates need to be applied.
# patch-3478
*/
CREATE TABLE IF NOT EXISTS `sql_versions` (
  `version` VARCHAR(255) DEFAULT NULL COMMENT 'version',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp')
  ENGINE=InnoDB;
INSERT INTO `sql_versions` (version) VALUES ('2.0.0');
INSERT INTO `sql_versions` (version) VALUES ('2.0.0-patch-3478');

/*
# @added 20200505 - Feature #3530: Add new index on ionosphere table - validated
#                   Branch #3262: py3
# Update index to include the validated column
*/
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`,`validated`);
INSERT INTO `sql_versions` (version) VALUES ('2.0.0-patch-3530');
