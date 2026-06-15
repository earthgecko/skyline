/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5370
*/

USE skyline;

/* @added 20240610 - Feature #5370: anomalies_updated
#                    Feature #5352: vista - bigquery
#                    Feature #5372: vista - bq_update
# To record updates made to an anomalies
*/
CREATE TABLE IF NOT EXISTS `anomalies_updated` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'unique id of the update',
  `anomaly_id` INT(11) NOT NULL COMMENT 'anomaly unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `changed_timestamp` INT(11) NOT NULL COMMENT 'unix timestamp of when the anomaly was changed',
  `column` VARCHAR(64) DEFAULT NULL COMMENT 'the column that was update',
  `previous_value` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
  `new_value` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
  PRIMARY KEY (id),
  INDEX `anomalies_updated` (`id`,`anomaly_id`,`metric_id`,`changed_timestamp`, `column`))
    ENGINE=InnoDB;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5370');
