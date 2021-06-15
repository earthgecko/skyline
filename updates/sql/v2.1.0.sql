/*
This is the SQL script to update Skyline from v2.0.0 to v2.1.0
*/

USE skyline;

/* # @added 20200825 - Feature #3704: Add alert to anomalies */
ALTER TABLE `anomalies` ADD COLUMN `alert` INT(11) DEFAULT NULL COMMENT 'if an alert was sent for the anomaly the timestamp it was sent' AFTER `user_id`;
CREATE INDEX alert ON anomalies (alert);

/*
Although any SMTP only anomalis cannot be updated in terms of alert time, slack
ones can be
*/
UPDATE `anomalies` SET `alert` = `slack_thread_ts`;

/*
# @added 20200928 - Task #3748: POC SNAB
#                   Branch #3068: SNAB
*/
CREATE TABLE IF NOT EXISTS `snab` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'snab id',
  `anomaly_id` INT(11) NOT NULL COMMENT 'anomaly id',
  `anomalyScore` DECIMAL(7,6) DEFAULT NULL COMMENT 'anomalyScore',
  `runtime` DECIMAL(11,6) DEFAULT NULL COMMENT 'runtime',
  `app_id` INT(11) NOT NULL COMMENT 'app id',
  `algorithm_group_id` INT(11) NOT NULL COMMENT 'algorithm group id',
  `algorithm_id` INT(11) DEFAULT NULL COMMENT 'algorithm id',
  `tP` TINYINT(1) DEFAULT NULL COMMENT 'true positive',
  `fP` TINYINT(1) DEFAULT NULL COMMENT 'false positive',
  `tN` TINYINT(1) DEFAULT NULL COMMENT 'true negative',
  `fN` TINYINT(1) DEFAULT NULL COMMENT 'false negative',
  `unsure` TINYINT(1) DEFAULT NULL COMMENT 'unsure',
  `snab_timestamp` INT(11) NOT NULL COMMENT 'unix timestamp at which the snab entry was added',
  `slack_thread_ts` DECIMAL(17,6) DEFAULT 0 COMMENT 'the slack thread ts',
  PRIMARY KEY (id),
  INDEX `snab` (`id`,`anomaly_id`,`anomalyScore`,`app_id`,`algorithm_group_id`,`algorithm_id`,`tP`,`fP`,`tN`,`fN`,`unsure`))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `algorithm_groups` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'algorithm group unique id',
  `algorithm_group` VARCHAR(255) NOT NULL COMMENT 'algorithm group name, e.g. three-sigma`',
  PRIMARY KEY (id),
  INDEX `algorithm_group_id` (`id`,`algorithm_group`)) ENGINE=InnoDB;
INSERT INTO `algorithm_groups` (algorithm_group) VALUES ('three-sigma');
INSERT INTO `algorithm_groups` (algorithm_group) VALUES ('matrixprofile');

/*
# @added 20201104 - Feature #3828: Add inactive columns to the metrics DB table
# Add inactive and inactive_at columns and add inactive and ionosphere_enabled
# to the index
*/
ALTER TABLE `metrics` ADD COLUMN `inactive` TINYINT(1) DEFAULT 0 COMMENT 'inactive 1 or active 0';
ALTER TABLE `metrics` ADD COLUMN `inactive_at` INT(11) DEFAULT NULL COMMENT 'unix timestamp when declared inactive';
ALTER TABLE `metrics` DROP INDEX `metric`;
/* Add inactive_at AND ionosphere_enabled */
ALTER TABLE `metrics` ADD INDEX `metric` (`id`,`metric`,`ionosphere_enabled`,`inactive`);

/*
# @added 20210201 - Feature #3934: ionosphere_performance
*/
ALTER TABLE `ionosphere_matched` DROP INDEX `features_profile_matched`;
ALTER TABLE `ionosphere_matched` ADD INDEX `features_profile_matched` (`id`,`fp_id`,`metric_timestamp`);
ALTER TABLE `ionosphere_layers_matched` DROP INDEX `layers_matched`;
ALTER TABLE `ionosphere_layers_matched` ADD INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`,`anomaly_timestamp`);

/*
# @added 20210308 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
*/
CREATE TABLE IF NOT EXISTS `anomaly_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly type id',
  `algorithm` VARCHAR(255) NOT NULL COMMENT 'algorithm name associated with type, e.g. adtk_level_shift`',
  `type` VARCHAR(255) NOT NULL COMMENT 'anomaly type name, e.g. levelshift`',
  PRIMARY KEY (id),
  INDEX `anomaly_type_id` (`id`,`type`)) ENGINE=InnoDB;
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_level_shift', 'level shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_volatility_shift', 'volatility shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_persist', 'persist');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_seasonal', 'seasonal');

CREATE TABLE IF NOT EXISTS `anomalies_type` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `type` VARCHAR(255) NOT NULL COMMENT 'a csv list of the anomaly_types ids e.g 1,2,3',
  INDEX `anomalies_type_id` (`id`,`metric_id`)) ENGINE=InnoDB;
/*
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3978-3642');
*/

INSERT INTO `sql_versions` (version) VALUES ('2.1.0');
