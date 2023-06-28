/*
This is the SQL script to update Skyline from v3.0.x to v3.1.0
*/

USE skyline;

/*
# @added 20220622 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Change engine and increase the column size to handle metric names and labels.
# First the index needs to be dropped otherwise the column cannot be modified
# and an error is thrown:
# ERROR 1071 (42000) at line 13: Specified key was too long; max key length is 3072 bytes
*/
ALTER TABLE metrics ENGINE=InnoDB;

DROP INDEX `metric` ON `metrics`;

ALTER TABLE `metrics` MODIFY COLUMN `metric` VARCHAR(4096);

CREATE INDEX `metric` ON `metrics`(`id`,`metric`(255),`ionosphere_enabled`,`inactive`);

/*
# @added 20220630 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
CREATE TABLE IF NOT EXISTS `labelled_metrics_names` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'metric name unique id',
  `metric_name` VARCHAR(4096) NOT NULL COMMENT 'metric name',
  PRIMARY KEY (id),
  INDEX `name` (`id`,`metric_name`(255)))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `labelled_metrics_labels` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'label unique id',
  `label` VARCHAR(4096) NOT NULL COMMENT 'label',
  PRIMARY KEY (id),
  INDEX `label` (`id`,`label`(255)))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `labelled_metrics_values` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'value unique id',
  `value` VARCHAR(4096) NOT NULL COMMENT 'value',
  PRIMARY KEY (id),
  INDEX `value` (`id`,`value`(255)))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `labelled_metrics_hashes` (
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `metric_hash` VARCHAR(4096) NOT NULL COMMENT 'metric hash',
  PRIMARY KEY (metric_id),
  INDEX `metric_hash` (`metric_id`,`metric_hash`(255)))
  ENGINE=InnoDB;
*/

/*
# @added 20221026 - Feature #4708: ionosphere - store and cache fp minmax data
# Added table to store the results of minmax scaled features profiles.
*/
CREATE TABLE IF NOT EXISTS `ionosphere_minmax` (
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `minmax_min` DOUBLE DEFAULT NULL COMMENT 'the min value in the minmax timeseries',
  `minmax_max` DOUBLE DEFAULT NULL COMMENT 'the max value in the minmax timeseries',
  `values_count` INT(10) DEFAULT NULL COMMENT 'the number of values in the fp timeseries',
  `features_count` INT(10) DEFAULT NULL COMMENT 'the number of features calculated',
  `features_sum` DOUBLE DEFAULT NULL COMMENT 'the sum of the features',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  `calc_time` FLOAT DEFAULT NULL COMMENT 'the time taken in seconds to calcalute the features',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (fp_id),
  INDEX `ionosphere_minmax` (`fp_id`,`tsfresh_version`))
  ENGINE=InnoDB;
/*
INSERT INTO `sql_versions` (version) VALUES ('3.1.0-patch-dev-4708');
*/

INSERT INTO `sql_versions` (version) VALUES ('3.1.0');
