/*
This the the SQL script to update Skyline from ionosphere (v1.1.1 - v1.1.2)
*/

USE skyline;

/*
# @added 20170305 - Feature #1960: ionosphere_layers
#                   Branch #922: ionosphere
*/
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
ALTER TABLE `ionosphere` ADD COLUMN `layers_id` INT(11) DEFAULT 0 COMMENT 'the id of the ionosphere_layers profile, 0 being none' AFTER `generation`;
COMMIT;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`);
COMMIT;

CREATE TABLE IF NOT EXISTS `ionosphere_layers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere matched unique id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id to which the layer is associated with',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `enabled` tinyint(1) DEFAULT 1 COMMENT 'the layer is enabled 1 or not enabled 0',
  `deleted` INT(10) DEFAULT NULL COMMENT 'the unix timestamp the layer was deleted',
  `matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times this layer has been matched',
  `last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this layer was matched',
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this layer was checked',
  `check_count` INT(10) DEFAULT 0 COMMENT 'the number of times this layer has been checked',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the layer',
  PRIMARY KEY (id),
  INDEX `ionosphere_layers` (`id`,`fp_id`,`metric_id`)  KEY_BLOCK_SIZE=255)
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `layers_algorithms` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'layer unique id',
  `layer_id` INT(11) NOT NULL COMMENT 'the id of the ionosphere_layers that the layer belongs to',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id to which the layer is associated with',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `layer` VARCHAR(2) DEFAULT NULL COMMENT 'the layer name, D, E, Es, F1 or F2',
  `type` VARCHAR(10) DEFAULT NULL COMMENT 'the layer type - value, time, day',
  `condition` VARCHAR(10) DEFAULT NULL COMMENT 'the condition - >, <, ==, !=, in, not in',
  `layer_boundary` VARCHAR(255) DEFAULT NULL COMMENT 'the value of the layer boundary, int or str',
  `times_in_row` INT DEFAULT NULL COMMENT 'number of times in a row',
  PRIMARY KEY (id),
  INDEX `layer_id` (`id`,`layer_id`,`fp_id`,`metric_id`)  KEY_BLOCK_SIZE=255)
  ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `ionosphere_layers_matched` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere_layers matched unique id',
  `layer_id` INT(11) NOT NULL COMMENT 'layer id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `anomaly_timestamp` INT(11) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above',
  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries which matched',
  PRIMARY KEY (id),
  INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`)  KEY_BLOCK_SIZE=255)
  ENGINE=InnoDB;
