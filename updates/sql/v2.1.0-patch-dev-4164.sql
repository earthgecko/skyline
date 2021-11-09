/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4164
*/

USE skyline;

/*
# @added 20210805 - Feature #4164: luminosity - cloudbursts
*/
CREATE TABLE IF NOT EXISTS `cloudburst` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'cloudburst unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric_id',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'cloudburst unix timestamp',
  `end` INT(10) DEFAULT 0 COMMENT 'cloudburst end unix timestamp',
  `duration` INT(11) NOT NULL COMMENT 'duration of the cloudburst',
  `from_timestamp` INT(10) DEFAULT 0 COMMENT 'cloudburst identified in period starting from unix timestamp',
  `resolution` INT(11) NOT NULL COMMENT 'cloudburst identified at a resolution of',
  `full_duration` INT(11) NOT NULL COMMENT 'cloudburst identified in a period of full_duration',
  `anomaly_id` INT(11) DEFAULT 0 COMMENT 'anomaly id of the anomaly associated with cloudburst',
  `match_id` INT(11) DEFAULT 0 COMMENT 'match id of the match associated with cloudburst',
  `fp_id` INT(11) DEFAULT 0 COMMENT 'fp id of the match associated with cloudburst',
  `layer_id` INT(11) DEFAULT 0 COMMENT 'layer id of the match associated with cloudburst',
  `added_at` INT(10) DEFAULT 0 COMMENT 'unix timestamp when the cloudburst was added',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the cloudburt',
  PRIMARY KEY (id),
  INDEX `cloudburst` (`id`,`metric_id`,`timestamp`,`anomaly_id`,`match_id`))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `cloudbursts` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'cloudbursts unique id',
  `cloudburst_id` INT(11) NOT NULL COMMENT 'the cloudburst id that this is related too',
  `metric_id` INT(11) NOT NULL COMMENT 'metric_id of the metric related to the cloudburst',
  `ppscore_1` DECIMAL(65,6) DEFAULT 0 COMMENT 'the persceptive power 1 score',
  `ppscore_2` DECIMAL(65,6) DEFAULT 0 COMMENT 'the persceptive power 2 score',
  PRIMARY KEY (id),
  INDEX `cloudbursts` (`id`,`cloudburst_id`,`metric_id`))
  ENGINE=InnoDB;

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-4164');
