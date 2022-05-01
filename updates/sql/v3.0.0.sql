/*
This is the SQL script to update Skyline from v2.0.0 to v3.0.0
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

/*
# @added 20210412 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
*/
ALTER TABLE `ionosphere_matched` ADD COLUMN `motifs_matched_id` INT DEFAULT NULL COMMENT 'the motif match id from the motifs_matched table, if this was a motif matched' AFTER `fp_checked`;
COMMIT;

/*
# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Added motif related columns to the ionosphere table
*/
ALTER TABLE `ionosphere` ADD COLUMN `motif_matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times a motif from this feature profile has been matched' AFTER `generation`;
COMMIT;

ALTER TABLE `ionosphere` ADD COLUMN `motif_last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time a motif from this feature profile was matched' AFTER `motif_matched_count`;
COMMIT;

ALTER TABLE `ionosphere` ADD COLUMN `motif_last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time a motif from this feature profile was checked' AFTER `motif_last_matched`;
COMMIT;

ALTER TABLE `ionosphere` ADD COLUMN `motif_checked_count` INT(10) DEFAULT 0 COMMENT 'the number of times a motifs from this feature profile have been checked' AFTER `motif_last_checked`;
COMMIT;

CREATE TABLE IF NOT EXISTS `motif_matched_types` (
  `id` INT(11) NOT NULL COMMENT 'matched type id',
  `type` VARCHAR(255) NOT NULL COMMENT 'motif match type name, e.g. exact`',
  PRIMARY KEY (id),
  INDEX `motif_matched_types` (`id`,`type`)) ENGINE=InnoDB;
/* NOTICE: these types are also maintained in ionosphere/inference.py */
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (1, 'exact');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (2, 'all_in_range');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (3, 'in_range');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (4, 'not_similar_enough');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (5, 'INVALIDATED');
INSERT INTO `motif_matched_types` (`id`,`type`) VALUES (6, 'distance');

CREATE TABLE IF NOT EXISTS `motifs_matched` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'inference matched unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `metric_timestamp` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the not anomalous datapoint that matched the motif of the fp',
  `primary_match` TINYINT(4) DEFAULT 0 COMMENT 'whether the match is the primary match for the instance',
  `index` INT(11) NOT NULL COMMENT 'the index of the motif within the features profile timeseries',
  `size` INT(11) NOT NULL COMMENT 'the size of the motif',
  `distance` DOUBLE DEFAULT 0 COMMENT 'the motif distance',
/* @added 202210427 - motif_area, fp_motif_area and area_percent_diff */
  `motif_area` DOUBLE DEFAULT 0 COMMENT 'the motif area from the composite trapezoidal rule',
  `fp_motif_area` DOUBLE DEFAULT 0 COMMENT 'the fp motif area from the composite trapezoidal rule',
  `area_percent_diff` DOUBLE DEFAULT 0 COMMENT 'the percent difference of the motif_area from the fp_motif_area',
/* @added 202210428 - fps_checked and runtime */
  `fps_checked` INT(11) DEFAULT 0 COMMENT 'the number of features profiles checked',
  `runtime` DOUBLE DEFAULT 0 COMMENT 'the inference runtime',
  `type_id` INT(11) DEFAULT 0 COMMENT 'the match type id',
  `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid',
  `user_id` INT DEFAULT NULL COMMENT 'the user id that validated the match',
  PRIMARY KEY (id),
  INDEX `inference_motifs_matched` (`id`,`metric_id`,`fp_id`,`metric_timestamp`,`primary_match`,`validated`))
  ENGINE=InnoDB;

/*
# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Store the not anomalous motifs
*/
CREATE TABLE IF NOT EXISTS `not_anomalous_motifs` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'motif element unique id',
  `motif_id` INT(11) NOT NULL COMMENT 'motif id',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'motif element unix timestamp',
  `value` DECIMAL(65,6) NOT NULL COMMENT 'motif element value',
  PRIMARY KEY (id),
  INDEX `inference_not_anomalous_motifs` (`id`,`motif_id`))
  ENGINE=InnoDB;
/*
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014');
*/

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

/*
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-4164');
*/

/*
# @added 20210928 - Feature #4264: luminosity - cross_correlation_relationships
#                   Feature #4266: metric_group DB table
*/
CREATE TABLE IF NOT EXISTS `metric_group` (
  `metric_id` INT(11) NOT NULL COMMENT 'the metric id',
  `related_metric_id` INT(11) NOT NULL COMMENT 'related metric id',
  `avg_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average correlation coefficient',
  `shifted_counts` JSON DEFAULT NULL COMMENT 'shifted counts',
  `avg_shifted_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average shifted correlation coefficient',
  `avg_ppscore` DECIMAL(65,6) DEFAULT NULL COMMENT 'the average persceptive power score',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'unix timestamp this was added or updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_group` (`metric_id`,`related_metric_id`,`avg_coefficient`,`avg_shifted_coefficient`,`avg_ppscore`))
  ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `metric_group_info` (
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `related_metrics` INT(11) DEFAULT 0 COMMENT 'number of metrics related to this metric group',
  `last_updated` INT(10) NOT NULL COMMENT 'unix timestamp the group was last updated',
  `updated_count` INT(11) DEFAULT 0 COMMENT 'the number of times this group has been updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_groups` (`metric_id`,`last_updated`))
  ENGINE=InnoDB;

/*
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4264');
*/

/*
# TODO
# @added 20210814 - Feature #4232: ionosphere_shared features profiles
*/

INSERT INTO `sql_versions` (version) VALUES ('3.0.0');
