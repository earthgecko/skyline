/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4014
*/

USE skyline;

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

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014');
