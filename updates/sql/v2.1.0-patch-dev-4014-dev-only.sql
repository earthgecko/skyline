/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4014-dev-only
*/

USE skyline;

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

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014-dev-only');
