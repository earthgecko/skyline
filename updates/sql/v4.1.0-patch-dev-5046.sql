/*
This is a SQL patch script fro development only
*/

USE skyline;

/*
# @added 20230811 - Feature #5046: comments
# This table records user comments for training_data, features_profile, etc.
*/
CREATE TABLE IF NOT EXISTS `comments` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'the metric id',
  `timestamp` INT(10) NOT NULL COMMENT 'unix timestamp of event',
  `user_id` INT NOT NULL COMMENT 'the user id that created the label',
  `anomaly_id` INT(11) DEFAULT NULL COMMENT 'anomaly id',
  `fp_id` INT(11) DEFAULT NULL COMMENT 'fp id',
  `match_id` INT(11) DEFAULT NULL COMMENT 'match id',
  `motif_match_id` INT(11) DEFAULT NULL COMMENT 'motif_match id',
  `snab_id` INT(11) DEFAULT NULL COMMENT 'snab id',
  `comment` TEXT DEFAULT NULL COMMENT 'the comment, max 65KB',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `comments` (`id`,`metric_id`,`timestamp`,`user_id`,`anomaly_id`,`fp_id`,`match_id`,`motif_match_id`,`snab_id`))
  ENGINE=InnoDB;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5046');
