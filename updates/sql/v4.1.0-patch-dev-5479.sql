/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5479
*/

USE skyline;

/*
# @added 20241004 - Feature #5479: ionosphere.alias_features_profile
# Table to record alias fps
*/
CREATE TABLE IF NOT EXISTS `alias_features_profile` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'unique id of the alias',
  `metric_id` INT(11) NOT NULL COMMENT 'the aliased metric id',
  `original_metric_id` INT(11) NOT NULL COMMENT 'the original metric id',
  `fp_id` INT(11) NOT NULL COMMENT 'fp id',
  `enabled` tinyint(1) DEFAULT 1 COMMENT 'if the alias is enabled 1 or not enabled 0',
  `created_timestamp` INT(11) NOT NULL COMMENT 'unix timestamp of when the alias was created',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the fp alias',
  `user_id` INT DEFAULT 0 COMMENT 'the user id that created the fp alias',
  PRIMARY KEY (id),
  INDEX `alias_features_profile` (`id`,`metric_id`,`original_metric_id`,`fp_id`,`enabled`))
    ENGINE=InnoDB;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5479');
