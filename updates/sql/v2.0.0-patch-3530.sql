/*
This is the SQL script to update Skyline from v2.0.0 with SQL patch-3530
*/

USE skyline;

/*
# @added 20200505 - Feature #3530: Add new index on ionosphere table - validated
#                   Branch #3262: py3
# Update index to include the validated column
*/
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`,`validated`);
CREATE TABLE IF NOT EXISTS `sql_versions` (
  `version` VARCHAR(255) DEFAULT NULL COMMENT 'version',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp')
  ENGINE=InnoDB;
INSERT INTO `sql_versions` (version) VALUES ('2.0.0-patch-3530');
