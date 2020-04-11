/*
This is the SQL script to update Skyline from v2.0.0 with SQL patch-3478
*/

USE skyline;

/*
# @added 20200411 - Feature #3478: sql_versions table
#                   Branch #3262: py3
# Added a versions table to the DB as a method to track what version of the DB schema is being run.
# This eases tracking and skipping versions by know what DB updates need to be applied.
*/
CREATE TABLE IF NOT EXISTS `sql_versions` (
  `version` VARCHAR(255) DEFAULT NULL COMMENT 'version',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp')
  ENGINE=InnoDB;
INSERT INTO `sql_versions` (version) VALUES ('2.0.0-patch-3478');
