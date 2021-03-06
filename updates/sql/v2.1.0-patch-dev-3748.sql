/*
This is a development SQL script to patch Skyline the initial v2.1.0 and bring
the DB up to date with SNAB additions
*/

USE skyline;

/*
# @added 20201007 - Task #3748: POC SNAB
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

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3748');
