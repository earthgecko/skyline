# This the the SQL script to update Skyline from crucible (v1.0.0 - v1.0.8) to
# Ionosphere v1.1.x

USE skyline;

# @added 20161207 - Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
#                   Task #1718: review.tsfresh
# This is the required SQL to update Skyline crucible (v1.0.0 to v1.0.8) to
# Ionosphere v1.1.x.
# Fix the timestamp int length from 11 to 10
ALTER TABLE `anomalies` MODIFY `anomaly_timestamp` INT(10) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above';

CREATE TABLE IF NOT EXISTS `ionosphere` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere features profile unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
# @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
# The timeseries full_duration needs to be recorded to allow Mirage metrics to
# be profiled on Redis timeseries data at FULL_DURATION
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries on which the features profile was created',
  `enabled` tinyint(1) DEFAULT NULL COMMENT 'the features profile is enabled 1 or not enabled 0',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  `calc_time` FLOAT DEFAULT NULL COMMENT 'the time taken in seconds to calcalute the features',
  `features_count` INT(10) DEFAULT NULL COMMENT 'the number of features calculated',
  `features_sum` DOUBLE DEFAULT NULL COMMENT 'the sum of the features',
  `deleted` INT(10) DEFAULT NULL COMMENT 'the unix timestamp the features profile was deleted',
  `matched_count` INT(11) DEFAULT NULL COMMENT 'the number of times this feature profile has been matched',
  `last_matched` INT(10) DEFAULT NULL COMMENT 'the unix timestamp of the last time this feature profile (or other) was matched',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `features_profile` (`id`,`metric_id`,`enabled`)  KEY_BLOCK_SIZE=255)
  ENGINE=InnoDB;

/*
This is an example features profiles metric table the Ionosphere generated
tables will be named z_fp_<metric_id>
*/
CREATE TABLE IF NOT EXISTS `z_fp_metricid` (
  `id` INT(11) NOT NULL COMMENT 'unique id',
  `fp_id` INT(11) NOT NULL COMMENT 'the features profile id',
  `feature_id` INT(5) NOT NULL COMMENT 'the id of the TSFRESH_FEATURES feature name',
  `value` DOUBLE DEFAULT NULL COMMENT 'the calculated value of the feature',
  PRIMARY KEY (id),
# @modified 20161224 - Task #1812: z_fp table type
# Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
# up, there can be LOTS of file_per_table z_fp_ tables/files without
# the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
# in the table cache as required.
#  INDEX `fp` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
  INDEX `fp` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=InnoDB;

/*
This is an example metric timeseries table the Ionosphere generated
tables will be named z_ts_<metric_id>
*/
CREATE TABLE IF NOT EXISTS `z_ts_metricid` (
  `id` INT(11) NOT NULL COMMENT 'timeseries entry unique id',
  `fp_id` INT(11) NOT NULL COMMENT 'the features profile id',
  `timestamp` INT(10) NOT NULL COMMENT 'a 10 digit unix epoch timestamp',
  `value` DOUBLE DEFAULT NULL COMMENT 'value',
  PRIMARY KEY (id),
# @modified 20161224 - Task #1812: z_fp table type
# Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
# up, there can be LOTS of file_per_table z_fp_ tables/files without
# the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
# in the table cache as required.
#  INDEX `metric` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
  INDEX `metric` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=InnoDB;
