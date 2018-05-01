/*
# This the the SQL script to update Skyline from crucible (v1.0.0 - v1.0.8) to
# Ionosphere v1.1.x
*/

USE skyline;

/*
# @added 20161207 - Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
#                   Task #1718: review.tsfresh
# This is the required SQL to update Skyline crucible (v1.0.0 to v1.0.8) to
# Ionosphere v1.1.x.
# Fix the timestamp int length from 11 to 10
*/
ALTER TABLE `anomalies` MODIFY `anomaly_timestamp` INT(10) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above';

CREATE TABLE IF NOT EXISTS `ionosphere` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere features profile unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
/*
# @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
# The timeseries full_duration needs to be recorded to allow Mirage metrics to
# be profiled on Redis timeseries data at FULL_DURATION
*/
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries on which the features profile was created',
/*
# @modified 20170120 - Feature #1854: Ionosphere learn - generations
# Added the anomaly_timestamp as the created_timestamp is different and in terms
# of the features profile data dir the anomaly_timestamp is required to be
# known.
*/
  `anomaly_timestamp` INT(10) DEFAULT 0 COMMENT 'anomaly unix timestamp, see notes on historic dates above',
  `enabled` tinyint(1) DEFAULT NULL COMMENT 'the features profile is enabled 1 or not enabled 0',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  `calc_time` FLOAT DEFAULT NULL COMMENT 'the time taken in seconds to calcalute the features',
  `features_count` INT(10) DEFAULT NULL COMMENT 'the number of features calculated',
  `features_sum` DOUBLE DEFAULT NULL COMMENT 'the sum of the features',
  `deleted` INT(10) DEFAULT NULL COMMENT 'the unix timestamp the features profile was deleted',
  `matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times this feature profile has been matched',
  `last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile (or other) was matched',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
/*
# @added 20161229 - Feature #1830: Ionosphere alerts
# Record times checked
*/
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile was checked',
  `checked_count` INT(10) DEFAULT 0 COMMENT 'the number of times this feature profile has been checked',
/*
# @added 20170110 - Feature #1854: Ionosphere learn - generations
# Added parent and generation for Ionosphere LEARN related features profiles
*/
  `parent_id` INT(10) DEFAULT 0 COMMENT 'the id of the parent features profile, 0 being the original human generated features profile',
  `generation` INT DEFAULT 0 COMMENT 'the number of generations between this feature profile and the original, 0 being the original human generated features profile',
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
/*
# @modified 20161224 - Task #1812: z_fp table type
# Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
# up, there can be LOTS of file_per_table z_fp_ tables/files without
# the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
# in the table cache as required.
#  INDEX `fp` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
*/
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
/*
# @modified 20161224 - Task #1812: z_fp table type
# Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
# up, there can be LOTS of file_per_table z_fp_ tables/files without
# the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
# in the table cache as required.
#  INDEX `metric` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
*/
  INDEX `metric` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255) ENGINE=InnoDB;

/*
# @added 20170107 - Feature #1844: ionosphere_matched DB table
#                   Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
# This table will allow for each not anomalous match that Ionosphere records to
# be reviewed.  It could get big and perhaps should be optional
*/
CREATE TABLE IF NOT EXISTS `ionosphere_matched` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere matched unique id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `metric_timestamp` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the not anomalous timeseries that matched the features profile',
/*
# @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
# Added details of match anomalies for verification added to tsfresh_version
*/
  `all_calc_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the calculated features of the not_anomalous timeseries',
  `all_calc_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the calculated features of the not_anomalous timeseries',
  `sum_common_values` DOUBLE DEFAULT 0 COMMENT 'the sum of the common calculated features of the not_anomalous timeseries',
  `common_features_count` INT(10) DEFAULT 0 COMMENT 'the number of the common calculated features of the not_anomalous timeseries',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  PRIMARY KEY (id),
  INDEX `features_profile_matched` (`id`,`fp_id`)  KEY_BLOCK_SIZE=255)
  ENGINE=InnoDB;

/*
# @added 20170113 - Feature #1854: Ionosphere learn - generations
# Added max_generations and percent_diff_from_origin for Ionosphere LEARN
# related features profiles
*/
ALTER TABLE `metrics` ADD COLUMN `learn_full_duration_days` INT DEFAULT 30 COMMENT 'Ionosphere learn - the number days data to be used for learning the metric' AFTER `created_timestamp`;
COMMIT;
ALTER TABLE `metrics` ADD COLUMN `learn_valid_ts_older_than` INT DEFAULT 3661 COMMENT 'Ionosphere learn - the age in seconds of a timeseries before it is valid to learn from' AFTER `learn_full_duration_days`;
COMMIT;
/*
# @modified 20170116 - Feature #1854: Ionosphere learn - generations
# Fix name typo
ALTER TABLE `metrics` ADD COLUMN `max_generations` INT DEFAULT 5 COMMENT 'Ionosphere learn - the maximum number of generations that can be learnt for this metric' AFTER `learn_valid_ts_older`;
*/
ALTER TABLE `metrics` ADD COLUMN `max_generations` INT DEFAULT 5 COMMENT 'Ionosphere learn - the maximum number of generations that can be learnt for this metric' AFTER `learn_valid_ts_older_than`;
COMMIT;
ALTER TABLE `metrics` ADD COLUMN `max_percent_diff_from_origin` DOUBLE DEFAULT 7.0 COMMENT 'Ionosphere learn - the maximum percentage difference that a learn features profile sum can be from the original human generated features profile' AFTER `max_generations`;
