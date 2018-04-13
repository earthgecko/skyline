CREATE SCHEMA IF NOT EXISTS `skyline` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

use skyline;

/*
NOTES:

- The MyISAM storage engine is used for the metadata type tables because it is
  a simpler struucture and faster for data which is often queried and FULL TEXT
  searching.
- The InnoDB storage engine is used for the anomaly table - mostly writes.
- anomaly_timestamp - is limited by the extent of unix data, it does not suit
  old historical timestamp, e.g. 72 million years ago SN 2016coj went supernova,
  just a long term wiider consideration.
- z_fp_ tables are InnoDB tables and each metric that has a feature profile
  created has a z_fp_<metric_id> and a z_ts_<metric_id> table created, therefore
  if you have 10000 metrics and you created features profile for each one, there
  would be 20000 tables.  Bear in mind, that not all metrics will have features
  profiles created as YOU have to manually create each one.

*/

CREATE TABLE IF NOT EXISTS `hosts` (
  `id` INT NOT NULL AUTO_INCREMENT COMMENT 'host unique id',
  `host` VARCHAR(255) NOT NULL COMMENT 'host name, e.g. skyline-prod-1',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
# @modified 20180413 - Bug #2340: MySQL key_block_size
#                      MySQL key_block_size #45
# Removed KEY_BLOCK_SIZE=255 from all CREATE TABLE statements as under
# MySQL 5.7 breaks
#  INDEX `host_id` (`id`, `host`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
  INDEX `host_id` (`id`, `host`)) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `apps` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'app unique id',
  `app` VARCHAR(255) NOT NULL COMMENT 'app name, e.g. analyzer',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `app` (`id`,`app`)) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `algorithms` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'algorithm unique id',
  `algorithm` VARCHAR(255) NOT NULL COMMENT 'algorithm name, e.g. least_squares`',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `algorithm_id` (`id`,`algorithm`)) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `sources` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'source unique id',
  `source` VARCHAR(255) NOT NULL COMMENT 'name of the data source, e.g. graphite, Kepler, webcam',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (`id`),
  INDEX `app` (`id`,`source` ASC)) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `metrics` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'metric unique id',
  `metric` VARCHAR(255) NOT NULL COMMENT 'metric name',
  `ionosphere_enabled` tinyint(1) DEFAULT NULL COMMENT 'are ionosphere rules enabled 1 or not enabled 0 on the metric',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
/*
# @added 20170113 - Feature #1854: Ionosphere learn - generations
# Added max_generations and percent_diff_from_origin for Ionosphere LEARN
# related features profiles
*/
  `learn_full_duration_days` INT DEFAULT 30 COMMENT 'Ionosphere learn - the number days data to be used for learning the metric',
  `learn_valid_ts_older_than` INT DEFAULT 3661 COMMENT 'Ionosphere learn - the age in seconds of a timeseries before it is valid to learn from',
  `max_generations` INT DEFAULT 5 COMMENT 'Ionosphere learn - the maximum number of generations that can be learnt for this metric',
  `max_percent_diff_from_origin` DOUBLE DEFAULT 7.0 COMMENT 'Ionosphere learn - the maximum percentage difference that a learn features profile sum can be from the original human generated features profile',
  PRIMARY KEY (id),
  INDEX `metric` (`id`,`metric`)) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `anomalies` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `host_id` INT(11) NOT NULL COMMENT 'host id',
  `app_id` INT(11) NOT NULL COMMENT 'app id',
  `source_id` INT(11) NOT NULL COMMENT 'source id',
  `anomaly_timestamp` INT(11) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above',
  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries in which the anomaly was detected, can be 0 if not relevant',
/*
# store numeric array in mysql numpy
# http://stackoverflow.com/questions/7043158/insert-numpy-array-into-mysql-database
# for later, maybe image arrays...
# http://stackoverflow.com/questions/30713062/store-numpy-array-in-mysql
# @added 20161124 - Branch #922: ionosphere - added this note
# As long as the numpy array method does not violate the First Normal Form, e.g.
# numpy input is considered validated and numeric only?
# @added 20161207 - Branch #922: ionosphere - added this note
# Another way - http://acviana.github.io/posts/2014/numpy-arrays-and-sql/ - numpy float64 arrays
# BLOB could be used, would also suit storing msgpack timeseries too.
*/
  `algorithms_run` VARCHAR(255) NOT NULL COMMENT 'a csv list of the alogrithm ids e.g 1,2,3,4,5,6,8,9',
  `triggered_algorithms` VARCHAR(255) NOT NULL COMMENT 'a csv list of the triggered alogrithm ids e.g 1,2,4,6,8,9',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
/*
# Why index anomaly_timestamp and created_timestamp?  Because this is thinking
# wider than just realtime, e.g. analyze the Havard lightcurve plates, this
# being historical data, we may not know where in a historical set of metrics
# when the anomaly occured, but knowing roughly when the anomalies would have
# been created.
*/
  INDEX `anomaly` (`id`,`metric_id`,`host_id`,`app_id`,`source_id`,`anomaly_timestamp`,
                   `full_duration`,`triggered_algorithms`,`created_timestamp`))
    ENGINE=InnoDB;

/*
CREATE TABLE IF NOT EXISTS `skyline`.`ionosphere` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere rule unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `enabled` tinyint(1) DEFAULT NULL COMMENT 'rule is enabled 1 or not enabled 0',
# @modified 20161124 - Branch #922: ionosphere
#                      Task #1718: review.tsfresh
# Disabled all ignore_ patterns, related to early prototyping of 'simple' rules
# and primitive feature extraction, after tsfresh came along and over fulfilled
# the feature extraction requirement.
#  `ignore_less_than` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint less than value',
#  `ignore_greater_than` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint greater than value',
#  `ignore_step_change_percent` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with step change less than %',
#  `ignore_step_change_value` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with step change less than value',
#  `ignore_rate_change_value` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with rate change less than value',
# @added 20161124 - Branch #922: ionosphere
#                   Task #1718: review.tsfresh
# Store feature profile as an array?  What type?
# How many chars ... roughly...
# gary@mc11:/tmp$ cat /home/gary/admin/of/rm/task1718/test.data.formats/20161118/stats.statsd.bad_lines_seen/stats.statsd.bad_lines_seen.20161110.metric.ts.value.txt.features.transposed.csv | wc -c
# 10887
# gary@mc11:/tmp$
# No they are correct storing csv arays is not the way... it does violate the
# First Normal Form ... so we must revert to the tsfresh column wise logic in
# this case.  Even though the idea of a possible > 1400 column and LOTS of rows
# td.id data horrifies me... so be it.  Quite suitable to being listed or dict,
# that said... ALTER table ADD column within the app does not appeal to me.
# The numpy array storage mentioned above is still a possible valid method as it
# may not violate the First Normal Form
# hmmm features table?
#  `feature_profile_array` INT(11) NOT NULL COMMENT 'the number of times this feature profile (or other) has been matched',
  `matched_count` INT(11) NOT NULL COMMENT 'the number of times this feature profile (or other) has been matched',
  `last_matched` INT(11) NOT NULL COMMENT 'the timestamp of the last time this feature profile (or other) was matched unix timestamp',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `metric_id` (`metric_id` ASC)) ENGINE=InnoDB;
*/

/*
# @added 20161124 - Branch #922: ionosphere
#                   Task #1718: review.tsfresh
CREATE TABLE IF NOT EXISTS `feature_profile` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'feature profile unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
# shitlots of columns.... ??? How?? And what type?
  `value__symmetry_looking__r_0.65` int(11) DEFAULT NULL COMMENT 'are ionosphere rules enabled 1 or not enabled 0 on the metric',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `metric` (`id`,`metric`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
*/

/*
# @added 20161124 - Branch #922: ionosphere
#                   Task #1718: review.tsfresh
# @modified 20161130 - Branch #922: ionosphere
# Although MySQL can do 4 billion InnoDB tables, as Rick James pointed out on
# March 14, 2015
# > A table involves one to three files in a directory. A database involves one
# > directory in a parent directory. Having a "lot" of tables or databases can
# > be slow. This is because the Operating System does not necessarily do a
# > speedy job of locating files/directories when there are tens of thousands of
# > such in a single directory.
# He probably has a point.
# A timeseries table per metric is at least a little better, but these will be
# done by Python on features profile creations.  But we can make one and define
# the table here.
# CREATE TABLE IF NOT EXISTS `fp_timeseries` (
CREATE TABLE IF NOT EXISTS `ts_metricid` (
  `fp_id` INT(11) NOT NULL COMMENT 'feature profile unique id',
# store timeseries in numpy numeric array
# http://stackoverflow.com/questions/7043158/insert-numpy-array-into-mysql-database
# http://stackoverflow.com/questions/30713062/store-numpy-array-in-mysql
# As long as the numpy array method does not violate the First Normal Form, e.g.
# numpy input is considered validated and numeric only?
# @modified 20161130 - Branch #922: ionosphere
# Create a row per timestamp, value
#  `timeseries` VARCHAR(255) NOT NULL COMMENT 'a csv list of the alogrithm ids e.g 1,2,3,4,5,6,8,9',
  `timestamp` INT(10) NOT NULL COMMENT 'a 10 digit unix epoch timestamp',
  `value` INT(10) NOT NULL COMMENT 'a 10 digit unix epoch timestamp',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `metric` (`id`,`metric`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;
*/

/*
# @added 20161207 - Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
#                   Task #1718: review.tsfresh
# This is the required SQL to update Skyline crucible (v1.0.0 to v1.0.8) to
# Ionosphere v1.1.x.  It is idempotent, but replication IF NOT EXISTS caveats
# apply
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
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile was checked',
  `check_count` INT(10) DEFAULT 0 COMMENT 'the number of times this feature profile has been checked',
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
/*
# @added 20170402 - Feature #2000: Ionosphere - validated
*/
  `validated` INT(10) DEFAULT 0 COMMENT 'unix timestamp when validated, 0 being none',
/* # @added 20170305 - Feature #1960: ionosphere_layers */
  `layers_id` INT(11) DEFAULT 0 COMMENT 'the id of the ionosphere_layers profile, 0 being none',
  PRIMARY KEY (id),
  INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layer_id`))
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
  INDEX `fp` (`id`,`fp_id`)) ENGINE=InnoDB;

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
  INDEX `metric` (`id`,`fp_id`)) ENGINE=InnoDB;

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
# Added details of match anomalies for verification added up to the
# tsfresh_version column.
*/
  `all_calc_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the calculated features of the not_anomalous timeseries',
  `all_calc_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the calculated features of the not_anomalous timeseries',
  `sum_common_values` DOUBLE DEFAULT 0 COMMENT 'the sum of the common calculated features of the not_anomalous timeseries',
  `common_features_count` INT(10) DEFAULT 0 COMMENT 'the number of the common calculated features of the not_anomalous timeseries',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  PRIMARY KEY (id),
  INDEX `features_profile_matched` (`id`,`fp_id`))
  ENGINE=InnoDB;

/*
# @added 20170303 - Feature #1960: ionosphere_layers
#                   Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
*/
CREATE TABLE IF NOT EXISTS `ionosphere_layers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere matched unique id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id to which the layer is associated with',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `enabled` tinyint(1) DEFAULT 1 COMMENT 'the layer is enabled 1 or not enabled 0',
  `deleted` INT(10) DEFAULT NULL COMMENT 'the unix timestamp the layer was deleted',
  `matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times this layer has been matched',
  `last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this layer was matched',
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this layer was checked',
  `check_count` INT(10) DEFAULT 0 COMMENT 'the number of times this layer has been checked',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the layer',
/*
# @added 20170402 - Feature #2000: Ionosphere - validated
*/
  `validated` INT(10) DEFAULT 0 COMMENT 'unix timestamp when validated, 0 being none',
  PRIMARY KEY (id),
  INDEX `ionosphere_layers` (`id`,`fp_id`,`metric_id`))
  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `layers_algorithms` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'layer unique id',
  `layer_id` INT(11) NOT NULL COMMENT 'the id of the ionosphere_layers that the layer belongs to',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id to which the layer is associated with',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `layer` VARCHAR(2) DEFAULT NULL COMMENT 'the layer name, D, E, Es, F1 or F2',
  `type` VARCHAR(10) DEFAULT NULL COMMENT 'the layer type - value, time, day',
  `condition` VARCHAR(10) DEFAULT NULL COMMENT 'the condition - >, <, ==, !=, in, not in',
  `layer_boundary` VARCHAR(255) DEFAULT NULL COMMENT 'the value of the layer boundary, int or str',
  `times_in_row` INT DEFAULT NULL COMMENT 'number of times in a row',
  PRIMARY KEY (id),
  INDEX `layer_id` (`id`,`layer_id`,`fp_id`,`metric_id`))
  ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS `ionosphere_layers_matched` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere_layers matched unique id',
  `layer_id` INT(11) NOT NULL COMMENT 'layer id',
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `anomaly_timestamp` INT(11) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above',
  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries which matched',
  PRIMARY KEY (id),
  INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`))
  ENGINE=InnoDB;

# @added 20180413 - Branch #2270: luminosity
CREATE TABLE IF NOT EXISTS `luminosity` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `coefficient` DECIMAL(6,5) NOT NULL COMMENT 'correlation coefficient',
  `shifted` TINYINT NOT NULL COMMENT 'shifted',
  `shifted_coefficient` DECIMAL(6,5) NOT NULL COMMENT 'shifted correlation coefficient',
  INDEX `luminosity` (`id`,`metric_id`))
  ENGINE=InnoDB;

/*
# mariadb
# https://mariadb.com/kb/en/mariadb/installing-mariadb-alongside-mysql/
# possible and possible to run side by side, fiddly but possible...
*/
