CREATE SCHEMA IF NOT EXISTS `skyline` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

use skyline;

/*
NOTES:

- The MyISAM storage engine is used for the metadata type tables because it is
  a simpler structure and faster for data which is often queried and FULL TEXT
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

/*
# @modified 20220622 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Increase the column size to handle metric names and labels
  `metric` VARCHAR(255) NOT NULL COMMENT 'metric name',
*/
  `metric` VARCHAR(4096) NOT NULL COMMENT 'metric name',
/*
# @modified 20190501 - Task #2980: Change DB defaults from NULL
  `ionosphere_enabled` tinyint(1) DEFAULT NULL COMMENT 'are ionosphere rules enabled 1 or not enabled 0 on the metric',
*/
  `ionosphere_enabled` tinyint(1) DEFAULT 0 COMMENT 'are ionosphere rules enabled 1 or not enabled 0 on the metric',
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
/*
# @added 20201104 - Feature #3828: Add inactive columns to the metrics DB table
# Add inactive and inactive_at columns
*/
  `inactive` TINYINT(1) DEFAULT 0 COMMENT 'inactive 1 or active 0',
  `inactive_at` INT(11) DEFAULT NULL COMMENT 'unix timestamp when declared inactive',
  PRIMARY KEY (id),
/*
# @modified 20201104 - Feature #3828: Add inactive columns to the metrics DB table
# Added inactive and ionosphere_enabled to the index
  INDEX `metric` (`id`,`metric`)) ENGINE=MyISAM;
*/
/*
# @modified 20220622 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Increase the column size and change engine to handle metric names and labels
  INDEX `metric` (`id`,`metric`,`ionosphere_enabled`,`inactive`)) ENGINE=MyISAM;
*/
  INDEX `metric` (`id`,`metric`(255),`ionosphere_enabled`,`inactive`)) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `anomalies` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `host_id` INT(11) NOT NULL COMMENT 'host id',
  `app_id` INT(11) NOT NULL COMMENT 'app id',
  `source_id` INT(11) NOT NULL COMMENT 'source id',
  `anomaly_timestamp` INT(11) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates above',
/* @added 20191031 - Feature #3306: Record anomaly_end_timestamp
                     Branch #3262: py3
# Added anomaly_end_timestamp */
  `anomaly_end_timestamp` INT(11) DEFAULT NULL COMMENT 'end of the anomaly unix timestamp',
/* @modified 20181025 - Bug #2638: anomalies db table - anomalous_datapoint greater than DECIMAL
# Changed DECIMAL(18,6) to DECIMAL(65,6) for really large numbers
#  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
*/
  `anomalous_datapoint` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
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
/* @added 20190501 - Branch #2646: slack
# Add the slack thread timestamp
*/
  `slack_thread_ts` DECIMAL(17,6) DEFAULT 0 COMMENT 'the slack thread ts',
/*
# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
*/
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the anomaly',
  `user_id` INT DEFAULT 0 COMMENT 'the user id that created the label',
/* # @added 20200825 - Feature #3704: Add alert to anomalies */
  `alert` INT(11) DEFAULT NULL COMMENT 'if an alert was sent for the anomaly the timestamp it was sent',
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

/* @added 20191231 - Feature #3370: Add additional indices to DB
                     Branch #3262: py3
# For better query performance */
CREATE INDEX anomaly_timestamp ON anomalies (anomaly_timestamp);

/* # @added 20200825 - Feature #3704: Add alert to anomalies */
CREATE INDEX alert ON anomalies (alert);

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
/*
# @modified 20190501 - Task #2980: Change DB defaults from NULL
  `enabled` tinyint(1) DEFAULT NULL COMMENT 'the features profile is enabled 1 or not enabled 0',
*/
  `enabled` tinyint(1) DEFAULT 1 COMMENT 'the features profile is enabled 1 or not enabled 0',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  `calc_time` FLOAT DEFAULT NULL COMMENT 'the time taken in seconds to calcalute the features',
  `features_count` INT(10) DEFAULT NULL COMMENT 'the number of features calculated',
  `features_sum` DOUBLE DEFAULT NULL COMMENT 'the sum of the features',
  `deleted` INT(10) DEFAULT NULL COMMENT 'the unix timestamp the features profile was deleted',
  `matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times this feature profile has been matched',
  `last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile (or other) was matched',
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile was checked',
/*
# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Removed used column which was replace when the checked_count column was added
# below under #1830 and #2546, so check_count was there but never used.
  `check_count` INT(10) DEFAULT 0 COMMENT 'the number of times this feature profile has been checked',
*/
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
/*
# @added 20161229 - Feature #1830: Ionosphere alerts
# Record times checked
# @modified 20180821 - Bug #2546: Fix SQL errors
  `last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time this feature profile was checked',
*/
  `checked_count` INT(10) DEFAULT 0 COMMENT 'the number of times this feature profile has been checked',
/*
# @added 20170110 - Feature #1854: Ionosphere learn - generations
# Added parent and generation for Ionosphere LEARN related features profiles
*/
  `parent_id` INT(10) DEFAULT 0 COMMENT 'the id of the parent features profile, 0 being the original human generated features profile',
  `generation` INT DEFAULT 0 COMMENT 'the number of generations between this feature profile and the original, 0 being the original human generated features profile',
/*
# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# Added motif related columns
*/
  `motif_matched_count` INT(11) DEFAULT 0 COMMENT 'the number of times a motif from this feature profile has been matched',
  `motif_last_matched` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time a motif from this feature profile was matched',
  `motif_last_checked` INT(10) DEFAULT 0 COMMENT 'the unix timestamp of the last time a motif from this feature profile was checked',
  `motif_checked_count` INT(10) DEFAULT 0 COMMENT 'the number of times a motifs from this feature profile have been checked',
/*
# @added 20170402 - Feature #2000: Ionosphere - validated
*/
  `validated` INT(10) DEFAULT 0 COMMENT 'unix timestamp when validated, 0 being none',
/* # @added 20170305 - Feature #1960: ionosphere_layers */
  `layers_id` INT(11) DEFAULT 0 COMMENT 'the id of the ionosphere_layers profile, 0 being none',
/* # @added 20190328 - Feature #2484: FULL_DURATION feature profiles */
  `echo_fp` tinyint(1) DEFAULT 0 COMMENT 'an echo features profile, 1 being yes and 0 being no',
/*
# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
*/
  `user_id` INT DEFAULT 0 COMMENT 'the user id that created the features profile',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the features profile',
  `validated_user_id` INT DEFAULT 0 COMMENT 'the user id that validated the features profiles',
  PRIMARY KEY (id),
/*
# @modified 20180821 - Bug #2546: Fix SQL errors
  INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layer_id`))
*/
  INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`))
  ENGINE=InnoDB;

/* @added 20191231 - Feature #3370: Add additional indices to DB
                     Branch #3262: py3
# For better query performance */
CREATE INDEX metric_id ON ionosphere (metric_id);

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
/*
# @added 20180620 - Feature #2404: Ionosphere - fluid approximation
#                   Branch #2270: luminosity
*/
  `minmax` TINYINT(4) DEFAULT 0 COMMENT 'whether the match was made using minmax scaling, 0 being no',
  `minmax_fp_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the fp timeseries',
  `minmax_fp_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the fp timeseries',
  `minmax_anomalous_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the anomalous timeseries',
  `minmax_anomalous_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the anomalous timeseries',
/*
# @added 2018075 - Task #2446: Optimize Ionosphere
#                  Branch #2270: luminosity
Added fp_count and fp_checked
*/
  `fp_count` INT(10) DEFAULT 0 COMMENT 'the total number of features profiles for the metric which were valid to check',
  `fp_checked` INT(10) DEFAULT 0 COMMENT 'the number of features profiles checked until the match was made',
/*
# @added 20210412 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
*/
  `motifs_matched_id` INT DEFAULT NULL COMMENT 'the motif match id from the motifs_matched table, if this was a motif matched',
/*
# @added 20190601 - Feature #3084: Ionosphere - validated matches
*/
  `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid',
/*
# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
*/
  `user_id` INT DEFAULT NULL COMMENT 'the user id that validated the match',
  PRIMARY KEY (id),
/*
# @modified 20210201 - Feature #3934: ionosphere_performance
  INDEX `features_profile_matched` (`id`,`fp_id`))
*/
  INDEX `features_profile_matched` (`id`,`fp_id`,`metric_timestamp`))
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
/* @modified 20190501 - Bug #2638: anomalies db table - anomalous_datapoint greater than DECIMAL
# Changed DECIMAL(18,6) to DECIMAL(65,6) for really large numbers
#  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
*/
  `anomalous_datapoint` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries which matched',
/*
# @added 2018075 - Task #2446: Optimize Ionosphere
#                  Branch #2270: luminosity
Added layers_count and layers_checked
*/
  `layers_count` INT(10) DEFAULT 0 COMMENT 'the total number of layers for the metric which were valid to check',
  `layers_checked` INT(10) DEFAULT 0 COMMENT 'the number of layers checked until the match was made',
/*
# @added 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
*/
  `approx_close` TINYINT(4) DEFAULT 0 COMMENT 'whether the match was made using approximately_close, 0 being no',
/*
# @added 20190601 - Feature #3084: Ionosphere - validated matches
*/
  `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid',
/*
# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
*/
  `user_id` INT DEFAULT NULL COMMENT 'the user id that validated the match',
  PRIMARY KEY (id),
/*
# @modified 20210201 - Feature #3934: ionosphere_performance
  INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`))
*/
  INDEX `layers_matched` (`id`,`layer_id`,`fp_id`,`metric_id`,`anomaly_timestamp`))
  ENGINE=InnoDB;

# @added 20180413 - Branch #2270: luminosity
CREATE TABLE IF NOT EXISTS `luminosity` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `coefficient` DECIMAL(6,5) NOT NULL COMMENT 'correlation coefficient',
# @modified 20180413 - Branch #2270: luminosity
# Changed shifted from TINYINT (-128, 0, 127, 255) to SMALLINT (-32768, 0, 32767, 65535)
# as shifted seconds change be higher than 255
#  `shifted` TINYINT NOT NULL COMMENT 'shifted',
  `shifted` SMALLINT NOT NULL COMMENT 'shifted',
  `shifted_coefficient` DECIMAL(6,5) NOT NULL COMMENT 'shifted correlation coefficient',
  INDEX `luminosity` (`id`,`metric_id`))
  ENGINE=InnoDB;

/*
# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
*/
CREATE TABLE IF NOT EXISTS `users` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'user id',
  `user` VARCHAR(255) DEFAULT NULL COMMENT 'user name',
  `description` VARCHAR(255) DEFAULT NULL COMMENT 'description',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `users` (`id`,`user`))
  ENGINE=InnoDB;
INSERT INTO `users` (user,description) VALUES ('Skyline','The default Skyline user');
INSERT INTO `users` (user,description) VALUES ('admin','The default admin user');

/*
# @added 20200928 - Task #3748: POC SNAB
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


/*
# @added 20210308 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
*/
CREATE TABLE IF NOT EXISTS `anomaly_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly type id',
  `algorithm` VARCHAR(255) NOT NULL COMMENT 'algorithm name associated with type, e.g. adtk_level_shift`',
  `type` VARCHAR(255) NOT NULL COMMENT 'anomaly type name, e.g. levelshift`',
  PRIMARY KEY (id),
  INDEX `anomaly_type_id` (`id`,`type`)) ENGINE=InnoDB;
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_level_shift', 'level shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_volatility_shift', 'volatility shift');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_persist', 'persist');
INSERT INTO `anomaly_types` (`algorithm`,`type`) VALUES ('adtk_seasonal', 'seasonal');

CREATE TABLE IF NOT EXISTS `anomalies_type` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `type` VARCHAR(255) NOT NULL COMMENT 'a csv list of the anomaly_types ids e.g 1,2,3',
  INDEX `anomalies_type_id` (`id`,`metric_id`)) ENGINE=InnoDB;

/*
# @added 20210412 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
*/
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

/*
# @added 20210805 - Feature #4164: luminosity - cloudbursts
*/
CREATE TABLE IF NOT EXISTS `cloudburst` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'cloudburst unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric_id',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'cloudburst unix timestamp',
  `end` INT(10) DEFAULT 0 COMMENT 'cloudburst end unix timestamp',
  `duration` INT(11) NOT NULL COMMENT 'duration of the cloudburst',
  `from_timestamp` INT(10) DEFAULT 0 COMMENT 'cloudburst identified in period starting from unix timestamp',
  `resolution` INT(11) NOT NULL COMMENT 'cloudburst identified at a resolution of',
  `full_duration` INT(11) NOT NULL COMMENT 'cloudburst identified in a period of full_duration',
  `anomaly_id` INT(11) DEFAULT 0 COMMENT 'anomaly id of the anomaly associated with cloudburst',
  `match_id` INT(11) DEFAULT 0 COMMENT 'match id of the match associated with cloudburst',
  `fp_id` INT(11) DEFAULT 0 COMMENT 'fp id of the match associated with cloudburst',
  `layer_id` INT(11) DEFAULT 0 COMMENT 'layer id of the match associated with cloudburst',
  `added_at` INT(10) DEFAULT 0 COMMENT 'unix timestamp when the cloudburst was added',
  `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the cloudburt',
  PRIMARY KEY (id),
  INDEX `cloudburst` (`id`,`metric_id`,`timestamp`,`anomaly_id`,`match_id`))
  ENGINE=InnoDB;

/*
# @added 20210805 - Feature #4164: luminosity - cloudbursts
*/
CREATE TABLE IF NOT EXISTS `cloudbursts` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'cloudbursts unique id',
  `cloudburst_id` INT(11) NOT NULL COMMENT 'the cloudburst id that this is related too',
  `metric_id` INT(11) NOT NULL COMMENT 'metric_id of the metric related to the cloudburst',
  `ppscore_1` DECIMAL(65,6) DEFAULT 0 COMMENT 'the persceptive power 1 score',
  `ppscore_2` DECIMAL(65,6) DEFAULT 0 COMMENT 'the persceptive power 2 score',
  PRIMARY KEY (id),
  INDEX `cloudbursts` (`id`,`cloudburst_id`,`metric_id`))
  ENGINE=InnoDB;

/*
# @added 20210928 - Feature #4264: luminosity - cross_correlation_relationships
#                   Feature #4266: metric_group DB table
*/
CREATE TABLE IF NOT EXISTS `metric_group` (
  `metric_id` INT(11) NOT NULL COMMENT 'the metric id',
  `related_metric_id` INT(11) NOT NULL COMMENT 'related metric id',
  `avg_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average correlation coefficient',
  `shifted_counts` JSON DEFAULT NULL COMMENT 'shifted counts',
  `avg_shifted_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average shifted correlation coefficient',
  `avg_ppscore` DECIMAL(65,6) DEFAULT NULL COMMENT 'the average persceptive power score',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'unix timestamp this was added or updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_group` (`metric_id`,`related_metric_id`,`avg_coefficient`,`avg_shifted_coefficient`,`avg_ppscore`))
  ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `metric_group_info` (
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `related_metrics` INT(11) DEFAULT 0 COMMENT 'number of metrics related to this metric group',
  `last_updated` INT(10) NOT NULL COMMENT 'unix timestamp the group was last updated',
  `updated_count` INT(11) DEFAULT 0 COMMENT 'the number of times this group has been updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_groups` (`metric_id`,`last_updated`))
  ENGINE=InnoDB;

/*
# @added 20200411 - Feature #3478: sql_versions table
#                   Branch #3262: py3
# Added a versions table to the DB as a method to track what version of the DB schema is being run.
# This eases tracking and skipping versions by know what DB updates need to be applied.
# patch-3478
*/
CREATE TABLE IF NOT EXISTS `sql_versions` (
  `version` VARCHAR(255) DEFAULT NULL COMMENT 'version',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp')
  ENGINE=InnoDB;
/* INSERT INTO `sql_versions` (version) VALUES ('2.0.0'); */
/* INSERT INTO `sql_versions` (version) VALUES ('2.1.0'); */
/* INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3978-3642'); */
/* INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4014'); */
/* INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-4164'); */
/* INSERT INTO `sql_versions` (version) VALUES ('2.1.0'); */

INSERT INTO `sql_versions` (version) VALUES ('3.1.0');

/*
# mariadb
# https://mariadb.com/kb/en/mariadb/installing-mariadb-alongside-mysql/
# possible and possible to run side by side, fiddly but possible...
*/
