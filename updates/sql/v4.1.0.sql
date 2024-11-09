/*
This is the SQL script to update Skyline from v4.0.0 to v4.1.0
*/

USE skyline;

/*
# @added 20230831 - Feature #5038: snab_results_algorithms
# This table records results per algorithm run in a snab check. This is
# particularly useful where multiple algorithms are run in the algorithm run via
# the snab check
*/
CREATE TABLE IF NOT EXISTS `snab_results_algorithms` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'unique id',
  `snab_id` INT(11) NOT NULL COMMENT 'snab_id',
  `algorithm_group_id` INT(11) NOT NULL COMMENT 'algorithm group id',
  `algorithm_id` INT(11) NOT NULL COMMENT 'algorithm id',
  `anomalyScore` TINYINT(1) NOT NULL COMMENT 'whether the specific algorithm_id returned anomalous',
  `runtime` DECIMAL(11,6) DEFAULT NULL COMMENT 'runtime',
  `consensus_achieved` VARCHAR(255) DEFAULT NULL COMMENT 'a csv list of the consensus alogrithm ids e.g 1,2,4,6',
  PRIMARY KEY (id),
  INDEX `snab_id` (`algorithm_group_id`, `algorithm_id`,`anomalyScore`,`consensus_achieved`))
  ENGINE=InnoDB;

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

/*
# @added 20231228 - Feature #4708: ionosphere - store and cache fp minmax data
#                   Task #5178: Build and test skyline v4.1.0
# This was originally added on 20221026 and in-situ for
# Feature #4708: ionosphere - store and cache fp minmax data, it was included in
# the v4.0.0.sql, but it was not added to the skyline.sql for v4.0.0 so
# retrospectively added here in v4.1.0 for any instances built on v4.0.0 that
# used skyline.sql for the build and did not upgrade to v4.0.0
# Added table to store the results of minmax scaled features profiles.
*/
CREATE TABLE IF NOT EXISTS `ionosphere_minmax` (
  `fp_id` INT(11) NOT NULL COMMENT 'features profile id',
  `minmax_min` DOUBLE DEFAULT NULL COMMENT 'the min value in the minmax timeseries',
  `minmax_max` DOUBLE DEFAULT NULL COMMENT 'the max value in the minmax timeseries',
  `values_count` INT(10) DEFAULT NULL COMMENT 'the number of values in the fp timeseries',
  `features_count` INT(10) DEFAULT NULL COMMENT 'the number of features calculated',
  `features_sum` DOUBLE DEFAULT NULL COMMENT 'the sum of the features',
  `tsfresh_version` VARCHAR(12) DEFAULT NULL COMMENT 'the tsfresh version on which the features profile was calculated',
  `calc_time` FLOAT DEFAULT NULL COMMENT 'the time taken in seconds to calcalute the features',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (fp_id),
  INDEX `ionosphere_minmax` (`fp_id`,`tsfresh_version`))
  ENGINE=InnoDB;

/*
# @added 20240103 - Feature #4672: ionosphere_downsampled
#                   Task #5178: Build and test skyline v4.1.0
# Add resolution to ionosphere table
*/
ALTER TABLE `ionosphere` ADD COLUMN `resolution` SMALLINT DEFAULT 0 COMMENT 'the resolution of the features profile' AFTER `full_duration`;
COMMIT;

/* @added 20240610 - Feature #5370: anomalies_updated
#                    Feature #5352: vista - bigquery
#                    Feature #5372: vista - bq_update
# To record updates made to an anomalies
*/
CREATE TABLE IF NOT EXISTS `anomalies_updated` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'unique id of the update',
  `anomaly_id` INT(11) NOT NULL COMMENT 'anomaly unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `changed_timestamp` INT(11) NOT NULL COMMENT 'unix timestamp of when the anomaly was changed',
  `column` VARCHAR(64) DEFAULT NULL COMMENT 'the column that was update',
  `previous_value` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
  `new_value` DECIMAL(65,6) NOT NULL COMMENT 'anomalous datapoint',
  PRIMARY KEY (id),
  INDEX `anomalies_updated` (`id`,`anomaly_id`,`metric_id`,`changed_timestamp`, `column`))
    ENGINE=InnoDB;

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

/*
# @added 20241018 - Feature #5481: ionosphere.copy_features_profile
#                   Feature #5479: ionosphere.alias_features_profile
# Add the validated_user_id column (if it does not exist) and the alias_id
# column to the ionosphere table
*/
-- In updates/sql/v1.3.0.sql the validated_user_id was mistakenly left out but
-- added to the skyline.sql which has created a situation where some skyline DBs
-- have it and others do not.  Add the column if it does not exist
SET @table_name = 'ionosphere';
SET @column_name = 'validated_user_id';
SET @after_column = 'label';
SET @column_definition = CONCAT('INT DEFAULT 0 COMMENT \'the user id that validated the features profiles\' AFTER ', @after_column);
-- If the column does not exist add it
SET @sql = (
  SELECT IF(
    NOT EXISTS (
      SELECT *
      FROM information_schema.COLUMNS
      WHERE TABLE_NAME = @table_name
      AND COLUMN_NAME = @column_name
      AND TABLE_SCHEMA = DATABASE()
    ),
    CONCAT('ALTER TABLE ', @table_name, ' ADD COLUMN ', @column_name, ' ', @column_definition, ';'),
    'SELECT "Column already exists."'
  )
);
-- Execute the generated SQL statement
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

USE skyline;
ALTER TABLE `ionosphere` ADD COLUMN `alias_id` INT(11) DEFAULT 0 COMMENT 'the alias_id of the alias_features_profile if it has one' AFTER `validated_user_id`;
COMMIT;

/*
# @added 20241021 - Feature #5481: ionosphere.copy_features_profile
#                   Feature #5479: ionosphere.alias_features_profile
# Added alias_id to index
*/
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
COMMIT;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`,`validated`,`alias_id`);
COMMIT;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0');

