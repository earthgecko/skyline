/*
This is the SQL script is used to patch v3.1.0 to v3.1.0-patch-dev-4708
*/

USE skyline;

/*
# @added 20221026 - Feature #4708: ionosphere - store and cache fp minmax data
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

INSERT INTO `sql_versions` (version) VALUES ('3.1.0-patch-dev-4708');
