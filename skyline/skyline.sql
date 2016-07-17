CREATE SCHEMA `skyline` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
NOTES:

- The MyISAM storage engine is used for the metadata type tables because it is
  a simpler struucture and faster for data which is often queried and FULL TEXT
  searching.
- The InnoDB storage engine is used for the anomaly table - mostly writes.
- anomaly_timestamp - is limited by the extent of unix data, it does not suit
  old historical timestamp, e.g. 72 million years ago SN 2016coj went supernova,
  just a long term wiider consideration.

*/

CREATE TABLE `hosts` (
  `id` INT NOT NULL AUTO_INCREMENT COMMENT 'host unique id',
  `host` VARCHAR(255) NOT NULL COMMENT 'host name, e.g. skyline-prod-1',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `host_id` (`id`, `host`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;

CREATE TABLE `apps` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'app unique id',
  `app` VARCHAR(255) NOT NULL COMMENT 'app name, e.g. analyzer',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `app` (`id`,`app`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;

CREATE TABLE `algorithms` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'algorithm unique id',
  `algorithm` VARCHAR(255) NOT NULL COMMENT 'algorithm name, e.g. least_squares`',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `algorithm_id` (`id`,`algorithm`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;

CREATE TABLE `sources` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'source unique id',
  `source` VARCHAR(255) NOT NULL COMMENT 'name of the data source, e.g. graphite, Kepler, webcam',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (`id`),
  INDEX `app` (`id`,`source` ASC)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;

CREATE TABLE `metrics` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'metric unique id',
  `metric` VARCHAR(255) NOT NULL COMMENT 'metric name',
  `ionosphere_enabled` tinyint(1) DEFAULT NULL COMMENT 'are ionosphere rules enabled 1 or not enabled 0 on the metric',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `metric` (`id`,`metric`)  KEY_BLOCK_SIZE=255) ENGINE=MyISAM;

CREATE TABLE `anomalies` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'anomaly unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `host_id` INT(11) NOT NULL COMMENT 'host id',
  `app_id` INT(11) NOT NULL COMMENT 'app id',
  `source_id` INT(11) NOT NULL COMMENT 'source id',
  `anomaly_timestamp` INT(11) NOT NULL COMMENT 'anomaly unix timestamp, see notes on historic dates',
  `anomalous_datapoint` DECIMAL(18,6) NOT NULL COMMENT 'anomalous datapoint',
  `full_duration` INT(11) NOT NULL COMMENT 'The full duration of the timeseries in which the anomaly was detected, can be 0 if not relevant',
# store numeric array in mysql numpy
# http://stackoverflow.com/questions/7043158/insert-numpy-array-into-mysql-database
# for later, maybe image arrays...
# http://stackoverflow.com/questions/30713062/store-numpy-array-in-mysql
  `algorithms_run` VARCHAR(255) NOT NULL COMMENT 'a csv list of the alogrithm ids e.g 1,2,3,4,5,6,8,9',
  `triggered_algorithms` VARCHAR(255) NOT NULL COMMENT 'a csv list of the triggered alogrithm ids e.g 1,2,4,6,8,9',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
# Why index anomaly_timestamp and created_timestamp?  Because this is thinking
# wider than just realtime, e.g. analyze the Havard lightcurve plates, this
# being historical data, we may not know where in a historical set of metrics
# when the anomaly occured, but knowing roughly when the anomalies would have
# been created.
  INDEX `anomaly` (`id`,`metric_id`,`host_id`,`app_id`,`source_id`,`anomaly_timestamp`,
                   `full_duration`,`triggered_algorithms`,`created_timestamp`)  KEY_BLOCK_SIZE=255)
    ENGINE=InnoDB;

/*
CREATE TABLE `skyline`.`ionosphere` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'ionosphere rule unique id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `enabled` tinyint(1) DEFAULT NULL COMMENT 'rule is enabled 1 or not enabled 0',
  `ignore_less_than` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint less than value',
  `ignore_greater_than` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint greater than value',
  `ignore_step_change_percent` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with step change less than %',
  `ignore_step_change_value` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with step change less than value',
  `ignore_rate_change_value` INT(11) NULL DEFAULT NULL COMMENT 'ignore anomalous datapoint with rate change less than value',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  PRIMARY KEY (id),
  INDEX `metric_id` (`metric_id` ASC)) ENGINE=InnoDB;
*/

# mariadb
# https://mariadb.com/kb/en/mariadb/installing-mariadb-alongside-mysql/
# possible and possible to run side by side, fiddly but possible...
