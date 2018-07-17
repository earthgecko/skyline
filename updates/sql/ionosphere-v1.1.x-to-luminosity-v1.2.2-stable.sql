/*
This the the SQL script to update Skyline from ionosphere (v1.1.3 - v1.2.0)
*/

USE skyline;

# @added 20180413 - Branch #2270: luminosity
CREATE TABLE IF NOT EXISTS `luminosity` (
  `id` INT(11) NOT NULL COMMENT 'anomaly id',
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `coefficient` DECIMAL(6,5) NOT NULL COMMENT 'correlation coefficient',
  `shifted` TINYINT NOT NULL COMMENT 'shifted',
  `shifted_coefficient` DECIMAL(6,5) NOT NULL COMMENT 'shifted correlation coefficient',
  INDEX `luminosity` (`id`,`metric_id`))
  ENGINE=InnoDB;

# @added 20180413 - Branch #2270: luminosity
# Changed shifted from TINYINT (-128, 0, 127, 255) to SMALLINT (-32768, 0, 32767, 65535)
# as shifted seconds change be higher than 255
ALTER TABLE `luminosity` MODIFY `shifted` SMALLINT

# @added 20180620 - Feature #2404: Ionosphere - fluid approximation
#                   Branch #2270: luminosity
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax` TINYINT(4) DEFAULT 0 COMMENT 'whether the match was made using minmax scaling, 0 being no' AFTER `tsfresh_version`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_fp_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the fp timeseries' AFTER `minmax`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_fp_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the fp timeseries' AFTER `minmax_fp_features_sum`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_anomalous_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the anomalous timeseries' AFTER `minmax_fp_features_count`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_anomalous_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the anomalous timeseries' AFTER `minmax_anomalous_features_sum`;
COMMIT;

# @added 2018075 - Task #2446: Optimize Ionosphere
#                  Branch #2270: luminosity
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_count` INT(10) DEFAULT 0 COMMENT 'the total number of features profiles for the metric which were valid to check' AFTER `minmax_anomalous_features_count`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_checked` INT(10) DEFAULT 0 COMMENT 'the number of features profiles checked until the match was made' AFTER `fp_count`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_count` INT(10) DEFAULT 0 COMMENT 'the total number of layers for the metric which were valid to check' AFTER `full_duration`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_checked` INT(10) DEFAULT 0 COMMENT 'the number of layers checked until the match was made' AFTER `layers_count`;
COMMIT;
