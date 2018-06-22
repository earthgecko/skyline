/*
This the the SQL script to update Skyline from ionosphere (v1.2.0 - v1.2.1)
*/

USE skyline;

# @added 20180620 - Feature #2404: Ionosphere - fluid approximation
#                   Branch #2270: luminosity
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax` TINYINT(4) DEFAULT 0 COMMENT 'whether the match was made using minmax scaling, 0 being no' AFTER `tsfresh_version`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_fp_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the fp timeseries' AFTER `minmax`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_fp_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the fp timeseries' AFTER `minmax_fp_features_sum`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_anomalous_features_sum` DOUBLE DEFAULT 0 COMMENT 'the sum of all the minmax scaled calculated features of the anomalous timeseries' AFTER `minmax_fp_features_count`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `minmax_anomalous_features_count` INT(10) DEFAULT 0 COMMENT 'the number of all the minmax scaled calculated features of the anomalous timeseries' AFTER `minmax_anomalous_features_sum`;
COMMIT;
