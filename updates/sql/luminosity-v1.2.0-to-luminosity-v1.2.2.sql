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

# @added 2018075 - Task #2446: Optimize Ionosphere
#                  Branch #2270: luminosity
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_count` INT(10) DEFAULT 0 COMMENT 'the total number of features profiles for the metric which were valid to check' AFTER `minmax_anomalous_features_count`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_checked` INT(10) DEFAULT 0 COMMENT 'the number of features profiles checked until the match was made' AFTER `fp_count`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_count` INT(10) DEFAULT 0 COMMENT 'the total number of layers for the metric which were valid to check' AFTER `full_duration`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_checked` INT(10) DEFAULT 0 COMMENT 'the number of layers checked until the match was made' AFTER `layers_count`;
COMMIT;
