/*
This the the SQL script to update Skyline from Luminosity (v1.2.1-beta - v1.2.1-stable)
*/

USE skyline;

# @added 20180715 - Task #2446: Optimize Ionosphere
#                   Branch #2270: luminosity
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_count` INT(10) DEFAULT 0 COMMENT 'the total number of features profiles for the metric which were valid to check' AFTER `minmax_anomalous_features_count`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `fp_checked` INT(10) DEFAULT 0 COMMENT 'the number of features profiles checked until the match was made' AFTER `fp_count`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_count` INT(10) DEFAULT 0 COMMENT 'the total number of layers for the metric which were valid to check' AFTER `full_duration`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `layers_checked` INT(10) DEFAULT 0 COMMENT 'the number of layers checked until the match was made' AFTER `layers_count`;
COMMIT;
