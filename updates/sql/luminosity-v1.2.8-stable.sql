/*
This the the SQL script to update Skyline to v1.2.8-stable-luminosity
*/

USE skyline;
# @added 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `approx_close` TINYINT(4) DEFAULT 0 COMMENT 'whether the match was made using approximately_close, 0 being no' AFTER `layers_checked`;
COMMIT;
