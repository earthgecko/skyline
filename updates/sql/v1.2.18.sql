/*
This is the SQL script to update Skyline from ionosphere (v1.2.16 to v1.3.0)
*/

USE skyline;

# @added 20190601 - Feature #3084: Ionosphere - validated matches
ALTER TABLE `ionosphere_matched` ADD COLUMN `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid' AFTER `fp_checked`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid' AFTER `approx_close`;
