/*
This the the SQL script to update Skyline from ionosphere (v1.1.2 - v1.1.3)
*/

USE skyline;

/*
# @added 20170402 - Feature #2000: Ionosphere - validated
#                   Branch #922: ionosphere
*/
ALTER TABLE `ionosphere` ADD COLUMN `validated` INT(10) DEFAULT 0 COMMENT 'unix timestamp when validated, 0 being none' AFTER `generation`;
COMMIT;
ALTER TABLE `ionosphere_layers` ADD COLUMN `validated` INT(10) DEFAULT 0 COMMENT 'unix timestamp when validated, 0 being none' AFTER `label`;
COMMIT;
UPDATE ionosphere SET validated=1 WHERE generation < 2;
