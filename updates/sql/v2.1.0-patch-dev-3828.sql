/*
This is a development SQL script to patch Skyline the initial v2.1.0 and bring
the DB up to date with inactive additions to the metrics table.
*/

USE skyline;

/*
# @added 20201104 - Feature #3828: Add inactive columns to the metrics DB table
# Add inactive and inactive_at columns and add inactive and ionosphere_enabled
# to the index
*/
ALTER TABLE `metrics` ADD COLUMN `inactive` TINYINT(1) DEFAULT 0 COMMENT 'inactive 1 or active 0';
ALTER TABLE `metrics` ADD COLUMN `inactive_at` INT(11) DEFAULT NULL COMMENT 'unix timestamp when declared inactive';
ALTER TABLE `metrics` DROP INDEX `metric`;
/* Add inactive_at AND ionosphere_enabled */
ALTER TABLE `metrics` ADD INDEX `metric` (`id`,`metric`,`ionosphere_enabled`,`inactive`);
INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-3828');
