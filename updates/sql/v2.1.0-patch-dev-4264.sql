/*
This is a development SQL script to patch Skyline from v2.1.0-patch-dev-4264
*/

USE skyline;

/*
# @added 20210928 - Feature #4264: luminosity - cross_correlation_relationships
#                   Feature #4266: metric_group DB table
*/
CREATE TABLE IF NOT EXISTS `metric_group` (
  `metric_id` INT(11) NOT NULL COMMENT 'the metric id',
  `related_metric_id` INT(11) NOT NULL COMMENT 'related metric id',
  `avg_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average correlation coefficient',
  `shifted_counts` JSON DEFAULT NULL COMMENT 'shifted counts',
  `avg_shifted_coefficient` DECIMAL(6,5) DEFAULT NULL COMMENT 'average shifted correlation coefficient',
  `avg_ppscore` DECIMAL(65,6) DEFAULT NULL COMMENT 'the average persceptive power score',
  `timestamp` INT(10) DEFAULT 0 COMMENT 'unix timestamp this was added or updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_group` (`metric_id`,`related_metric_id`,`avg_coefficient`,`avg_shifted_coefficient`,`avg_ppscore`))
  ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `metric_group_info` (
  `metric_id` INT(11) NOT NULL COMMENT 'metric id',
  `related_metrics` INT(11) DEFAULT 0 COMMENT 'number of metrics related to this metric group',
  `last_updated` INT(10) NOT NULL COMMENT 'unix timestamp the group was last updated',
  `updated_count` INT(11) DEFAULT 0 COMMENT 'the number of times this group has been updated',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `metric_groups` (`metric_id`,`last_updated`))
  ENGINE=InnoDB;

INSERT INTO `sql_versions` (version) VALUES ('2.1.0-patch-dev-4264');
