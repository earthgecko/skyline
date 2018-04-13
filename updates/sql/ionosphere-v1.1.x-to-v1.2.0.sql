/*
This the the SQL script to update Skyline from ionosphere (v1.1.2 - v1.1.3)
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
