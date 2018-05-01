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
