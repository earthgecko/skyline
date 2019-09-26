/*
This is the SQL script to update Skyline from ionosphere (v1.2.17 to v1.3.0)
*/

USE skyline;

# @added 20190601 - Feature #3084: Ionosphere - validated matches
ALTER TABLE `ionosphere_matched` ADD COLUMN `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid' AFTER `fp_checked`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `validated` TINYINT(4) DEFAULT 0 COMMENT 'whether the match has been validated, 0 being no, 1 validated, 2 invalid' AFTER `approx_close`;
COMMIT;

# @added 20190919 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
CREATE TABLE IF NOT EXISTS `users` (
  `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT 'user id',
  `user` VARCHAR(255) DEFAULT NULL COMMENT 'user name',
  `description` VARCHAR(255) DEFAULT NULL COMMENT 'description',
  `created_timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created timestamp',
  INDEX `users` (`id`,`user`))
  ENGINE=InnoDB;
INSERT INTO `users` (user,description) VALUES ('Skyline','The default Skyline user');
INSERT INTO `users` (user,description) VALUES ('admin','The default admin user');
ALTER TABLE `ionosphere` ADD COLUMN `user_id` INT DEFAULT NULL COMMENT 'the user id that created the features profile' AFTER `echo_fp`;
ALTER TABLE `ionosphere` ADD COLUMN `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the features profile' AFTER `user_id`;
ALTER TABLE `anomalies` ADD COLUMN `label` VARCHAR(255) DEFAULT NULL COMMENT 'a label for the anomaly' AFTER `slack_thread_ts`;
ALTER TABLE `anomalies` ADD COLUMN `user_id` INT DEFAULT NULL COMMENT 'the user id that created the label' AFTER `label`;
ALTER TABLE `ionosphere_matched` ADD COLUMN `user_id` INT DEFAULT NULL COMMENT 'the user id that validated the match' AFTER `validated`;
ALTER TABLE `ionosphere_layers_matched` ADD COLUMN `user_id` INT DEFAULT NULL COMMENT 'the user id that validated the match' AFTER `validated`;
COMMIT;
UPDATE `ionosphere` SET user_id=1 WHERE generation > 0;
UPDATE `ionosphere` SET user_id=2 WHERE generation = 0;
UPDATE `ionosphere_matched` SET user_id=2 WHERE validated > 0;
UPDATE `ionosphere_layers_matched` SET user_id=2 WHERE validated > 0;
