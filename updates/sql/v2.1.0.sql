/*
This is the SQL script to update Skyline from v2.0.0 to v2.1.0
*/

USE skyline;

/* # @added 20200825 - Feature #3704: Add alert to anomalies */
ALTER TABLE `anomalies` ADD COLUMN `alert` INT(11) DEFAULT NULL COMMENT 'if an alert was sent for the anomaly the timestamp it was sent' AFTER `user_id`;
CREATE INDEX alert ON anomalies (alert);

/*
Although any SMTP only anomalis cannot be updated in terms of alert time, slack
ones can be
*/
UPDATE `anomalies` SET `alert` = `slack_thread_ts`;
