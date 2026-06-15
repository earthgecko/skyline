/*
This is a SQL patch script for development only
*/

USE skyline;

/*
# @added 20240103 - Feature #4672: ionosphere_downsampled
#                   Task #5178: Build and test skyline v4.1.0
# Add resolution to ionosphere table
*/
ALTER TABLE `ionosphere` ADD COLUMN `resolution` SMALLINT DEFAULT 0 COMMENT 'the resolution of the features profile' AFTER `full_duration`;
COMMIT;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5178-4672');
