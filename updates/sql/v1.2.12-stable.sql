/*
This is the SQL script to update Skyline from v1.2.9-v1.2.11 to v1.2.12
*/

USE skyline;

# @added 20190328 - Feature #2484: FULL_DURATION feature profiles
# ionosphere_echo
ALTER TABLE `ionosphere` ADD COLUMN `echo_fp` tinyint(1) DEFAULT 0 COMMENT 'an echo features profile, 1 being yes and 0 being no' AFTER `layers_id`;
COMMIT;
