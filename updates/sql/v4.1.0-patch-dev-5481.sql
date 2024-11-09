/*
This is the SQL script to update Skyline to v4.1.0-patch-dev-5479
*/

/*
# @added 20241018 - Feature #5481: ionosphere.copy_features_profile
#                   Feature #5479: ionosphere.alias_features_profile
# Add the validated_user_id column (if it does not exist) and the alias_id
# column to the ionosphere table
*/
-- In updates/sql/v1.3.0.sql the validated_user_id was mistakenly left out but
-- added to the skyline.sql which has created a situation where some skyline DBs
-- have it and others do not.  Add the column if it does not exist
SET @table_name = 'ionosphere';
SET @column_name = 'validated_user_id';
SET @after_column = 'label';
SET @column_definition = CONCAT('INT DEFAULT 0 COMMENT \'the user id that validated the features profiles\' AFTER ', @after_column);
-- If the column does not exist add it
SET @sql = (
  SELECT IF(
    NOT EXISTS (
      SELECT *
      FROM information_schema.COLUMNS
      WHERE TABLE_NAME = @table_name
      AND COLUMN_NAME = @column_name
      AND TABLE_SCHEMA = DATABASE()
    ),
    CONCAT('ALTER TABLE ', @table_name, ' ADD COLUMN ', @column_name, ' ', @column_definition, ';'),
    'SELECT "Column already exists."'
  )
);
-- Execute the generated SQL statement
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

USE skyline;
ALTER TABLE `ionosphere` ADD COLUMN `alias_id` INT(11) DEFAULT 0 COMMENT 'the alias_id of the alias_features_profile if it has one' AFTER `validated_user_id`;
COMMIT;

/*
# @added 20241021 - Feature #5481: ionosphere.copy_features_profile
#                   Feature #5479: ionosphere.alias_features_profile
# Added alias_id to index
*/
ALTER TABLE `ionosphere` DROP INDEX `features_profile`;
COMMIT;
ALTER TABLE `ionosphere` ADD INDEX `features_profile` (`id`,`metric_id`,`enabled`,`layers_id`,`validated`,`alias_id`);
COMMIT;

INSERT INTO `sql_versions` (version) VALUES ('4.1.0-patch.dev.5481');
