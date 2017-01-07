==================================
Upgrading - Crucible to Ionosphere
==================================

This section covers the steps involved in upgrading an existing Skyline
implementation that is running on a Crucible branch version of Skyline (>= 1.0.0)

.. todo: Document all the upgrade steps

New settings.py variables
-------------------------

There is new dedicated Ionosphere settings section with the additions of all the
new Ionosphere variables.

Modified settings.py docstrings
-------------------------------

There are some changes in the ALERTS docstrings that cover pattern matching that
should be reviewed.

Clean up Skyline permissions
----------------------------

After restarting all your Skyline apps and verifying all is working, please
consider cleaning up any incorrect permissions that were set on the data
directories due to an octal bug that was introduced with the Crucible branch.

.. warning:: The below bash snippet needs the path to your Skyline directory and
  is based on all your the app directories being subdirectories of this parent
  directory, if your set up uses different directory paths for different apps,
  please modify the as below snippet as appropriate for your setup.

.. code-block:: bash

  # For example - YOUR_SKYLINE_DIR="/opt/skyline"
  YOUR_SKYLINE_DIR="<YOUR_SKYLINE_DIR>"

  ls -1 /opt/"$YOUR_SKYLINE_DIR"/ | grep "crucible\|ionosphere\|mirage\|panaroma" | while read i_dir
  do
    chmod 0755 "/opt/${YOUR_SKYLINE_DIR}/${i_dir}"
    find "/opt/${YOUR_SKYLINE_DIR}/${i_dir}" -type d -exec chmod 0755 {} \;
    find "/opt/${YOUR_SKYLINE_DIR}/${i_dir}" -type f -exec chmod 0644 {} \;
  done

Update your MySQL Skyline database
----------------------------------

- Backup your Skyline MySQL DB.
- Review and run the updates/sql/crucible_to_ionosphere.sql script against your
  database again.  There is one ALTER and a number of new tables.
