==================================
Upgrading - Crucible to Ionosphere
==================================

This section covers the steps involved in upgrading an existing Skyline
implementation that is running on a Crucible branch version of Skyline (>= 1.0.0)

.. todo: Document all the upgrade steps


Clean up Skyline permissions
----------------------------

After restarting all your Skyline apps and verifying all is working, please
consider cleaning up any incorrect permissions that were set on the data
directories due to an octal bug that was introduced with the Crucible branch.

.. code-block:: bash

  ls -1 /opt/skyline/ | grep "crucible\|ionosphere\|mirage\|panaroma" | while read i_dir
  do
    chmod 0755 "/opt/skyline/${i_dir}"
    find "/opt/skyline/${i_dir}" -type d -exec chmod 0755 {} \;
    find "/opt/skyline/${i_dir}" -type f -exec chmod 0644 {} \;
  done
