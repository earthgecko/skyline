"""
Test class for Skyline all imports
"""

import sys
import os.path
import os
import re
import mock
import traceback

python_version = int(sys.version_info[0])

if python_version == 2:
    from mock import Mock as MagicMock
if python_version == 3:
    from unittest.mock import MagicMock

current_dir = os.path.dirname(os.path.realpath(__file__))
print ('current_dir: %s' % current_dir)
parent_dir = os.path.join(os.path.dirname(os.path.realpath(current_dir)))
print ('parent_dir: %s' % parent_dir)
skyline_dir = parent_dir + '/skyline'
print ('skyline_dir: %s' % skyline_dir)
sys.path.append(skyline_dir)


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
            return Mock()

MOCK_MODULES = ['pygerduty', 'python-simple-hipchat']
if python_version == 2:
    sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)
if python_version == 3:
    sys.modules.update((mod_name, MagicMock()) for mod_name in MOCK_MODULES)

import settings

print (settings.ALGORITHMS)

skyline_settings_file = skyline_dir + '/settings.py'

skyline_apps = ['analyzer', 'horizon', 'webapp', 'mirage', 'boundary',
                'crucible', 'panorama']

for skyline_app in skyline_apps:

    skyline_dir_app = parent_dir + '/skyline/' + skyline_app
    sys.path.append(skyline_dir_app)

    skyline_python_files = []

    for path, dirs, files in os.walk(skyline_dir_app):
        for filename in files:
            pattern = re.compile("^.*\.py$")
            filename_match = pattern.match(str(filename))
            if filename_match:
                python_file = os.path.join(path, filename)
                skyline_python_files.append(python_file)

    import_tmp_file = '%s/%s_imports.py' % (current_dir, skyline_app)
    import_tmp_file_pyc = '%s/%s_imports.pyc' % (current_dir, skyline_app)
    if os.path.exists(import_tmp_file):
        os.remove(import_tmp_file)

    with open(import_tmp_file, 'w') as f:
        pass

    first_line = 'print ("Testing %s imports")\n' % skyline_app
    with open(import_tmp_file, 'w') as f:
        f.write(first_line)

    traceback_line = 'import traceback\n'
    with open(import_tmp_file, 'a') as f:
        f.write(traceback_line)

    skyline_app_class = skyline_app.title()
    app_import_line = 'from %s import %s' % (skyline_app, skyline_app_class)

    for skyline_python_file in skyline_python_files:
        if str(skyline_python_file) == skyline_settings_file:
            continue

        try_line = 'try:\n'
        except_line = 'except:\n'
        with open(skyline_python_file, 'r') as f:
            for line in f:
                import_line = str(line).lstrip()
                if 'import ' in import_line:
                    if 'from settings ' in import_line:
                        continue
                    elif 'import settings' in import_line:
                        continue
                    elif app_import_line in import_line:
                        continue
                    else:
                        if import_line.startswith(('from ', 'import ')):
                            try_import_line = '    ' + str(import_line)
                            except_import_line1 = '    print(traceback.format_exc())\n'
                            except_import_line2 = '    print ("Failed to import")\n'
                            with open(import_tmp_file, 'a') as f:
                                f.write(try_line)
                                f.write(try_import_line)
                                f.write(except_line)
                                f.write(except_import_line1)
                                f.write(except_import_line2)

    if skyline_app in 'analyzer mirage boundary crucible':
        dir_line = 'sys.path.append(\'%s\')\n' % skyline_dir_app
        try_app_import_line = '    %s\n' % app_import_line
        except_import_line1 = '    print(traceback.format_exc())\n'
        except_import_line2 = '    print ("Failed to import")\n'
        with open(import_tmp_file, 'a') as f:
            f.write('import sys\n')
            f.write(dir_line)
            f.write(try_line)
            f.write(try_app_import_line)
            f.write(except_line)
            f.write(except_import_line1)
            f.write(except_import_line2)

    try:
        if skyline_app == 'analyzer':
            import analyzer_imports
        if skyline_app == 'horizon':
            import horizon_imports
        if skyline_app == 'webapp':
            import webapp_imports
        if skyline_app == 'mirage':
            import mirage_imports
        if skyline_app == 'boundary':
            import boundary_imports
        if skyline_app == 'crucible':
            import crucible_imports
        if skyline_app == 'panorama':
            import panorama_imports
        if os.path.exists(import_tmp_file):
            os.remove(import_tmp_file)
        if os.path.exists(import_tmp_file_pyc):
            os.remove(import_tmp_file_pyc)
        print ('%s imports OK' % (skyline_app))
    except:
        print (traceback.format_exc())
        print ('Failed to import %s imports' % (skyline_app))
