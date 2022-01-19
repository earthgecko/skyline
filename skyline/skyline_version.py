"""
version info
"""
# @modified 20170109 - Feature #1854: Ionosphere learn
# Added learn
# __version_info__ = ('1', '1', '0')
# @modified 20210614 - Branch #1444: thunder
# __version_info__ = ('2', '1', '0')
# __version_info__ = ('2', '1', '0')
# @modified 20211109 - Bug #4308: matrixprofile - fN on big drops
# __version_info__ = ('2', '1', '0-4264')
# __branch__ = 'SNAB'
# __version_tag__ = 'cloudburst'
# __version_info__ = ('2', '1', '0-4309')
# __branch__ = 'SNAB'
# __version_tag__ = 'trigger_history_override'
# __version_info__ = ('2', '1', '0-4328')
# __branch__ = 'SNAB'
# __version_tag__ = 'BATCH_METRICS_CUSTOM_FULL_DURATIONS'
__version_info__ = ('2', '1', '0-4376')
__branch__ = 'SNAB'
__version_tag__ = 'update_external_settings'

__version__ = '.'.join(__version_info__)
# @modified 20190117 - release #2748: v1.2.11
# Changed version naming in terms release names from vx.y.z instead of of using
# vx.y.z-__version_tag__ e.g. now v1.2.11 instead of v1.2.11-stable
# __absolute_version__ = 'Skyline (%s v%s-%s)' % (__branch__, __version__, __version_tag__)
__absolute_version__ = 'Skyline (%s v%s %s)' % (__branch__, __version__, __version_tag__)

# __version__ = '0.5.0'
# __absolute_version__ = Skyline (crucible v0.5.0-beta)
