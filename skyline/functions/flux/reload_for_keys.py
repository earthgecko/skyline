"""
reload_for_keys.py
"""
from time import time

from skyline_functions import get_redis_conn_decoded


# @added 20240318 - Feature #5308: flux - reload_for_keys
def reload_for_keys(caller):
    """
    There are times such as on boot where flux can load before the
    skyline.external_settings has been fully created with all the external API
    keys.  This function allows flux to be reloaded when a request with an
    invalid key is made.  The function ensures that there are only 3 reloads
    attempted per hour in a back off manner.

    rtype: reloading, reload_ttl

    :param caller: the calling context
    :type caller: str
    :return: {'reloading: boolean, 'reload_ttl': int, 'errors': list}
    :rtype: dict

    """

    def set_reload_key(expiry, last_key=None):
        flux_set_to_reload = False
        expiry_key_set = 0
        try:
            flux_set_to_reload = redis_conn_decoded.set('skyline.external_settings.update.metrics_manager', 'fluxd_reload_flux')
        except Exception as err:
            flux_set_to_reload = 'flux_set_reload, err: %s' % err
        if flux_set_to_reload:
            reload_expiry_key = 'flux.reload_for_keys.%s' % str(expiry)
            try:
                expiry_key_set = redis_conn_decoded.setex(reload_expiry_key, expiry, int(time()))
                if expiry_key_set:
                    expiry_key_set = expiry
            except Exception as err:
                expiry_key_set = 'expiry_key_set, err: %s' % err
            if last_key:
                try:
                    redis_conn_decoded.delete(last_key)
                except:
                    pass
        return flux_set_to_reload, expiry_key_set

    reloading = False
    reload_ttl = 0
    errors = []
    results = {'reloading': reloading, 'reload_ttl': reload_ttl, 'errors': errors}

    try:
        redis_conn_decoded = get_redis_conn_decoded('flux')
    except Exception as err:
        redis_conn_decoded = None
        errors.append(err)

    expiries_dict = {
        3600: 0,
        300: 3600,
        180: 300,
        60: 180,
    }

    reload_ttl = 0
    for expiry, next_expiry in expiries_dict.items():

        if reload_ttl > 0:
            break

        # Check if the expiry second reload key exists
        reload_expiry_key = 'flux.reload_for_keys.%s' % str(expiry)
        try:
            reload_ttl = redis_conn_decoded.ttl(reload_expiry_key)
        except Exception as err:
            errors.append(err)
            reload_ttl = 0
        # If the hour expiry exists just return
        if expiry == 3600 and reload_ttl > 0:
            results['reload_ttl'] = reload_ttl
            break
        if reload_ttl > 0 and reload_ttl < 60:
            try:
                flux_set_to_reload, reload_ttl = set_reload_key(next_expiry, last_key=reload_expiry_key)
            except Exception as err:
                errors.append(err)
            if isinstance(reload_ttl, int):
                results['reload_ttl'] = reload_ttl
            if isinstance(flux_set_to_reload, bool):
                if flux_set_to_reload:
                    results['reloading'] = flux_set_to_reload
                    break

    if not reload_ttl or reload_ttl == -2:
        # On the first call if no expiry key is present set the first 60 second key
        try:
            flux_set_to_reload, reload_ttl = set_reload_key(60)
            if isinstance(flux_set_to_reload, str):
                errors.append(flux_set_to_reload)
            if isinstance(reload_ttl, str):
                errors.append(reload_ttl)
            if isinstance(reload_ttl, int):
                results['reload_ttl'] = reload_ttl
            if isinstance(flux_set_to_reload, bool):
                if flux_set_to_reload:
                    results['reloading'] = flux_set_to_reload
        except Exception as err:
            errors.append(err)
    if errors:
        results['errors'] = errors
    return results
