"""
determine_smtp_server.py
"""
import settings


# @added 20220203 - Feature #4416: settings - additional SMTP_OPTS
def determine_smtp_server():
    """
    Return the SMTP server details dictionary

    :return: smtp_server
    :rtype: dict

    """
    smtp_server = {
        'host': '127.0.0.1',
        'port': 25,
        'ssl': False,
        'user': None,
        'password': None,
    }
    try:
        smtp_server['host'] = settings.SMTP_OPTS['smtp_server']['host']
    except KeyError:
        smtp_server['host'] = '127.0.0.1'
    except:
        smtp_server['host'] = '127.0.0.1'
    try:
        smtp_server['port'] = settings.SMTP_OPTS['smtp_server']['port']
    except KeyError:
        smtp_server['port'] = 25
    except:
        smtp_server['port'] = 25
    try:
        smtp_server['ssl'] = settings.SMTP_OPTS['smtp_server']['ssl']
    except KeyError:
        smtp_server['ssl'] = False
    except:
        smtp_server['ssl'] = False
    try:
        smtp_server['user'] = settings.SMTP_OPTS['smtp_server']['user']
    except KeyError:
        smtp_server['user'] = None
    except:
        smtp_server['user'] = None
    try:
        smtp_server['password'] = settings.SMTP_OPTS['smtp_server']['password']
    except KeyError:
        smtp_server['password'] = None
    except:
        smtp_server['password'] = None
    return smtp_server
