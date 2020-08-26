"""
slack functions

These are shared slack functions that are required in multiple modules.
"""
import logging
import traceback

import settings

# @modified 20200701 - Task #3612: Upgrade to slack v2
#                      Task #3608: Update Skyline to Python 3.8.3 and deps
#                      Task #3556: Update deps
# from slackclient import SlackClient
# slackclient v2 has a version function, < v2 does not
try:
    from slack import version as slackVersion
    slack_version = slackVersion.__version__
except:
    slack_version = '1.3'
if slack_version == '1.3':
    from slackclient import SlackClient
else:
    from slack import WebClient

token = settings.SLACK_OPTS['bot_user_oauth_access_token']
try:
    icon_emoji = settings.SLACK_OPTS['icon_emoji']
except:
    icon_emoji = ':chart_with_upwards_trend:'


def slack_post_message(current_skyline_app, channel, thread_ts, message):
    """
    Post a message to a slack channel or thread.

    :param current_skyline_app: the skyline app using this function
    :param channel: the slack channel
    :param thread_ts: the slack thread timestamp
    :param message: message
    :type current_skyline_app: str
    :type channel: str
    :type thread_ts: str or None
    :type message: str
    :return: slack response dict
    :rtype: dict

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # @added 20200826 - Bug #3710: Gracefully handle slack failures
    slack_post = {'ok': False}

    try:
        # @modified 20200701 - Task #3612: Upgrade to slack v2
        #                      Task #3608: Update Skyline to Python 3.8.3 and deps
        #                      Task #3556: Update deps
        if slack_version == '1.3':
            sc = SlackClient(token)
        else:
            sc = WebClient(token, timeout=10)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: slack_post_message :: falied to connect slack')
        # @modified 20200826 - Bug #3710: Gracefully handle slack failures
        # return False
        return slack_post

    if thread_ts:
        if thread_ts == 'None':
            thread_ts = None

    # slack_post = None

    # In terms of the generated Slack URLS for threads the
    # timestamps have no dots e.g.:
    # https://<an_org>.slack.com/archives/<a_channel>/p1543994173000700
    # However in terms of the sc.api_call the thread_ts
    # needs the format declared in the dict response e.g.
    # u'ts': u'1543994173.000700'}]} with the dot so in this
    # case '1543994173.000700'
    if thread_ts:
        try:
            # @modified 20200701 - Task #3612: Upgrade to slack v2
            if slack_version == '1.3':
                slack_post = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=message,
                    thread_ts=thread_ts
                )
            else:
                slack_post = sc.chat_postMessage(
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=message,
                    thread_ts=thread_ts
                )
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: slack_post_to_thread :: falied to post message to thread %s - %s' % (
                    thread_ts, message))
            # @modified 20200826 - Bug #3710: Gracefully handle slack failures
            # return False
            return slack_post
    else:
        try:
            # @modified 20200701 - Task #3612: Upgrade to slack v2
            if slack_version == '1.3':
                slack_post = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=message
                )
            else:
                slack_post = sc.chat_postMessage(
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=message
                )
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: slack_post_message :: falied to post message to thread %s - %s' % (
                    thread_ts, message))
            # @modified 20200826 - Bug #3710: Gracefully handle slack failures
            # return False
            return slack_post

    if slack_post['ok']:
        current_logger.info(
            'slack_post_message :: posted message to channel %s, thread %s - %s' % (
                channel, str(thread_ts), message))
    else:
        current_logger.error(
            'error :: slack_post_message :: falied to post message to channel %s, thread %s - %s' % (
                channel, str(thread_ts), message))
        current_logger.error(
            'error :: slack_post_message :: slack response dict follows')
        try:
            current_logger.error(str(slack_post))
        except:
            current_logger.error('error :: slack_post_message :: no slack response dict found')
        # @modified 20200826 - Bug #3710: Gracefully handle slack failures
        # return False
        return slack_post

    return slack_post


def slack_post_reaction(current_skyline_app, channel, thread_ts, emoji):
    """
    Post a message to a slack channel or thread.

    :param current_skyline_app: the skyline app using this function
    :param channel: the slack channel
    :param thread_ts: the slack thread timestamp
    :param emoji: emoji e.g. thumbsup
    :type current_skyline_app: str
    :type channel: str
    :type thread_ts: str
    :type emoji: str
    :return: slack response dict
    :rtype: dict

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # @added 20200826 - Bug #3710: Gracefully handle slack failures
    slack_response = {'ok': False}

    try:
        # @modified 20200701 - Task #3612: Upgrade to slack v2
        #                      Task #3608: Update Skyline to Python 3.8.3 and deps
        #                      Task #3556: Update deps
        # sc = SlackClient(token)
        if slack_version == '1.3':
            sc = SlackClient(token)
        else:
            sc = WebClient(token, timeout=10)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: slack_post_message :: falied to connect slack')
        # @modified 20200826 - Bug #3710: Gracefully handle slack failures
        # return False
        return slack_response

    # slack_response = None

    try:
        # @modified 20200701 - Task #3612: Upgrade to slack v2
        #                      Task #3608: Update Skyline to Python 3.8.3 and deps
        #                      Task #3556: Update deps
        if slack_version == '1.3':
            slack_response = sc.api_call(
                'reactions.add',
                channel=channel,
                name=emoji,
                timestamp=thread_ts
            )
        else:
            slack_response = sc.reactions_add(
                channel=channel,
                name=emoji,
                timestamp=thread_ts
            )
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: slack_post_reaction :: falied to post reaction to thread %s - %s' % (
                thread_ts, emoji))
        # @modified 20200826 - Bug #3710: Gracefully handle slack failures
        # return False
        return slack_response
    if not slack_response['ok']:
        if str(slack_response['error']) == 'already_reacted':
            current_logger.info(
                'slack_post_reaction :: already_reacted to channel %s, thread %s, ok' % (
                    channel, str(thread_ts)))
        else:
            current_logger.error(
                'error :: slack_post_reaction :: falied to post reaction to channel %s, thread %s - %s' % (
                    channel, str(thread_ts), emoji))
            current_logger.error(
                'error :: slack_post_reaction :: slack response dict follows')
            try:
                current_logger.error(str(slack_response))
            except:
                current_logger.error('error :: slack_post_reaction :: no slack response dict found')
            # @modified 20200826 - Bug #3710: Gracefully handle slack failures
            # return False
            return slack_response

    return slack_response
