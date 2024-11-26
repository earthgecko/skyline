"""
slack functions

These are shared slack functions that are required in multiple modules.
"""
import os
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

# @added 20230605 - Feature #4932: mute_alerts_on
default_channel = 'general'
try:
    default_channel = settings.SLACK_OPTS['default_channel']
except:
    default_channel = 'general'

# @modified 20240729 - Feature #5418: slack_post_message - image_file parameter
# Added image_file parameter
def slack_post_message(current_skyline_app, channel, thread_ts, message, image_file=None):
    """
    Post a message to a slack channel or thread.

    :param current_skyline_app: the skyline app using this function
    :param channel: the slack channel
    :param thread_ts: the slack thread timestamp
    :param message: message
    :param image: the path and filename of an attach to attach to the slack post
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

    # @modified 20240729 - Feature #5418: slack_post_message - image parameter
    # Added image_file parameter
    use_slack_file_upload = False
    if image_file:
        if os.path.isfile(image_file):
            use_slack_file_upload = True
    if use_slack_file_upload:
        try:
            uploaded = slack_file_upload(current_skyline_app, channel, thread_ts, message, image_file=image_file)
            if uploaded:
                slack_post['ok'] = True
                slack_post['file_uploaded'] = True
                return slack_post
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: slack_post_message :: falied to connect slack, err: %s' % err)
            slack_post['file_uploaded'] = False
            return slack_post

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

    # @added 20230605 - Feature #4932: mute_alerts_on
    if not channel:
        channel = default_channel

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
        except Exception as err:
            # @added 20220422 - Bug #4448: Handle missing slack response
            # Handle slack SSL errors which occur more than one would expect
            if 'CERTIFICATE_VERIFY_FAILED' in str(err):
                slack_post = {'ok': False, 'slack_ssl_error': True}
                fail_msg = 'warning :: slack_post_message :: failed to post message to thread (slack SSL issue) - %s - %s - %s' % (
                    thread_ts, message, err)
                current_logger.info('%s' % fail_msg)
            else:
                current_logger.error(traceback.format_exc())
                current_logger.error(
                    'error :: slack_post_message :: failed to post message channel: %s, to thread %s - %s - %s' % (
                        str(channel), str(thread_ts), message, err))
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
        except Exception as err:
            # @added 20220422 - Bug #4448: Handle missing slack response
            # Handle slack SSL errors which occur more than one would expect
            if 'CERTIFICATE_VERIFY_FAILED' in str(err):
                slack_post = {'ok': False, 'slack_ssl_error': True}
                fail_msg = 'warning :: slack_post_message :: failed to post message to thread (slack SSL issue) - %s - %s - %s' % (
                    thread_ts, message, err)
                current_logger.info('%s' % fail_msg)
            else:
                current_logger.error(traceback.format_exc())
                current_logger.error(
                    'error :: slack_post_message :: failed to post message channel: %s, to thread %s - %s - %s' % (
                        str(channel), str(thread_ts), message, err))
            # @modified 20200826 - Bug #3710: Gracefully handle slack failures
            # return False
            return slack_post

    if slack_post['ok']:
        current_logger.info(
            'slack_post_message :: posted message to channel %s, thread %s - %s' % (
                channel, str(thread_ts), message))
    else:
        current_logger.error(
            'error :: slack_post_message :: failed to post message to channel %s, thread %s - %s' % (
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
            'error :: slack_post_message :: failed to connect slack')
        # @modified 20200826 - Bug #3710: Gracefully handle slack failures
        # return False
        return slack_response

    # slack_response = None

    # @added 20220328 - Bug #4448: Handle missing slack response
    # Handle slack SSL errors which occur more than one would expect
    slack_ssl_error = False

    # @added 20230605 - Feature #4932: mute_alerts_on
    if not channel:
        channel = default_channel

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
    except Exception as err:
        # @added 20200826 - Bug #3710: Gracefully handle slack failures
        # Handle already_reacted
        if 'already_reacted' in str(err):
            current_logger.info(
                'slack_post_reaction :: post reaction to thread %s - %s - (already_reacted)' % (
                    thread_ts, emoji))
            slack_response['ok'] = True
        elif 'CERTIFICATE_VERIFY_FAILED' in str(err):
            slack_ssl_error = True

            # @added 20220422 - Bug #4448: Handle missing slack response
            # Handle slack SSL errors which occur more than one would expect
            slack_response = {'ok': False, 'slack_ssl_error': True}

            fail_msg = 'warning :: create_features_profile :: failed to slack_post_message - %s' % err
            current_logger.info('%s' % fail_msg)
        else:
            current_logger.error(traceback.format_exc())
            current_logger.info(
                'warning :: slack_post_reaction :: failed to post reaction to thread %s - %s - %s' % (
                    thread_ts, emoji, err))
            # @modified 20200826 - Bug #3710: Gracefully handle slack failures
            # return False
            return slack_response
    if not slack_response['ok'] and not slack_ssl_error:
        # @modified 20220214 - Bug #4448: Handle missing slack response
        # if str(slack_response['error']) == 'already_reacted':
        slack_response_error = None
        try:
            slack_response_error = slack_response['error']
        except KeyError:
            slack_response_error = slack_response['error']
            fail_msg = 'error :: create_features_profile :: no slack response'
            current_logger.error('%s' % fail_msg)
        if slack_response_error == 'already_reacted':
            current_logger.info(
                'slack_post_reaction :: already_reacted to channel %s, thread %s, ok' % (
                    channel, str(thread_ts)))
        else:
            current_logger.error(
                'error :: slack_post_reaction :: failed to post reaction to channel %s, thread %s - %s' % (
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


# @added 20240729 - Feature #5418: slack_post_message - image_file parameter
def slack_file_upload(current_skyline_app, channel, thread_ts, message, image_file=None):
    """
    Post a message with an image to a slack channel or thread.

    :param current_skyline_app: the skyline app using this function
    :param channel: the slack channel
    :param thread_ts: the slack thread timestamp
    :param message: message
    :param image_file: the path and filename of an attach to attach to the slack post
    :type current_skyline_app: str
    :type channel: str
    :type thread_ts: str or None
    :type message: str
    :return: slack response dict
    :rtype: dict

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    file_uploaded = False

    try:
        slack_thread_updates = settings.SLACK_OPTS['thread_updates']
    except:
        slack_thread_updates = False
    if not settings.SLACK_ENABLED:
        slack_thread_updates = False
    slack_file_upload = False
    slack_thread_ts = 0

    try:
        sc = WebClient(token, timeout=10)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: slack_file_upload :: falied to connect slack')
        return file_uploaded

    if thread_ts:
        if thread_ts == 'None':
            thread_ts = None

    if not channel:
        channel = default_channel

    try:
        # slack does not allow embedded images, nor links behind authentication
        # or color text, so we have jump through all the API hoops to end up
        # having to upload an image with a very basic message.
        if os.path.isfile(image_file):
            filename = os.path.basename(image_file)
            slack_file_upload = sc.files_upload(
                filename=filename, channels=channel,
                initial_comment=message, file=open(image_file, 'rb'))
            if not slack_file_upload['ok']:
                current_logger.error('error :: slack_file_upload :: failed to send slack message')
            else:
                current_logger.info('slack_file_upload :: sent slack message')
                file_uploaded = True
                if slack_thread_updates:
                    # The sc.api_call 'files.upload' response which generates
                    # slack_file_upload has a different structure depending
                    # on whether a channel is private or public.  That also
                    # goes for free or hosted slack too.
                    slack_group = None
                    slack_group_list = None

                    # This is basically the channel id of your channel, the
                    # name could be used so that if in future it is used or
                    # displayed in a UI
                    # This block only works for free slack workspace private
                    # channels.  Although this should be handled in the
                    # SLACK_OPTS as slack_account_type: 'free|hosted' and
                    # default_channel_type = 'private|public', it is going
                    # to be handled in the code for the time being so as not
                    # to inconvience users to update their settings.py for
                    # v.1.2.17 # TODO next release with settings change add
                    # these.
                    slack_group = None
                    slack_group_trace_groups = None
                    slack_group_trace_channels = None
                    try:
                        slack_group = slack_file_upload['file']['groups'][0]
                        current_logger.info('slack_file_upload :: slack group has been set from \'groups\' as %s' % (
                            str(slack_group)))
                        slack_group_list = slack_file_upload['file']['shares']['private'][slack_group]
                        slack_thread_ts = slack_group_list[0]['ts'].encode('utf-8')
                        current_logger.info('slack_file_upload :: slack group is %s and the slack_thread_ts is %s' % (
                            str(slack_group), str(slack_thread_ts)))
                    except:
                        slack_group_trace_groups = traceback.format_exc()
                        current_logger.info('slack_file_upload :: failed to determine slack_group using groups')
                    if not slack_group:
                        # Try by channel
                        try:
                            slack_group = slack_file_upload['file']['channels'][0]
                            current_logger.info('slack_file_upload :: slack group has been set from \'channels\' as %s' % (
                                str(slack_group)))
                        except:
                            slack_group_trace_channels = traceback.format_exc()
                            current_logger.info('slack_file_upload :: failed to determine slack_group using channels')
                            current_logger.error('error :: slack_file_upload :: failed to determine slack_group using groups or channels')
                            current_logger.error('error :: slack_file_upload :: traceback from slack_group_trace_groups follows:')
                            current_logger.error(str(slack_group_trace_groups))
                            current_logger.error('error :: slack_file_upload :: traceback from slack_group_trace_channels follows:')
                            current_logger.error(str(slack_group_trace_channels))
                            current_logger.error('error :: slack_file_upload :: faied to determine slack_thread_ts')
                    slack_group_list = None
                    if slack_group:
                        slack_group_list_trace_private = None
                        slack_group_list_trace_public = None
                        # Try private channel
                        try:
                            slack_group_list = slack_file_upload['file']['shares']['private'][slack_group]
                            current_logger.info('slack_file_upload :: slack_group_list determined from private channel and slack_group %s' % (
                                str(slack_group)))
                        except:
                            slack_group_list_trace_private = traceback.format_exc()
                            current_logger.info('slack_file_upload :: failed to determine slack_group_list using private channel')
                        if not slack_group_list:
                            # Try public channel
                            try:
                                slack_group_list = slack_file_upload['file']['shares']['public'][slack_group]
                                current_logger.info('slack_file_upload :: slack_group_list determined from public channel and slack_group %s' % (
                                    str(slack_group)))
                            except:
                                slack_group_list_trace_public = traceback.format_exc()
                                current_logger.info('slack_file_upload :: failed to determine slack_group_list using public channel')
                                current_logger.info('slack_file_upload :: failed to determine slack_group_list using private or public channel')
                                current_logger.error('error :: slack_file_upload :: traceback from slack_group_list_trace_private follows:')
                                current_logger.error(str(slack_group_list_trace_private))
                                current_logger.error('error :: slack_file_upload :: traceback from slack_group_list_trace_public follows:')
                                current_logger.error(str(slack_group_list_trace_public))
                                current_logger.error('error :: slack_file_upload :: faied to determine slack_thread_ts')
                    if slack_group_list:
                        try:
                            slack_thread_ts = slack_group_list[0]['ts']
                            current_logger.info('slack_file_upload :: slack group is %s and the slack_thread_ts is %s' % (
                                str(slack_group), str(slack_thread_ts)))
                            return file_uploaded
                        except:
                            current_logger.error(traceback.format_exc())
                            current_logger.info('slack_file_upload :: failed to determine slack_thread_ts')
                            return file_uploaded
        else:
            send_text = message + '  ::  error :: there was no image to upload'
            send_message = sc.api_call(
                'chat.postMessage',
                channel=channel,
                icon_emoji=icon_emoji,
                text=send_text)
            if not send_message['ok']:
                current_logger.error('error :: slack_file_upload :: failed to send slack message')
                return False
            else:
                current_logger.info('slack_file_upload :: sent slack message')
                return True

    except:
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: slack_file_upload :: could not upload file')
        return False

