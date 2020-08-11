slack
=====

Skyline can be integrated with slack in terms of alerts and making automatic
updates to slack alert threads, informing you that a training data page has been
reviewed, a features profile has been created or learnt and validated.

To use slack with Skyline you need to create a slack skyline app and a bot user.

On slack:

- Create a Slack App
- Basic Information > Building Apps for Slack > Add features and functionality > Bots
- Bot User click Add a Bot User
- Set Display name and Default username to skyline
- And Save Changes
- In the left hand Settings menu for the skyline app, click Basic Information
- In App Credentials click Add features and functionality > Permissions
- Under OAuth Tokens & Redirect URLs click Install App to Workspace and click Authorize
- Copy the Bot User OAuth Access Token and add it to :mod:`settings.SLACK_OPTS` as the value for bot_user_oauth_access_token
- If you want to add the Skyline icon to the app, in the left hand Settings menu for the skyline app, click Basic Information
  Under Display Information > App icon & Preview and click and add:
  https://github.com/earthgecko/skyline/blob/master/docs/images/slack.skyline.app.icon.png
- Create a new channel called skyline and invite the skyline bot user.

Skyline slack settings
----------------------

In `settings.py` set

.. code-block:: python

  SLACK_ENABLED = True

And update the the `SLACK_OPTS` as appropriate for your metrics.
Note that even if you map multiple channels to a metric namespace, only ONE, the
first channel, will get slack updates to the thread.
Also note the channels are parsed in order and the first match will be the
channel/s were alerts are sent.

.. code-block:: python

  SLACK_OPTS = {
      # Bot User OAuth Access Token
      'bot_user_oauth_access_token': 'YOUR_slack_bot_user_oauth_access_token',
      # list of slack channels to notify about each anomaly
      # (similar to SMTP_OPTS['recipients'])
      # channel names - you can either pass the channel name (#general) or encoded
      # ID (C024BE91L)
      'channels': {
          'skyline': ('#skyline',),
          'skyline_test.alerters.test': ('#skyline',),
          'horizon.udp.test': ('#skyline'),
      },
      'icon_emoji': ':chart_with_upwards_trend:',
      # Your default slack Skyline channel name e.g. '#skyline'
      'default_channel': 'YOUR_default_slack_channel',
      # Your default slack Skyline channel id e.g. 'C0XXXXXX'
      'default_channel_id': 'YOUR_default_slack_channel_id',
      # Whether to update slack message threads on any of the below events
      'thread_updates': True,
      'message_on_training_data_viewed': True,
      'message_on_training_data_viewed_reaction_emoji': 'eyes',
      'message_on_features_profile_created': True,
      'message_on_features_profile_created_reaction_emoji': 'thumbsup',
      'message_on_features_profile_learnt': True,
      'message_on_features_profile_learnt_reaction_emoji': 'heavy_check_mark',
      'message_on_features_profile_disabled': True,
      'message_on_features_profile_disabled_reaction_emoji': 'x',
      'message_on_validated_features_profiles': True,
  }
