.. role:: skyblue
.. role:: red
.. role:: brow

:red:`re`:brow:`brow`
=====================

:red:`re`:brow:`brow` was added to Skyline in the Ionosphere branch, however
Skyline uses a modified port of Marian Steinbach's excellent `rebrow`_ Redis
browser.  :red:`re`:brow:`brow` was developed and meant as a localhost
application only and by default is not secure, especially not to be run on any
publicly accessible server.

:red:`re`:brow:`brow` makes Skyline development AND having to administer
multiple Redis instances, very handy to have, it is very nice and easy to have
a web UI view of Redis.

Further, being able to easily view Skyline metric sets such as:

- analyzer.boring
- analyzer.too_short
- analyzer.stale

Is very useful to understanding your metric pipelines and often can be used to
inform you on any namespaces that are not spending metrics frequently, etc.
This is can be very useful at finding metrics that you thought you were sending
constantly, but apparently not.  Therefore in itself :red:`re`:brow:`brow` is
extremely useful in finding possible or occasional misconfiguration in some of
your metrics.  :red:`re`:brow:`brow` allows you to browse through and see your
data which makes it a make tool for discovering things about your metrics and
Redis.

For these reasons, Skyline runs a ported version of Marian Steinbach's rebrow
in the webapp itself, this ported version is modified in the following ways:

- The rebrow's pubsub functionality has not been ported as Skyline does not
  need it.
- The Webapp ported version requires authentication in Flask (http basic auth
  over SSL)
- It reads and converts msgpack formatted keys to a rebrowable, veiwable format.
- It adds the ability to authenticate with Redis, inspired by the elky84 PR
  https://github.com/marians/rebrow/pull/20 which adds very basic Redis auth to
  rebrow (not really fit for production but inspiring)
- To make Rebrow Redis authentication production suitable,  JWT encoding has
  been add to a encoded token instead of the password parameter that the
  elky84 PR describes.

The Skyline set up is aimed and building Skyline as reasonably hard as possible,
with the requirement for authentication and SSL encryption on the Skyline Webapp
frontend proxy (Apache or nginx) and the addition of a JWT encoded json payload
as the token, in combination with a client_id (simple md5 hash of client ip and
the client user agent) and a salt which is sent in a POST over a SSL encrypted
connection (but then does become a URL parameter), Skyline does manage to make
:red:`re`:brow:`brow` more suited to deployment in a non localhost environment

It could still be compromised if an attacker knew your IP, your user agent, the
token and the salt, and they had access to an authorized IP address and
authenticated against Skyline, then if they sent spoofed X-Forwarded-For and
User Agent headers, your token and the salt, then yes they could delete all your
Skyline Redis keys.

These further modifications to :red:`re`:brow:`brow` were required with the
introduction of Luminosity, which needs the ability to query multiple Redis
instances to run cross correlations for Skyline is deployed in multiple
locations.

.. _rebrow: https://github.com/marians/rebrow
