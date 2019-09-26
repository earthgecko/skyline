###########
Development
###########

.. toctree::

  building-documentation
  dawn
  dawn-docker
  webapp
  debugging
  ionosphere
  tsfresh
  pytz

DRY
###

The current iteration of Skyline has a fair bit of repetition in some of the
functions, etc in each module.  The process of consolidating these is ongoing,
however due to the nature of some of the things, some things do need slightly
different implementations.

On going refactoring
####################

The code style in the Skyline codebase is somewhat organic, over time more and
changes have been made mostly in the following contexts.

- Logging trying to achieve consistency in logging string formats such as
	``error :: message``, etc.  Maybe there should be independent standard and
	error logs :)
- Quotation style, although there are not any hard and fast rules in this there
	has been an attempt to try and refactor any strings that were quoted with
	double quotes to single quoted strings.  This was more to do with try align
	the code so that double quotes can be reserved for strings that interpolate
	the variable in the string not via "%".  Although this is specifically
	Pythonic in a way, it can be and although not covered in any PEP seen, here
	have been some references to this being an acceptable and preferred style by
	some.  Seeing as this is common in other languages too, such a ruby and shell.
	It is not a rule :) and there are still instances of double quoted string in
	places, maybe.
- ``except:`` - Attempts are always being made to try and ``try:`` and
	``except:`` wherever possible with the goal being that all Skyline apps handle
	failure gracefully, log informatively, without logging vommiting through an
	entire ``assigned_metrics`` loop and exiting whenever a critical error is
	encountered at start up.
- Trying to achieve PEP8, wherever possible and not inconvenient.
- Adding any found Python-3 patterns discovered while doing something as long as
	it is wrapped in the appropriate conditional, it should have no effect on the
	functioning of the apps with Python2.7 and is there to test when Python3
	becomes desired.
