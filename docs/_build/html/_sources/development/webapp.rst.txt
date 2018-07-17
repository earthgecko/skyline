********************
Development - Webapp
********************

Flask
=====

The Skyline Webapp has arguably grown to the point were Flask may no longer
necessarily by the best choice for the frontend UI any more.  For a number of
reasons, such as:

* The frontend UI functionality is going to grow, with the addition of other
  things requiring more visualizations.
* A high-level Python Web framework like Django may be more appropriate in the
  long run.

The reasons for sticking with Flask at this point are:

* Because Flask is pretty cool.
* It is a microframework not a full blown web framework.
* It is probably simpler.
* Therefore, it keeps more focus at the "doing stuff" with Skyline, other and
  Python side of the equation for now, rather than at writing a new web UI with
  Django and porting the current stuff to Django.
* A fair bit of time has been spent adding new things with Flask.
* With gunicorn, Flask can.
* For now it is more than good enough.
* web development, one drop at a time
