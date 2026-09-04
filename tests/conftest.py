"""Shared test-session configuration.

Two named Hypothesis profiles are registered.  The ``default`` profile
stays untouched, so ordinary runs are unchanged.  The nightly workflow
selects ``HYPOTHESIS_PROFILE=nightly`` for the full property-testing
profile with more examples and no per-example deadline; the ``ci``
profile is available for local lane reproduction.
"""

from hypothesis import settings

settings.register_profile("ci", max_examples=25)
settings.register_profile("nightly", max_examples=250, deadline=None)
