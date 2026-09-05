# wsgi.py – WSGI entry-point
#
# Render / Gunicorn:  gunicorn wsgi:application
# PythoAnywhere:      point WSGI config at this file, set enable-threads = true
#
from app import app as application  # noqa: F401
