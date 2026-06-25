#!/bin/sh
chown -R app:app /data
exec gosu app "$@"
