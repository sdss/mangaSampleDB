#!/usr/bin/env python3
# encoding: utf-8
#
# connection.py
#
# Created by José Sánchez-Gallego on Aug 13, 2016.


from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from sqlalchemy import create_engine


__all__ = ('create_connection')


def create_connection(db_name='manga', username='', password='',
                      host='localhost', port=5432):
    """Creates a connection to the DB and returns the engine."""

    connection_parameters = {'username': username, 'password': password,
                             'host': host, 'port': port, 'db_name': db_name}

    engine = create_engine(
        'postgresql://{username:s}:{password:s}@{host:s}:{port:d}/{db_name:s}'
        .format(**connection_parameters))

    return engine
