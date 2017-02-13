#!/usr/bin/env python3
# encoding: utf-8
#
# table_to_db.py
#
# Created by José Sánchez-Gallego on Aug 12, 2016.


from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

try:
    from cStringIO.StringIO import StringIO
except ImportError:
    from io import StringIO

import warnings

try:
    import progressbar
except ImportError:
    progressbar = False

import sqlalchemy as sql
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper, configure_mappers

from .connection import create_connection

import numpy as np


def _warning(message, category, *args, **kwargs):
    print('{0}: {1}'.format(category.__name__, message))


warnings.showwarning = _warning

__all__ = ('table_to_db')

_verbose = False


def print_verbose(text, level='INFO'):
    """Prints a log message if verbose is True."""

    if _verbose:
        print('{0}: {1}'.format(level.upper(), text))


def getPSQLtype(numpyType, colShape):
    """Returns the Postgresql type for a Numpy data type."""

    if numpyType == np.uint8:
        sqlType = sql.SmallInteger
    elif numpyType in [np.int16, np.int32, np.int64]:
        sqlType = sql.Integer
    elif numpyType in [np.float32, np.float64]:
        sqlType = sql.Float
    elif numpyType in [np.string_, np.str_]:
        sqlType = sql.String
    else:
        raise RuntimeError('the data type {0} cannot be converted to '
                           'PosgreSQL.'.format(numpyType))

    if len(colShape) == 1:
        return sqlType
    elif len(colShape) == 2:
        return postgresql.ARRAY(sqlType, dimensions=1)
    elif len(colShape) == 3:
        return postgresql.ARRAY(sqlType, dimensions=2)
    else:

        raise RuntimeError('arrays with dimensionality larger than 2 are not '
                           'currently supported.')


def table_to_db(table, db_name, schema, table_name, engine=None,
                connection_parameters=None, overwrite=False,
                chunk_size=20000, verbose=False):
    """Loads an Astropy table as a new table in a DB.

    Uses the COPY command in SQL to load an Astropy table efficiently into
    a new database table.

    Parameters:
        table (astropy.table object):
            The astropy table to load. The table must not contain a pk column.
        db_name (str):
            The name of the DB in which the table will be loaded.
        schema (str):
            The schema in ``db_name`` in which the table will be created.
        table_name (str):
            The name of the new table that will be created.
        engine (SQLAlchemy |engine|):
            A SQLAlchemy engine used for connection to the DB. if None,
            the default connection details, or alternatively
            ``connection_parameters``, will be used to create a new engine.
        connection_parameters (dict):
            A dictionary with the parameters to connect to the db. For example,
            ``database_parameters={'username': 'my_user',
            'password': 'my_pass', 'host': 'localhost', 'port': 5432}``.
        overwrite (bool):
            If the table already exists, drops it before recreating it.
        chunk_size (int):
            The frequency, in number of rows, for committing to the DB.
        verbose (bool):
            Controls the level of verbosity.

    Returns:
        NewTableModel:
            The class containing the SQLAlchemy mapping of the new table.

    Example:
        An example of use
          >>> import astropy.table
          >>> my_table = astropy.table.Table([('a', 'b'), (1, 2)],
            names=['letters', 'numbers'])
          >>> table_db = table_to_db(my_table, 'my_db',
            'schema1', 'my_new_table')

    .. |engine| replace:: Engine `<http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_

    """

    global _verbose
    _verbose = verbose

    # Creates the connection to the DB.
    print_verbose('Creating connection to DB.')
    engine = engine or create_connection(
        db_name, connection_parameters=connection_parameters)

    # Checks whether the table exists
    print_verbose('Checking if table {0} exists.'.format(table_name))
    if check_table_exists(engine, schema, table_name, drop=overwrite):
        raise ValueError('table {0} exists and overwrite=False'
                         .format(table_name))

    # Creates the new table
    print_verbose('Creating table {0}.'.format(table_name))
    NewTable = create_new_table(schema, table_name, table, engine)

    # Loads the data into the new table.
    print_verbose('Loading data ...')
    load_data(table, schema, table_name, engine, chunk_size=chunk_size)

    return NewTable


def check_table_exists(engine, schema, table_name, drop=False):
    """Returns True if a table exists. If ``drop=True``, drops the table."""

    inspector = Inspector.from_engine(engine)
    if table_name in inspector.get_table_names(schema=schema):
        if not drop:
            print_verbose('Table {0} exists. Not dropping it.'
                          .format(table_name))
            return True
        else:
            warnings.warn('table {0} exists but dropping it because drop=True.'
                          .format(table_name), UserWarning)
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute('DROP TABLE IF EXISTS {0}.{1} CASCADE;'
                           .format(schema, table_name))
            connection.commit()
            cursor.close()
            return False

    print_verbose('Table {0} does not exist in the database'
                  .format(table_name))
    return False


def create_new_table(schema, table_name, table_data, engine):
    """Creates a new empty table with the format of the table data.

    Return a model for the new table.

    """

    columns = [sql.Column('pk', sql.Integer, primary_key=True,
                          autoincrement=True)]

    for nn, colName in enumerate(table_data.colnames):
        dtype = table_data.columns[nn].dtype.type
        shape = table_data.columns[nn].shape
        sqlType = getPSQLtype(dtype, shape)
        columns.append(sql.Column(colName.lower(), sqlType))

    meta = sql.MetaData(schema=schema)
    newTable = sql.Table(table_name, meta, *columns)
    meta.create_all(engine)

    class NewTable(object):
        __table__ = newTable

    mapper(NewTable, newTable)
    configure_mappers()

    return NewTable


def load_data(table, schema, table_name, engine, chunk_size=10000):
    """Loads a table into a DB table using COPY."""

    connection = engine.raw_connection()
    cursor = connection.cursor()

    # If the progressbar package is installed, uses it to create a progress bar
    if progressbar:
        bar = progressbar.ProgressBar()
        iterable = bar(range(len(table)))
    else:
        iterable = range(len(table))

    chunk = 0
    tmp_list = []
    for ii in iterable:

        row = table[ii]

        # Adds the pk
        row_data = [str(ii + 1)]

        for col_value in row:
            if np.isscalar(col_value):
                row_data.append(str(col_value))
            else:
                row_data.append(
                    str(col_value.tolist())
                    .replace('\n', '')
                    .replace('[', '{').replace(']', '}'))

        tmp_list.append('\t'.join(row_data))
        chunk += 1

        # If we have reached a chunk commit point, or this is the last item,
        # copy and commits to the database.
        last_item = ii == len(table) - 1
        if chunk == chunk_size or (last_item and len(tmp_list) > 0):
            ss = StringIO('\n'.join(tmp_list))
            cursor.copy_from(ss, '{0}.{1}'.format(schema, table_name))
            connection.commit()
            tmp_list = []
            chunk = 0

    cursor.close()

    return
