#!/usr/bin/env python
# encoding: utf-8
"""

catalogue.py

Created by José Sánchez-Gallego on 18 Feb 2016.
Licensed under a 3-clause BSD license.

Revision history:
    18 Feb 2016 J. Sánchez-Gallego
      Initial version

"""

from __future__ import division
from __future__ import print_function
import sqlalchemy as sql
from sqlalchemy.dialects import postgresql
from SDSSconnect import DatabaseConnection
from astropy import table
import numpy as np
import os
import sys


def getPSQLtype(numpyType, colShape):
    """Returns the Postgresql type for a Numpy data type."""

    if numpyType == np.uint8:
        sqlType = sql.SmallInteger
    elif numpyType in [np.int16, np.int32, np.int64]:
        sqlType = sql.Integer
    elif numpyType in [np.float32, np.float64]:
        sqlType = sql.Float
    elif numpyType == np.string_:
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


def _createNewTable(catName, catData, engine):
    """Creates a new empty table in mangaSampleDB."""

    columns = [sql.Column('pk', sql.Integer, primary_key=True,
                          autoincrement=True)]

    for nn, colName in enumerate(catData.colnames):
        dtype = catData.columns[nn].dtype.type
        shape = catData.columns[nn].shape
        sqlType = getPSQLtype(dtype, shape)
        columns.append(sql.Column(colName.lower(), sqlType))

    columns += [sql.Column('catalogue_pk', sql.SmallInteger, )]

    meta = sql.MetaData(schema='mangasampledb')
    newTable = sql.Table(catName, meta, *columns)
    newTable.create(engine)

    return newTable


# def _updateColumns(catname, catData):
#     """Checks if new columns need to be added to an existing table."""
#
#     try:
#         from migrate import changeset
#     except:
#         raise RuntimeError('sqlalchemy-migrate is not installed. It is '
#                            'necessary to uptade table columns.')
#
#     from sdss.internal.database.utah.mangadb import SampleModelClasses
#
#     allTables = SampleModelClasses.Base.metadata.tables.keys()
#
#     if 'mangasampledb.' + catname not in allTables:
#         raise RuntimeError('error while trying to update table {0}. The '
#                            'table does not exist.'.format(catname))
#
#     camelTable = SampleModelClasses.camelizeClassName(
#         None, catname, None)
#
#     sqlTable = SampleModelClasses.Base.classes[camelTable].__table__
#     allColumns = (sqlTable.columns.keys())
#
#     newCols = []
#     for col in catData.colnames:
#         if col.lower() not in allColumns:
#             newCols.append(col)
#
#     try:
#         for colName in newCols:
#             dtype = catData.columns[colName].dtype.type
#             shape = catData.columns[colName].shape
#             sqlType = getPSQLtype(dtype, shape)
#             sqlColumn = sql.Column(colName.lower(), sqlType)
#             sqlColumn.create(sqlTable)
#     except Exception as ee:
#         raise RuntimeError('error found while trying to append column '
#                            '{0}: {1}'.format(colName, ee))


def runTests(args):
    """Runs tests for mangaSampleDB ingestion."""

    pass


def _checkValue(value):
    """Converts NaNs to None."""

    if np.isscalar(value):
        try:
            if np.isnan(value):
                return None
            else:
                return value
        except:
            return value
    else:
        return np.where(np.isnan(value), None, value)


def ingestCatalogue(catfile, catname, version, current=False,
                    match=None, connstring=None, step=500, **kwargs):
    """Runs the catalogue ingestion."""

    # Runs some sanity checks
    if match is not None:
        assert len(match) == 2, ('Too few files for match loading '
                                 '(MATCH_FILE MATCH_DESCRIPTION).')
        assert os.path.exists(match[0]), 'MATCH_FILE could not be found.'
        assert os.path.exists(match[1]), 'MATCH_DESCRIPTION could not be found'
        assert match[0] != match[1], ('MATCH_FILE and MATCH_DESCRIPTION '
                                      'cannot be identical.')

    assert os.path.exists(catfile), 'CATFILE could not be found.'

    # Creates the appropriate connection to the DB.
    if connstring is None:
        db = DatabaseConnection('mangadb_local', models=['mangasampledb'])
    else:
        db = DatabaseConnection(databaseConnectionString=connstring,
                                models=['mangasampledb'])

    session = db.Session()
    sampleDB = db.mangasampledb

    # Checks if the catalogue name and version already exists. If not, creates
    # the appropriate catalogue table
    with session.begin(subtransactions=True):
        catalogue = session.query(sampleDB.Catalogue).filter(
            sampleDB.Catalogue.catalogue_name == catname).scalar()
        if catalogue is not None:
            if catalogue.version == version:
                raise RuntimeError('(CATNAME, VERSION) already exist '
                                   'in mangaSampleDB.')

    # Reads the catalogue file
    catData = table.Table.read(catfile, format='fits')

    # Creates the new table.
    newTable = _createNewTable(catname, catData, db.engine)

    colnames = catData.colnames
    nRows = len(catData)

    print('INFO: now inserting ...')
    print('INFO: inserting {0} rows at a time.'.format(step))
    sys.stdout.write('INFO: inserted 0 rows out of {0}.\r'.format(nRows))
    sys.stdout.flush()

    nn = 0
    while nn < nRows:

        mm = nn + step

        if mm >= nRows:
            mm = nRows

        dataDict = {}
        for ii, colname in enumerate(colnames):
            try:
                dataDict[colname] = np.where(np.isnan(catData[colname][nn:mm]),
                                             None,
                                             catData[colname][nn:mm])
            except:
                dataDict[colname] = catData[colname][nn:mm]

        dataLength = len(dataDict[colnames[0]])

        # Some older versions of PosgreSQL seem to have problem when inserting
        # an array of all NULLs. This ugly loop looks for those cases and
        # replaces the array with a simple NULL.
        data = []
        for ii in range(dataLength):
            dd = {}
            for col in colnames:
                value = dataDict[col][ii]
                if not np.isscalar(value) and not np.any(value):
                    value = None
                dd[col.lower()] = value
            data.append(dd)

        db.engine.execute(newTable.insert(data))

        if mm % step == 0:
            sys.stdout.write(
                'INFO: inserted {0} rows out of {1}.\r'.format(mm, nRows))
            sys.stdout.flush()

        nn = mm

    sys.stdout.write('INFO: inserted {0} rows.'.format(nRows))
    sys.stdout.flush()
