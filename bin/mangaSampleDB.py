#!/usr/bin/env python
# encoding: utf-8
"""
mangaSampleDB.py

Created by José Sánchez-Gallego on 23 Jul 2015.
Licensed under a 3-clause BSD license.

Revision history:
    23 Jul 2015 J. Sánchez-Gallego
      Initial version

"""

from __future__ import division
from __future__ import print_function
import sqlalchemy as sql
from sqlalchemy.dialects import postgresql
from sdss.internal.database.DatabaseConnection import DatabaseConnection
from sdss.internal.database import NumpyAdaptors
from astropy import table
import numpy as np
import argparse
import sys
import os


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


def _updateColumns(catalogueName, catData):
    """Checks if new columns need to be added to an existing table."""

    try:
        from migrate import changeset
    except:
        raise RuntimeError('sqlalchemy-migrate is not installed. It is '
                           'necessary to uptade table columns.')

    from sdss.internal.database.utah.mangadb import SampleModelClasses

    allTables = SampleModelClasses.Base.metadata.tables.keys()

    if 'mangasampledb.' + catalogueName not in allTables:
        raise RuntimeError('error while trying to update table {0}. The '
                           'table does not exist.'.format(catalogueName))

    camelTable = SampleModelClasses.camelizeClassName(
        None, catalogueName, None)

    sqlTable = SampleModelClasses.Base.classes[camelTable].__table__
    allColumns = (sqlTable.columns.keys())

    newCols = []
    for col in catData.colnames:
        if col.lower() not in allColumns:
            newCols.append(col)

    try:
        for colName in newCols:
            dtype = catData.columns[colName].dtype.type
            shape = catData.columns[colName].shape
            sqlType = getPSQLtype(dtype, shape)
            sqlColumn = sql.Column(colName.lower(), sqlType)
            sqlColumn.create(sqlTable)
    except Exception as ee:
        raise RuntimeError('error found while trying to append column '
                           '{0}: {1}'.format(colName, ee))


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


def ingestCatalogue(args):
    """Runs the catalogue ingestion."""

    # Retrieves the information for the arguments
    catalogueFile = args.CATFILE
    catalogueVersion = args.VERSION
    catalogueName = args.CATNAME.lower()
    match = args.match
    current = args.current
    replace = args.replace
    connectionString = args.connString

    # Runs some sanity checks
    if match is not None:
        assert len(match) == 2, ('Too few files for match loading '
                                 '(MATCH_FILE MATCH_DESCRIPTION).')
        assert os.path.exists(match[0]), 'MATCH_FILE could not be found.'
        assert os.path.exists(match[1]), 'MATCH_DESCRIPTION could not be found'
        assert match[0] != match[1], ('MATCH_FILE and MATCH_DESCRIPTION '
                                      'cannot be identical.')

    assert os.path.exists(catalogueFile), 'CATFILE could not be found.'

    # Creates the appropriate connection to the DB.
    if connectionString is None:
        from sdss.internal.database.connections import \
            UtahMangaDatabaseConnection
        db = UtahMangaDatabaseConnection
    else:
        db = DatabaseConnection(database_connection_string=connectionString)

    # Now we import the ModelClasses for mangaSampleDB
    from sdss.internal.database.utah.mangadb \
        import SampleModelClasses as sampleDB

    session = db.Session()

    # Checks if the catalogue name and version already exists. If not, creates
    # the appropriate catalogue table
    with session.begin(subtransactions=True):
        catalogue = session.query(sampleDB.Catalogue).filter(
            sampleDB.Catalogue.catalogue_name == catalogueName).scalar()
        if catalogue is not None:
            if catalogue.version == catalogueVersion:
                raise RuntimeError('(CATNAME, VERSION) already exist '
                                   'in mangaSampleDB.')

    # Reads the catalogue file
    catData = table.Table.read(catalogueFile, format='fits')

    # If the catalogue table already exists, makes sure that all columns are
    # present. Otherwise, creates the table with the right columns.
    if catalogue is None:
        try:
            newTable = _createNewTable(catalogueName, catData, db.engine)
        except Exception as ee:
            print('Exception found while creating table {0}: {1}'
                  .format(catalogueName, ee))
    else:
        _updateColumns(catalogueName, catData)
        camelTable = sampleDB.camelizeClassName(None, catalogueName, None)
        newTable = sampleDB.Base.classes[camelTable].__table__

    dataDict = {}
    for ii, colname in enumerate(catData.colnames):
        try:
            dataDict[colname] = np.where(np.isnan(catData[colname]), None,
                                         catData[colname])
        except:
            dataDict[colname] = catData[colname]

    colnames = catData.colnames
    nRows = len(catData)

    # Now we add the data. First we create a list of dictionaries with the data
    data = [{colName.lower(): dataDict[colName][ii]
             for colName in colnames} for ii in range(nRows)]
    print('Inserting')
    db.engine.execute(newTable.insert(), data)


def main(argv=None):

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]))

    parser.add_argument('--connection-string', type=str, dest='connString',
                        help='The connection string to be used when calling '
                             'DatabaseConnection. If not defined, the default '
                             'connection was not used.')

    subparsers = parser.add_subparsers(title='actions')

    parserTest = subparsers.add_parser(
        'test', help='runs the test suite', description='Runs the test suite.')
    parserTest.set_defaults(func=runTests)

    parserIngest = subparsers.add_parser(
        'ingest', help='loads a catalogue in mangaSampleDB',
        description='Loads a catalogue in mangaSampleDB.')
    parserIngest.add_argument('CATFILE', metavar='CATFILE', type=str,
                              help='The file with the catalogue to load')
    parserIngest.add_argument('CATNAME', metavar='CATNAME', type=str,
                              help='The name for this catalogue')
    parserIngest.add_argument('VERSION', metavar='VERSION', type=str,
                              help='The version of the catalogue being loaded')
    parserIngest.add_argument('-c', '--current', dest='current',
                              action='store_true',
                              help='Makes the catalogue being loaded the '
                                   'current version.')
    parserIngest.add_argument('-r', '--replace', dest='replace',
                              action='store_true',
                              help='If the catalogue-version exists, '
                              'removes its recodes before ingesting.')
    parserIngest.add_argument('-m', '--match', dest='match', type=str,
                              action='store', nargs=2,
                              metavar=('MATCH_FILE', 'MATCH_DESCRIPTION'),
                              help='The file contianing the matching between '
                                   'mangaids and the catalogue being loaded '
                                   'and the file with the description on how '
                                   'the matching was performed.')
    parserIngest.set_defaults(func=ingestCatalogue)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
