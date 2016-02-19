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
from sqlalchemy.orm import mapper, configure_mappers
from sqlalchemy.ext.declarative import declarative_base
from SDSSconnect import DatabaseConnection
from sqlalchemy.engine.reflection import Inspector
from astropy import table
import warnings
import numpy as np
import os
import sys


def _warning(message, category=UserWarning, filename='', lineno=-1):
    print('{0}: {1}'.format(category.__name__, message))

warnings.showwarning = _warning


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
    meta.create_all(engine)

    class NewCatTable(object):
        __table__ = newTable
        pass

    mapper(NewCatTable, newTable)
    configure_mappers()

    return NewCatTable


def _createCatalogueRecord(db, session, catname, version,
                           match=None, current=True):
    """Adds a new row to the mangasampledb.catalogue table."""

    with session.begin():
        newCatalogue = db.mangasampledb.Catalogue()
        newCatalogue.catalogue_name = catname
        newCatalogue.version = version
        if match:
            newCatalogue.matched = True
            newCatalogue.match_description = open(match[1], 'r').read()
        else:
            newCatalogue.matched = False

        session.add(newCatalogue)

    # If current is True, checks if other version of this catalogue is
    # already current. If so, removes it an makes the new one current.
    if current:

        currentCheck = session.query(
            db.mangasampledb.CurrentCatalogue.pk).join(
                db.mangasampledb.Catalogue).filter(
                    db.mangasampledb.Catalogue.catalogue_name == catname
        ).scalar()

        with session.begin():
            if currentCheck:
                currentToRemove = session.query(
                    db.mangasampledb.CurrentCatalogue).get(currentCheck)
                warnings.warn(
                    'removing {0} {1} as current catalogue'
                    .format(
                        currentToRemove.mangasampledb_catalogue.catalogue_name,
                        currentToRemove.mangasampledb_catalogue.version),
                    UserWarning)
                session.delete(currentToRemove)

            newCurrentCatalogue = db.mangasampledb.CurrentCatalogue(
                catalogue_pk=newCatalogue.pk)
            session.add(newCurrentCatalogue)
            warnings.warn('added {0} {1} as current catalogue'
                          .format(catname, version), UserWarning)

    return newCatalogue.pk


def _createRelationalTable(db, session, matchCat, NewCatTable):
    """Created a relation table between `NewTable` and manga_target."""

    newCatTableName = NewCatTable.__table__.name.lower()

    # Checks if table exists
    inspector = Inspector.from_engine(db.engine)
    tables = inspector.get_table_names(schema='mangasampledb')

    newTableMatchCol = [col.lower() for col in matchCat.colnames
                        if col.lower() != 'mangaid'][0]

    relationalTableName = 'manga_target_to_{0}'.format(newCatTableName)

    if relationalTableName not in tables:

        metadata = sql.MetaData(schema='mangasampledb')

        relationalTable = sql.Table(
            relationalTableName, metadata,
            sql.Column('pk', sql.Integer, primary_key=True),
            sql.Column('manga_target_pk', sql.Integer,
                       sql.ForeignKey(db.mangasampledb.MangaTarget.pk)),
            sql.Column('{0}_pk'.format(newCatTableName), sql.Integer,
                       sql.ForeignKey(NewCatTable.pk)))

        metadata.create_all(db.engine)

        class RelationalTable(object):
            __table__ = relationalTable

        mapper(RelationalTable, relationalTable)

    else:
        warnings.warn('table {0} already exists'.format(relationalTableName),
                      UserWarning)

        decBase = declarative_base(bind=db.engine)

        class RelationalTable(decBase):
            __tablename__ = relationalTableName
            __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    configure_mappers()

    with session.begin():

        for mangaid, matchValue in matchCat:

            mangaTargetPK = session.query(
                db.mangasampledb.MangaTarget.pk).filter(
                    db.mangasampledb.MangaTarget.mangaid == mangaid.strip()
            ).scalar()
            if not mangaTargetPK:
                raise ValueError('ERROR: creating table {0}: '
                                 'cannot find mangaid={1} in manga_target.'
                                 .format(relationalTableName, mangaid))

            matchPK = session.query(NewCatTable.pk).filter(
                sql.text('{0}.{1} = {2}'
                         .format(newCatTableName,
                                 newTableMatchCol, matchValue))).scalar()
            if not matchPK:
                continue
                raise ValueError('ERROR: creating table {0}: '
                                 'cannot find {1}={2} in {3}.'
                                 .format(relationalTableName,
                                         newTableMatchCol,
                                         matchValue,
                                         newCatTableName))

            # Check if the record already exists
            matchRecord = session.query(RelationalTable.pk).filter(
                RelationalTable.manga_target_pk == mangaTargetPK,
                sql.text('{0}.{1}_pk = {2}'.format(relationalTableName,
                                                   newCatTableName, matchPK))
            ).scalar()

            if matchRecord:
                continue
            else:
                newRecord = RelationalTable()
                newRecord.manga_target_pk = mangaTargetPK
                setattr(newRecord, '{0}_pk'.format(newCatTableName), matchPK)
                session.add(newRecord)

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


def ingestCatalogue(catfile, catname, version, current=True,
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

    # Checks if table already exists.
    inspector = Inspector.from_engine(db.engine)
    tables = inspector.get_table_names(schema='mangasampledb')

    if catname in tables:
        raise ValueError('table {0} already exists in mangasampledb. '
                         'Drop it before continuing.'.format(catname))

    # Checks if the catalogue name and version already exists. If not, adds
    with session.begin(subtransactions=True):
        catalogue = session.query(sampleDB.Catalogue.pk).filter(
            sampleDB.Catalogue.catalogue_name == catname,
            sampleDB.Catalogue.version == version).scalar()

    if catalogue is not None:
        catPK = catalogue
        warnings.warn('(CATNAME, VERSION) already exist in mangaSampleDB.',
                      UserWarning)
    else:
        print('INFO: creating record in mangasampledb.catalogue for '
              'CATNAME={0}, VERSION={1}.'.format(catname, version))
        catPK = _createCatalogueRecord(db, session, catname, version,
                                       match=match, current=current)

    # Reads the catalogue file
    catData = table.Table.read(catfile, format='fits')

    # Reads matching file, if any
    if match:
        matchCat = table.Table.read(match[0])
    else:
        matchCat = None

    # Creates the new table.
    NewCatTable = _createNewTable(catname, catData, db.engine)

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

        # Replaces NaN in arrays with None.
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
            dd['catalogue_pk'] = catPK
            data.append(dd)

        db.engine.execute(NewCatTable.__table__.insert(data))

        if mm % step == 0:
            sys.stdout.write(
                '\x1b[2KINFO: inserted {0} rows out of {1}.\r'
                .format(mm, nRows))
            sys.stdout.flush()

        nn = mm

    sys.stdout.write('INFO: inserted {0} rows.\n'.format(nRows))
    sys.stdout.flush()

    # If there is a matching catalogue, we create the table relating
    # the new catalogue with mangasampledb.manga_target.
    if matchCat:
        _createRelationalTable(db, session, matchCat, NewCatTable)
