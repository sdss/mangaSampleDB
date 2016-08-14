#!/usr/bin/env python3
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

import os
import warnings

import sqlalchemy as sql
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import mapper, configure_mappers

from astropy import table
import numpy as np

from mangaSampleDB.utils.table_to_db import table_to_db


def _warning(message, category=UserWarning, filename='', lineno=-1):
    print('{0}: {1}'.format(category.__name__, message))

warnings.showwarning = _warning

__all__ = ('ingestCatalogue')


def _createCatalogueRecord(Base, session, catname, version,
                           match=None, current=True):
    """Adds a new row to the mangasampledb.catalogue table."""

    Catalogue = Base.classes.catalogue
    CurrentCatalogue = Base.classes.current_catalogue

    with session.begin():
        newCatalogue = Catalogue()
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

        currentCheck = session.query(CurrentCatalogue.pk).join(
            Catalogue).filter(Catalogue.catalogue_name == catname).scalar()

        with session.begin():
            if currentCheck:
                currentToRemove = session.query(CurrentCatalogue).get(
                    currentCheck)
                warnings.warn(
                    'removing {0} {1} as current catalogue'
                    .format(
                        currentToRemove.mangasampledb_catalogue.catalogue_name,
                        currentToRemove.mangasampledb_catalogue.version),
                    UserWarning)
                session.delete(currentToRemove)

            newCurrentCatalogue = CurrentCatalogue(
                catalogue_pk=newCatalogue.pk)
            session.add(newCurrentCatalogue)
            print('INFO: added {0} {1} as current catalogue'
                  .format(catname, version))

    return newCatalogue.pk


def _createRelationalTable(Base, engine, session, metadata,
                           matchCat, NewCatTable):
    """Created a relation table between `NewTable` and manga_target."""

    MangaTarget = Base.classes.manga_target

    newCatTableName = NewCatTable.__table__.name.lower()

    # Checks if table exists
    inspector = Inspector.from_engine(engine)
    tables = inspector.get_table_names(schema='mangasampledb')

    matchCol = [col.lower() for col in matchCat.colnames
                if col.lower() != 'mangaid'][0]

    relationalTableName = 'manga_target_to_{0}'.format(newCatTableName)

    if relationalTableName not in tables:

        relationalTable = sql.Table(
            relationalTableName, metadata,
            sql.Column('pk', sql.Integer, primary_key=True),
            sql.Column('manga_target_pk', sql.Integer,
                       sql.ForeignKey(MangaTarget.pk)),
            sql.Column('{0}_pk'.format(newCatTableName), sql.Integer,
                       sql.ForeignKey(NewCatTable.pk)))

        metadata.create_all(engine)

        class RelationalTable(Base):
            __table__ = relationalTable

        mapper(RelationalTable, relationalTable)
        print('INFO: created table {0}'.format(relationalTableName))

    else:
        warnings.warn('table {0} already exists'.format(relationalTableName),
                      UserWarning)
        RelationalTable = Base.classes[relationalTableName]

    configure_mappers()

    print('INFO: loading data into {0} ...'.format(relationalTableName))

    # Gets information for pks and mangaids from MangaTarget
    mangaTargetData = session.query(MangaTarget.pk, MangaTarget.mangaid).all()
    mangaIds = np.array(list(zip(*mangaTargetData))[1])
    mangaTargetPks = np.array(list(zip(*mangaTargetData))[0])

    # Does the same with the new catalogue table, using the match column
    matchColData = session.query(
        NewCatTable.pk, getattr(NewCatTable, matchCol)).all()

    newTableMatchValues = np.array(list(zip(*matchColData))[1])
    newTableMatchPks = np.array(list(zip(*matchColData))[0])

    # Builds the insert dictionary making sure we get information about both
    # mangaid and new catalogue match column.
    insertData = []
    for mangaid, matchVal in matchCat:
        if mangaid not in mangaIds or matchVal not in newTableMatchValues:
            continue
        mangaTargetPk = mangaTargetPks[np.where(mangaIds == mangaid)][0]
        newTableMatchPk = newTableMatchPks[
            np.where(newTableMatchValues == matchVal)][0]
        insertData.append(
            {'manga_target_pk': int(mangaTargetPk),
             '{0}_pk'.format(newCatTableName): int(newTableMatchPk)})

    # Inserts the data
    engine.execute(RelationalTable.__table__.insert(insertData))

    return RelationalTable


def ingestCatalogue(catfile, catname, version, engine, current=True,
                    match=None, step=500, limit=False, overwrite=False,
                    verbose=False, **kwargs):
    """Runs the catalogue ingestion.

    Parameters:
        catfile (str):
            The FITS file containing the catalogue.
        catname (str):
            The name of the catalogue. The table created under mangasampledb
            will have that name.
        version (str):
            The version of the catalogue being ingested.
        engine (SQLAlchemy |engine|):
            The engine to use to connect to the DB.
        current (bool):
            ``True`` if the ingested version of the catalogue must be made
            current.
        match (None or tuple):
            If ``None``, not relational database will be created joining the
            ingested catalogue to mangasampledb.manga_target. Otherwise, a
            tuple of two elements, the first being the matching catalogue and
            the second the text file containing the description of how the
            matching was done. The matching catalogue must contain only two
            columns, ``mangaid`` and a column present in ``catfile`` that is
            a unique identifier of the targets in the ingested catalogue.
        step (int):
            The number of catalogue elements that must be inserted at a time.
        limit (bool):
            If ``True``, only the targets in ``catfile`` that are matched to
            MaNGA targets will be ingested. Requires ``match`` to be set.
        overwrite (bool):
            If ``True``, removes any table that already exists before recreting
            it.
        verbose (bool):
            Sets the verbosity mode.

    Returns:
        result (tuple):
            If ``match=None``, returns the model class for the new catalogue
            table. Otherwise, returs a tuple in which the first element is the
            model of the new catalogue table and the second is the model of
            the relational, many-to-many table joining it to
            mangasampledb.manga_target.

    .. |engine| replace:: Engine `<http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_

    """

    # Runs some sanity checks
    if match is not None:
        assert len(match) == 2, ('Too few files for match loading '
                                 '(MATCH_FILE MATCH_DESCRIPTION).')
        assert os.path.exists(match[0]), 'MATCH_FILE could not be found.'
        assert os.path.exists(match[1]), 'MATCH_DESCRIPTION could not be found'
        assert match[0] != match[1], ('MATCH_FILE and MATCH_DESCRIPTION '
                                      'cannot be identical.')

    assert os.path.exists(catfile), 'CATFILE could not be found.'

    if limit and not match:
        raise ValueError('limit=True but match not set.')

    # Bind the base to the current engine
    metadata = sql.MetaData(schema='mangasampledb')
    metadata.reflect(engine)
    Base = automap_base(metadata=metadata)
    Base.prepare()

    Catalogue = Base.classes.catalogue

    # Creates a session
    Session = sessionmaker(engine, autocommit=True)
    session = Session()

    # Checks if table already exists.
    inspector = Inspector.from_engine(engine)
    tables = inspector.get_table_names(schema='mangasampledb')

    if catname in tables:
        raise ValueError('table {0} already exists in mangasampledb. '
                         'Drop it before continuing.'.format(catname))

    # Checks if the catalogue name and version already exists. If not, adds it.
    with session.begin(subtransactions=True):
        catalogue = session.query(Catalogue.pk).filter(
            Catalogue.catalogue_name == catname,
            Catalogue.version == version).scalar()

    if catalogue is not None:
        catPK = catalogue
        warnings.warn('(CATNAME, VERSION)=({0}, {1}) '
                      'already exist in mangaSampleDB.'
                      .format(catname, version),
                      UserWarning)
    else:
        print('INFO: creating record in mangasampledb.catalogue for '
              'CATNAME={0}, VERSION={1}.'.format(catname, version))
        catPK = _createCatalogueRecord(Base, session, catname, version,
                                       match=match, current=current)

    # Reads the catalogue file
    catData = table.Table.read(catfile, format='fits')
    catData.add_column(table.Column(data=[catPK] * len(catData),
                                    name='catalogue_pk', dtype=int))

    # Reads matching file, if any
    if match:
        matchCat = table.Table.read(match[0])
        matchCol = [col.upper() for col in matchCat.colnames
                    if col.lower() != 'mangaid'][0]
    else:
        matchCat = None

    if limit:
        validIndx = np.where(np.in1d(catData[matchCol],
                                     matchCat[matchCol.lower()]))[0]
        catData = catData[validIndx]

    NewCatTable = table_to_db(catData, 'manga', 'mangasampledb', 'nsa',
                              engine=engine, overwrite=overwrite,
                              chunk_size=step, verbose=verbose)

    # If there is a matching catalogue, we create the table relating
    # the new catalogue with mangasampledb.manga_target.
    if matchCat:
        RelationalTable = _createRelationalTable(Base, engine, session,
                                                 metadata, matchCat,
                                                 NewCatTable)
        return (NewCatTable, RelationalTable)

    return RelationalTable
