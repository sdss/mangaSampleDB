#!/usr/bin/env python3
# encoding: utf-8
"""

targets.py

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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from astropy import table


def _warning(message, category, *args, **kwargs):
    print('{0}: {1}'.format(category.__name__, message))


warnings.showwarning = _warning

__all__ = ('loadMangaTargets')


def loadMangaTargets(mangaTargetsExtFile, drpall_file, engine):
    """Loads a list of manga targets to mangasampledb.manga_target.

    Parameters:
        mangaTargetsExtFile (str):
            The path to the MaNGA_targets_extNSA catalogue to load.
        drpall_file (srt):
            The path to the drpall file. This file is used to complement
            ``mangaTargetsExtFile`` with targets that have been observed but
            are not in the targetting catalogue (e.g., ancillaries or past
            target selections).
        engine (SQLAlchemy |engine|):
            The engine to use to connect to the DB.

    Returns:
        result (bool):
            Returns ``True`` if at least one row was inserted, False otherwise.

    .. |engine| replace:: Engine `<http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_

    """

    assert os.path.exists(mangaTargetsExtFile), 'file does not exit.'

    # Creates DB session

    Session = sessionmaker(bind=engine)
    session = Session()

    Base = declarative_base(bind=engine)

    class MangaTarget(Base):
        __tablename__ = 'manga_target'
        __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    targets = table.Table.read(mangaTargetsExtFile)
    drpall = table.Table.read(drpall_file)
    mangaIDs = [target.strip()
                for target in targets['MANGAID'].tolist() + drpall['mangaid'].tolist()]
    setMangaIDs = set(mangaIDs)

    if len(setMangaIDs) != len(mangaIDs):
        warnings.warn('there are {0} repeated mangaids in your input file. '
                      'Duplicates will be removed.'
                      .format(len(mangaIDs) - len(setMangaIDs)))

    dbMangaIDs = session.query(MangaTarget.mangaid).all()

    if len(dbMangaIDs) > 0:
        dbMangaIDs = list(zip(*dbMangaIDs))[0]

    setDbMangaIDs = set(dbMangaIDs)

    if len(setDbMangaIDs) != len(dbMangaIDs):
        warnings.warn('there are {0} repeated mangaids in your DB. '
                      'You should fix this.'
                      .format(len(dbMangaIDs) - len(setDbMangaIDs)))

    mangaIDs_insert = list(setMangaIDs - setDbMangaIDs)

    if len(mangaIDs_insert) != len(mangaIDs):
        warnings.warn('not inserting {0} targets because they '
                      'are already in the DB.'
                      .format(len(mangaIDs) - len(mangaIDs_insert)))

    if len(mangaIDs_insert) > 0:
        engine.execute(
            MangaTarget.__table__.insert(
                [{'mangaid': mangaid} for mangaid in mangaIDs_insert]))
        print('INFO: inserted {0} targets.'.format(len(mangaIDs_insert)))
        return True
    else:
        print('INFO: not inserting any target.')
        return False
