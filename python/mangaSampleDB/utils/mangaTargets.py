#!/usr/bin/env python
# encoding: utf-8
"""

mangaTargets.py

Created by José Sánchez-Gallego on 18 Feb 2016.
Licensed under a 3-clause BSD license.

Revision history:
    18 Feb 2016 J. Sánchez-Gallego
      Initial version

"""

from __future__ import division
from __future__ import print_function
from SDSSconnect import DatabaseConnection
import warnings
from astropy import table
import os


def _warning(message, category=UserWarning, filename='', lineno=-1):
    print('{0}: {1}'.format(category.__name__, message))

warnings.showwarning = _warning


def loadMangaTargets(mangaTargetsExtFile):
    """Loads a list of manga targets to mangasampledb.manga_target.

    Parameters
    ----------
    mangaTargetsExtFile : str
        The path to the MaNGA_targets_extNSA catalogue to load.

    """

    assert os.path.exists(mangaTargetsExtFile), 'file does not exit.'

    # Creates DB connection

    db = DatabaseConnection('mangadb_local', models=['mangasampledb'])
    session = db.Session()

    targets = table.Table.read(mangaTargetsExtFile)
    mangaIDs = [target.strip() for target in targets['MANGAID']]
    setMangaIDs = set(mangaIDs)

    if len(setMangaIDs) != len(mangaIDs):
        warnings.warn('there are {0} repeated mangaids in your input file. '
                      'Duplicates will be removed.'
                      .format(len(mangaIDs) - len(setMangaIDs)))

    dbMangaIDs = session.query(db.mangasampledb.MangaTarget.mangaid).all()
    if len(dbMangaIDs) > 0:
        dbMangaIDs = zip(*dbMangaIDs)[0]

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
        db.engine.execute(
            db.mangasampledb.MangaTarget.__table__.insert(
                [{'mangaid': mangaid} for mangaid in mangaIDs_insert]))
        print('INFO: inserted {0} targets.'.format(len(mangaIDs_insert)))
    else:
        print('INFO: not inserting any target.')
