#!/usr/bin/env python3
# encoding: utf-8
"""

characters.py

Created by José Sánchez-Gallego on 18 Feb 2016.
Licensed under a 3-clause BSD license.

Revision history:
    18 Feb 2016 J. Sánchez-Gallego
      Initial version

"""

from __future__ import division
from __future__ import print_function

import os
import sys
import warnings

try:
    import progressbar
except ImportError:
    progressbar = False

import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from astropy import table

__all__ = ('loadMangaCharacters')


def _warning(message, category=UserWarning, filename='', lineno=-1):
    print('{0}: {1}'.format(category.__name__, message))

warnings.showwarning = _warning


def loadMangaCharacters(characterList, imageDir, engine):
    """Loads a list of manga characters to mangasampledb.character.

    Parameters:
        characterList (str):
            The file containing the list of characters. Must have been
            generated using `parseMaNGACharacters.py`.
        imageDir (str):
            The path where the downloaded images can be found.
        engine (SQLAlchemy |engine|):
            The engine to use to connect to the DB.

    Return:
        result (bool):
            Return ``True`` if all the rows have been successfully inserted.

    .. |engine| replace:: Engine `<http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_

    """

    assert os.path.exists(characterList), 'file does not exit.'
    assert os.path.exists(imageDir), 'image dir does not exit.'

    # Bind the base to the current engine
    metadata = sql.MetaData()
    metadata.reflect(engine, schema='mangasampledb')
    Base = automap_base(metadata=metadata)
    Base.prepare()

    Character = Base.classes.character
    Anime = Base.classes.anime

    # Creates a session
    Session = sessionmaker(engine, autocommit=True)
    session = Session()

    characters = table.Table.read(characterList, format='ascii.fixed_width')
    nCharacter = len(characters)

    # If the progressbar package is installed, uses it to create a progress bar
    if progressbar:
        bar = progressbar.ProgressBar()
        iterable = bar(range(nCharacter))
    else:
        iterable = range(nCharacter)

    with session.begin():

        for ii in iterable:

            character = characters[ii]

            name = character['name'].strip()
            imagePath = os.path.join(imageDir, character['imageName'])
            animeName = character['manga']

            if not os.path.exists(imagePath):
                warnings.warn('image for {0} cannot be found. '
                              'Skipping character.'.format(name))

            image = open(imagePath, 'rb').read()

            nameQuery = session.query(Character).filter(
                Character.name == name).all()

            if len(nameQuery) > 0:
                continue

            animeQuery = session.query(Anime).filter(
                Anime.anime == animeName).all()

            if len(animeQuery) == 0:
                anime = Anime()
                anime.anime = animeName
                session.add(anime)
                session.flush()
            else:
                anime = animeQuery[0]

            newChar = Character()
            newChar.name = name
            newChar.picture = image
            newChar.manga_target_pk = None
            newChar.anime_pk = anime.pk

            session.add(newChar)

    return True
