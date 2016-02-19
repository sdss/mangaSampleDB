#!/usr/bin/env python
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
from SDSSconnect import DatabaseConnection
import warnings
from astropy import table
import os


def _warning(message, category=UserWarning, filename='', lineno=-1):
    print('{0}: {1}'.format(category.__name__, message))

warnings.showwarning = _warning


def loadMangaCharacters(characterList, imageDir):
    """Loads a list of manga characters to mangasampledb.character.

    Parameters
    ----------
    characterList : str
        The file containing the list of characters. Must have been generated
        using `parseMaNGACharacters.py`.
    imageDir : str
        The path where the downloaded images can be found.

    """

    assert os.path.exists(characterList), 'file does not exit.'
    assert os.path.exists(imageDir), 'image dir does not exit.'

    # Creates DB connection

    db = DatabaseConnection('mangadb_local', models=['mangasampledb'])
    session = db.Session()

    characters = table.Table.read(characterList, format='ascii.fixed_width')

    for character in characters:
        name = character['name'].strip()
        imagePath = os.path.join(imageDir, character['imageName'])
        animeName = character['manga']

        if not os.path.exists(imagePath):
            warnings.warn('image for {0} cannot be found. Skipping character.'
                          .format(name))

        image = open(imagePath, 'rb').read()

        with session.begin():

            nameQuery = session.query(db.mangasampledb.Character).filter(
                db.mangasampledb.Character.name == name).all()

            if len(nameQuery) > 0:
                continue

            newChar = db.mangasampledb.Character()
            newChar.name = name
            newChar.picture = image
            newChar.manga_target_pk = None

            session.add(newChar)
            session.flush()

            animeQuery = session.query(db.mangasampledb.Anime).filter(
                db.mangasampledb.Anime.anime == animeName).all()

            if len(animeQuery) == 0:
                anime = db.mangasampledb.Anime()
                anime.anime = animeName
                session.add(anime)
                session.flush()
            else:
                anime = animeQuery[0]

            newCharacterToAnime = db.mangasampledb.CharacterToAnime()
            newCharacterToAnime.character_pk = newChar.pk
            newCharacterToAnime.anime_pk = anime.pk
            session.add(newCharacterToAnime)
