#!/usr/bin/env python
# encoding: utf-8
"""
ModelClasses.py

Created by José Sánchez-Gallego on 23 Jul 2015.
Licensed under a 3-clause BSD license.

Revision history:
    23 Jul 2015 J. Sánchez-Gallego
      Initial version
    21 Feb 2016 J. Sánchez-Gallego
      Rewritten as classes derived from declarative base.

"""

from __future__ import division
from __future__ import print_function
from sdss.internal.database.DatabaseConnection import DatabaseConnection
from sqlalchemy.orm import relationship, configure_mappers, backref
from sqlalchemy.inspection import inspect
from sqlalchemy import ForeignKeyConstraint
import re

db = DatabaseConnection()
Base = db.Base


def cameliseClassname(tableName):
    """Produce a camelised class name."""

    return str(tableName[0].upper() +
               re.sub(r'_([a-z])',
               lambda m: m.group(1).upper(), tableName[1:]))


def ClassFactory(name, tableName, BaseClass=db.Base, fks=None):
    tableArgs = [{'autoload': True, 'schema': 'mangasampledb'}]
    if fks:
        for fk in fks:
            tableArgs.insert(0, ForeignKeyConstraint([fk[0]], [fk[1]]))

    newclass = type(
        name, (BaseClass,),
        {'__tablename__': tableName,
         '__table_args__': tuple(tableArgs)})

    return newclass


class MangaTarget(Base):
    __tablename__ = 'manga_target'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    def __repr__(self):
        return '<MangaTarget (pk={0}, mangaid={1})>'.format(self.pk,
                                                            self.mangaid)


class Anime(Base):
    __tablename__ = 'anime'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    def __repr__(self):
        return '<Anime (pk={0}, anime={1})>'.format(self.pk, self.anime)


class Character(Base):
    __tablename__ = 'character'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    target = relationship(MangaTarget, backref='character', uselist=False)
    anime = relationship(Anime, backref='characters')

    def __repr__(self):
        return '<Character (pk={0}, name={1})>'.format(self.pk, self.name)


class Catalogue(Base):
    __tablename__ = 'catalogue'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    @property
    def isCurrent(self):
        return self.currentCatalogue is not None

    def __repr__(self):
        return ('<Catalogue (pk={0}), catalogue={1}, version={2}, current={3}>'
                .format(self.pk, self.catalogue_name, self.version,
                        self.isCurrent))


class CurrentCatalogue(Base):
    __tablename__ = 'current_catalogue'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    catalogue = relationship(
        'Catalogue', backref=backref('currentCatalogue', uselist=False))

    def __repr__(self):
        return '<CurrentCatalogue (pk={0})>'.format(self.pk)


class MangaTargetToMangaTarget(Base):
    __tablename__ = 'manga_target_to_manga_target'
    __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    def __repr__(self):
        return '<MangaTargetToMangaTarget (pk={0})>'.format(self.pk)

# Now we create the remaining tables.
insp = inspect(db.engine)
schemaName = 'mangasampledb'
allTables = insp.get_table_names(schema=schemaName)

done_names = db.Base.metadata.tables.keys()
for tableName in allTables:
    if schemaName + '.' + tableName in done_names:
        continue
    className = cameliseClassname(str(tableName))

    newClass = ClassFactory(
        className, tableName,
        fks=[('catalogue_pk', 'mangasampledb.catalogue.pk')])
    newClass.catalogue = relationship(
        Catalogue, backref='{0}_objects'.format(tableName))
    locals()[className] = newClass
    done_names.append(schemaName + '.' + tableName)

    if 'manga_target_to_' + tableName in allTables:
        relationalTableName = 'manga_target_to_' + tableName
        relationalClassName = cameliseClassname(str(relationalTableName))
        newRelationalClass = ClassFactory(
            relationalClassName, relationalTableName,
            fks=[('manga_target_pk', 'mangasampledb.manga_target.pk'),
                 ('nsa_pk', 'mangasampledb.nsa.pk')])

        locals()[relationalClassName] = newRelationalClass
        done_names.append(schemaName + '.' + relationalTableName)

        newClass.mangaTargets = relationship(
            MangaTarget, backref='{0}_objects'.format(tableName),
            secondary=newRelationalClass.__table__)

configure_mappers()
