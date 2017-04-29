#!/usr/bin/env python
# encoding: utf-8
#
# update_cube_manga_target_pk.py
#
# Created by José Sánchez-Gallego on 28 Apr 2017.


from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import argparse
import os
import sys

from mangaSampleDB.utils import create_connection

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def update_cube_manga_target_pk(engine):
    """Matches mangadatadb.cube.manga_target_pk with mangasampledb.manga_target.pk."""

    # Creates DB session

    Session = sessionmaker(bind=engine, autocommit=True)
    session = Session()

    Base = declarative_base(bind=engine)

    class MangaTarget(Base):
        __tablename__ = 'manga_target'
        __table_args__ = {'autoload': True, 'schema': 'mangasampledb'}

    class Cube(Base):
        __tablename__ = 'cube'
        __table_args__ = {'autoload': True, 'schema': 'mangadatadb'}

    with session.begin():
        for cc in session.query(Cube).all():
            target = session.query(MangaTarget).filter(
                MangaTarget.mangaid == cc.mangaid).scalar()
            cc.manga_target_pk = target.pk


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]),
        description=('Updated mangadatadb.cube.manga_target_pk'))

    parser_db = parser.add_argument_group(title='Database connect arguments')
    parser_db.add_argument('-d', '--database', dest='database', type=str,
                           default='manga', help='The database name.')
    parser_db.add_argument('-u', '--user', dest='user', type=str,
                           default='manga', help='The database username.')
    parser_db.add_argument('-w', '--password', dest='password', type=str,
                           default='', help='The database password.')
    parser_db.add_argument('-H', '--host', dest='host', type=str,
                           default='localhost', help='The database host.')
    parser_db.add_argument('-p', '--port', dest='port', type=int, default=5432,
                           help='The database port.')

    args = parser.parse_args()

    engine = create_connection(db_name=args.database,
                               username=args.user,
                               password=args.password,
                               host=args.host,
                               port=args.port)

    update_cube_manga_target_pk(engine)
