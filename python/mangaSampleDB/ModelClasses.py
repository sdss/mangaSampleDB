#!/usr/bin/env python
# encoding: utf-8
"""
SampleModelClasses.py

Created by José Sánchez-Gallego on 23 Jul 2015.
Licensed under a 3-clause BSD license.

Revision history:
    23 Jul 2015 J. Sánchez-Gallego
      Initial version

"""

from __future__ import division
from __future__ import print_function
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sdss.internal.database.DatabaseConnection import DatabaseConnection
import cStringIO
import shutil
import re


def camelizeClassName(base, tablename, table):
    """Produces a camelised class name.

    E.g. words_and_underscores' -> 'WordsAndUnderscores'
    http://docs.sqlalchemy.org/en/rel_1_0/orm/extensions/automap.html

    """

    return str(tablename[0].upper() +
               re.sub(r'_([a-z])',
                      lambda m: m.group(1).upper(), tablename[1:]))


# Grabs engine
db = DatabaseConnection()
engine = db.engine

# Selects schema and automaps it.
metadata = MetaData(schema='mangasampledb')
Base = automap_base(bind=engine, metadata=metadata)

Base.prepare(engine, reflect=True, classname_for_table=camelizeClassName)

for cl in Base.classes.keys():
    exec('{0} = Base.classes.{0}'.format(cl))


# Adds customised methods to some tables.

def savePicture(self, path):
    """Saves the picture blob to disk."""

    buf = cStringIO.StringIO(self.picture)
    with open(path, 'w') as fd:
        buf.seek(0)
        shutil.copyfileobj(buf, fd)

    return buf

Character.savePicture = savePicture
