/*

MangaSampleDB schema

Database to house all the sample info specific to the MaNGA survey.

Created by José Sánchez-Gallego starting April 2014.
Updated by José Sánchez-Gallego in July 2015.
Finished (with much rejoicing) by José Sánchez-Gallego in February 2015.

*/

CREATE SCHEMA mangasampledb;

SET search_path TO mangasampledb;

CREATE TABLE mangasampledb.manga_target
    (pk SERIAL PRIMARY KEY NOT NULL,
     mangaid TEXT NOT NULL);

CREATE TABLE mangasampledb.catalogue
    (pk SERIAL PRIMARY KEY NOT NULL,
     catalogue_name TEXT NOT NULL,
     version TEXT NOT NULL,
     match_description TEXT,
     matched BOOLEAN);

CREATE TABLE mangasampledb.current_catalogue
    (pk SERIAL PRIMARY KEY NOT NULL,
     catalogue_pk SMALLINT NOT NULL);

CREATE TABLE mangasampledb.manga_target_to_manga_target
    (pk SERIAL PRIMARY KEY NOT NULL,
     manga_target1_pk INTEGER,
     manga_target2_pk INTEGER);

CREATE TABLE mangasampledb.character
    (pk SERIAL PRIMARY KEY NOT NULL,
     name TEXT, picture BYTEA,
     manga_target_pk INTEGER);

CREATE TABLE mangasampledb.anime
    (pk SERIAL PRIMARY KEY NOT NULL,
     anime TEXT);

CREATE TABLE mangasampledb.character_to_anime
    (pk SERIAL PRIMARY KEY NOT NULL,
     character_pk INTEGER,
     anime_pk INTEGER);

ALTER TABLE ONLY mangasampledb.manga_target_to_manga_target
    ADD CONSTRAINT manga_target1_fk FOREIGN KEY (manga_target1_pk)
    REFERENCES mangasampledb.manga_target(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY mangasampledb.manga_target_to_manga_target
    ADD CONSTRAINT manga_target2_fk FOREIGN KEY (manga_target2_pk)
    REFERENCES mangasampledb.manga_target(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY mangasampledb.character_to_anime
    ADD CONSTRAINT character_fk FOREIGN KEY (character_pk)
    REFERENCES mangasampledb.character(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY mangasampledb.character_to_anime
    ADD CONSTRAINT anime_fk FOREIGN KEY (anime_pk)
    REFERENCES mangasampledb.anime(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY mangasampledb.current_catalogue
    ADD CONSTRAINT catalogue_fk FOREIGN KEY (catalogue_pk)
    REFERENCES mangasampledb.catalogue(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY mangasampledb.character
    ADD CONSTRAINT manga_target_fk FOREIGN KEY (manga_target_pk)
    REFERENCES mangasampledb.manga_target(pk)
    ON UPDATE CASCADE ON DELETE CASCADE;
