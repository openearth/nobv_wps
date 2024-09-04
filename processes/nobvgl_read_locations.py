# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares
#       Gerrit Hendriksen
#       Gerrit Hendriksen@deltares.nl
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

# system pacakages
import os
import json
import configparser
import logging
from sqlalchemy import create_engine, func, select

logger = logging.getLogger("PYWPS")


# Read default configuration from file
def read_config(db):
    """Reads credential file based on the type of database passed

    Args:
        db (string): Indicates the type of database, either PG (PostgreSQL/PostGIS)
                     or Oracle (Oracle database)

    Returns:
        string: returns configuration file based on operating system and database type
    """
    if os.name == "nt":
        # logger.info('reading local configuration')
        devpath = r"C:\develop\nobv_wps\processes"
        # devpath=r'C:\projecten\grondwater_monitoring'
        confpath = os.path.join(devpath, "nobvgl_configuration.txt")
    else:
        confpath = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "nobvgl_configuration.txt"
        )
    if not os.path.exists(confpath):
        confpath = "/opt/pywps/processes/nobvgl_configuration.txt"
    # print("PG configpath", confpath)
    logger.info("path to configuration", confpath)

    # Parse and load
    cf = configparser.ConfigParser()
    cf.read(confpath)
    return cf


def createconnectiontopgdb():
    """create connection to PostgreSQL/PostGIS with contents of the configuration file

    Returns:
        sqlalchemy engine: SQLAlchemy Engine object
    """
    cf = read_config("pg")
    user = cf.get("PostGIS", "user")
    pwd = cf.get("PostGIS", "pass")
    host = cf.get("PostGIS", "host")
    db = cf.get("PostGIS", "db")
    engine = create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:5432/{d}".format(u=user, p=pwd, h=host, d=db)
    )
    logger.info("engine to PG created")
    # print("PG connection established")
    return engine


def getlocationsfromtableGL(prjnr):
    """Retrieves locations from Oracle Geolab using projectnr and credentials

    Args:
        prjnr (integer): Geolab has stored all data by projectnr.

    Returns:
        json: json object with combination of location and parameter
        measured (incl. a list per parameter for all instruments!)
    """
    engine = createconnectiontopgdb()
    with engine.connect() as conn:
        query = select(func.getlocations(prjnr))
        result = conn.execute(query).fetchone()[0]
        # c = connection.cursor()
        # # for now there is only 1 projectnr possible. In future it should be investigated if several projectnrs are desired
        # result = c.callfunc("getLocations", str, [prjnr])
        # # logger.info('retrieved result from Oracle db for prjnr',str(prjnr))
        print("results", str(len(result)))
        logger.info("number of results ", str(len(result)))
    return result


def getlocationsfromtable(nobv=True, geolab=True, prjnr=None):
    """Gets locations from two sources and combines them into 1 json
       This procedure replaces previous WPS in the end. (TODO)
    Args:
        nobv (bool, optional): Get locations from NOBV PostgreSQL/PostGIS database. Defaults to True.
        geolab (bool, optional): Get locations from GeoLab database. Defaults to True.
        prjnr (int, optional): Geolab works with projectnr. Defaults to 11206020.

    Returns:
        JSON : JSON with combined (if relevant) data
    """
    result = None
    res = None
    result2 = getlocationsfromtableGL(prjnr)
    rest = json.loads(result2)
    return rest


def test():
    prjnr = 11206021
    md = list()
    md = getlocationsfromtable(True, True, prjnr)
    return md
