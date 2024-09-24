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

# system pacakages for reading timeseries data from Oracle GeoLab database
import json
import os
import configparser
from sqlalchemy import create_engine, func, select


# Read default configuration from file
def read_config():
    """reads configuration file with configparser
       based on type of OS (NT is local, Linux is wps machine)

    Returns:
        Configparser object: configparser object with connection parameters
    """
    # Default config file (relative path, does not work on production, weird)
    if os.name == "nt":
        devpath = r"C:\develop\nobv_wps\processes"
        # devpath=r'C:\projecten\grondwater_monitoring'
        confpath = os.path.join(devpath, "nobvgl_configuration.txt")
    else:
        confpath = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "nobvgl_configuration.txt"
        )
    if not os.path.exists(confpath):
        confpath = "/opt/pywps/processes/nobvgl_configuration.txt"
    # Parse and load

    cf = configparser.ConfigParser()

    cf.read(confpath)
    return cf


def createconnectiontodb():
    """create connection with PostgreSQL database

    Returns:
        sqlalchemy engine: SQLalchemy engine
    """
    cf = read_config()
    user = cf.get("PostGIS", "user")
    pwd = cf.get("PostGIS", "pass")
    host = cf.get("PostGIS", "host")
    db = cf.get("PostGIS", "db")
    engine = create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:5432/{d}".format(u=user, p=pwd, h=host, d=db)
    )
    return engine


def gettsfromtable(measid, param, datefrom, dateto):
    """Call function gettimeseries in PostgreSQL database and return json with timeseries

    Args:
        projectnr (integer): projectnr
        measid (integer): the id of a location within the project
        param (text): parameter description
        datefrom (text): ISO8601 datetime YYYY-MM-DD
        dateto (text): ISO8601 datetime YYYY-MM-DD
        instrumentnr (integer): the number of the specific instrument (is not yet in the function!)

    Returns:
        json: timeseries incl. metadata about the type of parameter
    """
    engine = createconnectiontodb()
    print(measid, param, datefrom, dateto)
    with engine.connect() as c:
        query = select(func.gettimeseries(measid, param, datefrom, dateto))
        result = c.execute(query).fetchone()[0]
    return json.dumps(result)


def test():
    # no decision made yet to also pass parameter, for now the parameter name is couple to location id
    measid = 260462
    param = "regenval"
    datefrom = "2024-03-01"
    dateto = "2024-03-31"

    res = gettsfromtable(measid, param, datefrom, dateto)

    print(res)

    for i in res:
        for row in i:
            print(row)
