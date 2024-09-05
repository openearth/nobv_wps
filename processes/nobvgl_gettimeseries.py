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
from sqlalchemy import create_engine


# Read default configuration from file
def read_config():
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
    cf = read_config()
    user = cf.get("PostGIS", "user")
    pwd = cf.get("PostGIS", "pass")
    host = cf.get("PostGIS", "host")
    db = cf.get("PostGIS", "db")
    engine = create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:5432/{d}".format(u=user, p=pwd, h=host, d=db)
    )
    return engine


def gettsfromtable(locid, parameter, projectnr):
    # haal voor deze loc_id de tijdreeksdata op.
    # first create connection
    print(locid, parameter)
    print("-------------")
    print(
        "work in progress here, apparantly the getts is empty, not sure if the correct things are called."
    )
    print("-------------")
    connection = createconnectiontodb()
    c = connection.cursor()
    # projectnr = 11206020
    # getts = c.callfunc('getTimeseries',cx_Oracle.DB_TYPE_CLOB,[projectnr,1,locid])

    # from august 2023 the parameters should be:
    projectnr = 11206020
    measid = 3
    param = "regenval"
    datefrom = "2023-01-29"
    dateto = "2023-01-31"
    getts = c.callfunc(
        "getTimeseries",
        [projectnr, measid, param, datefrom, dateto],
    )

    # TODO implement this 2024-09-04
    # getTimeSeries(11206020, 3, 'regenval', '2024-03-01', '2025-01-01');

    # getts = c.callfunc('getTimeseries',str,[projectnr,1,locid])
    print("wtf is getts:", getts)
    result = ""  # todo, no idea what the format is
    return result
    # selecting datetime / grondwaterstand / temperatuur
    # return json.dumps(result)


def gettsfromtable_ext(
    measid, parameter, projectnr=None, datestart=None, dateend=None, instrnr=None
):
    """Extended timeseries retrievement from one of the databases that are supported.
       It is assumed that if a projetnr is provided the Geolab database is the database that holds the timeseries data
       this is an important switch

    Args:
        measid (string): string indicating what location data is requested for
        parameter (string): parameter name
        projectnr (string, optional): _description_. Defaults to None.
        datestart (datetime, optional): ISO86001 date. Defaults to None.
        dateend (datetime, optional): ISO86001 date. Defaults to None.
        instrnr (string, optional): String indicating which instrumentnr or diver!. Defaults to None.

    Returns:
        json: json with timeseries data
    """
    # haal voor deze loc_id de tijdreeksdata op.
    # first create connection
    print(measid, parameter, projectnr, datestart, dateend, instrnr)
    print("-------------")

    connection = createconnectiontodb()
    c = connection.cursor()

    # from august 2023 the parameters should be:
    # projectnr = 11206020
    # datestart = '2023-01-29'
    # dateend   = '2023-01-31'
    getts = c.callfunc(
        "getTimeseries",
        [projectnr, measid, parameter, datestart, dateend],
    )

    # getts = c.callfunc('getTimeseries',str,[projectnr,1,locid])
    if getts is None:
        print("No results retreived")
        return None
    else:
        result = ""  # TODO no idea what the object type is
        return result
    # selecting datetime / grondwaterstand / temperatuur
    # return json.dumps(result)


def test():
    # no decision made yet to also pass parameter, for now the parameter name is couple to location id
    dictp = {}
    dictp["1"] = "Extensometer"
    dictp["2"] = "Extensometer"
    dictp["3"] = "Regenmeter"
    dictp["4"] = "Regenmeter"

    lstlocs = ("Cabauw_1", "Cabauw_3", "Hazerwoude_2", "Hazerwoude_4")
    for locid in lstlocs:
        measid = locid.split("_")[1]
        param = dictp[measid].lower()
        res = gettsfromtable_ext(
            int(measid),
            param,
            projectnr=None,
            datestart=None,
            dateend=None,
            instrnr=None,
        )
        print("data for", locid)
        print(res)
