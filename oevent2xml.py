""" OEVENT 2 XML """

from collections import defaultdict
from datetime import datetime, timedelta

import pyxb.utils.domutils

from db import get_table, get_competitor_by_chip_number, query_db
from iof import ResultStatus, ResultList, PersonResult, Person, PersonName, Namespace, \
    PersonRaceResult, Organisation, ClassResult, Class, STD_ANON, DateAndOptionalTime, Event, SplitTime

pyxb.utils.domutils.BindingDOMSupport.SetDefaultNamespace(Namespace)

STATUS_CODES = {
    0: ResultStatus.Active,
    1: ResultStatus.OK,
    2: ResultStatus.Disqualified,
    3: ResultStatus.DidNotFinish,
    4: ResultStatus.DidNotStart,
    5: ResultStatus.MissingPunch
}


def to_person_result(competitor, start_time, stage):
    """
    Args:
        competitor: competitor data from OEVENT db
        start_time: competition start time
        stage: competition stage
    Returns:
        iof.PersonResult
    """
    start = start_time + timedelta(seconds=competitor['STARTTIME' + stage] / 100)

    x_result = PersonResult()
    x_result.Person = Person.Factory(Name=PersonName.Factory(
        Given=competitor['FIRSTNAME'], Family=competitor['LASTNAME']))
    if competitor['CLUBLONGNAME']:
        x_result.Organisation = Organisation.Factory(
            Name=competitor['CLUBLONGNAME'], ShortName=competitor['CLUBSHORTNAME'])

    x_person_result = PersonRaceResult()
    x_person_result.Status = STATUS_CODES[competitor['FINISHTYPE' + stage]]
    x_person_result.StartTime = start.isoformat() + "+02:00"
    if competitor['COMPETITIONTIME' + stage]:
        x_person_result.Time = competitor['COMPETITIONTIME' + stage] / 100

    if 'SPLITTIME' in competitor:
        station_code, punch_time = competitor['SPLITTIME']
        punch = datetime.fromtimestamp(punch_time)
        running_time = punch - start
        if station_code <= 10:
            if not competitor['COMPETITIONTIME' + stage] and not competitor['FINISHTYPE' + stage]:
                x_person_result.Time = running_time.total_seconds()
                x_person_result.Status = ResultStatus.OK
        else:
            split = SplitTime.Factory(ControlCode=str(station_code), Time=running_time.total_seconds())
            x_person_result.SplitTime.append(split)

    x_result.Result.append(x_person_result)
    return x_result


def to_class_result(category, start_time, stage):
    """
    Args:
        category: competitors for category from OEVENT db
        start_time: competition start time
        stage: competition stage
    Returns:
        iof.ClassResult
    """
    x_class_result = ClassResult()
    x_class_result.Class = Class.Factory(
        Name=category[0]['CATEGORYNAME'], ShortName=category[0]['CATEGORYNAME'])

    for competitor in category:
        x_class_result.PersonResult.append(to_person_result(competitor, start_time, stage))
    return x_class_result


def to_result_list(competition, categories, stage):
    """
    Args:
        competition: competition data from OEVENT db
        categories: competitors data from OEVENT db
        stage: competition stage
    Returns:
        iof.ResultList
    """
    start_time = competition['DATE' + stage] + timedelta(seconds=competition['FIRSTSTART' + stage])
    x_start_time = DateAndOptionalTime.Factory(
        Date=start_time.date().isoformat(), Time=start_time.time().isoformat() + "+02:00")
    x_event = Event.Factory(Name=competition['COMPETITIONNAME'], StartTime=x_start_time)

    x_result_list = ResultList()
    x_result_list.iofVersion = "3.0"
    x_result_list.Event = x_event
    x_result_list.createTime = datetime.now().isoformat() + "+02:00"
    x_result_list.creator = "OEVENT2XML v0.1"
    x_result_list.status = STD_ANON.Snapshot

    for _, category in categories.items():
        x_result_list.ClassResult.append(to_class_result(category, start_time, stage))

    return x_result_list


def to_xml(conn_fb, conn_sql, stage='1'):
    """
    Connects to the db, retrieves competition data and generates IOF v3 ResultList xml

    Returns:
        str: results in IOF v3 xml format
    """
    competition = get_table(conn_fb, 'OEVCOMPETITION')[0]
    competitors = get_table(conn_fb, 'OEVLISTSVIEW')

    categories = defaultdict(list)

    punches = query_db(conn_sql, 'SELECT chipNumber, time FROM punches WHERE stationCode = 0')
    punch_dict = {p[0]: p[1] for p in punches}

    for competitor in competitors:
        if (not competitor['ISVACANT']) and competitor['ISRUNNING' + stage]:
            if competitor['CHIPNUMBER' + stage] in punch_dict:
                competitor['SPLITTIME'] = (0, punch_dict[competitor['CHIPNUMBER' + stage]])
            categories[competitor['CATEGORYID']].append(competitor)

    x_result_list = to_result_list(competition, categories, stage)

    return x_result_list.toxml("utf-8")


def punch_xml(conn, chip_number, station_code, time, stage='1'):
    competitors = get_competitor_by_chip_number(conn, chip_number)
    competition = get_table(conn, "OEVCOMPETITION")[0]

    categories = defaultdict(list)

    for competitor in competitors:
        if (not competitor['ISVACANT']) and competitor['ISRUNNING' + stage]:
            competitor['SPLITTIME'] = (station_code, time)
            categories[competitor['CATEGORYID']].append(competitor)

    x_result_list = to_result_list(competition, categories, stage)

    return x_result_list.toxml("utf-8")
