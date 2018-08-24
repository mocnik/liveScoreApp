""" OEVENT 2 XML """

from collections import defaultdict
from datetime import datetime, timedelta

import pyxb.utils.domutils

from db import get_table, get_competitor_by_chip_number, query_db, get_wre_table
from iof import ResultStatus, ResultList, PersonResult, Person, PersonName, Namespace, \
    PersonRaceResult, Organisation, ClassResult, Class, STD_ANON, DateAndOptionalTime, Event, SplitTime, Id

pyxb.utils.domutils.BindingDOMSupport.SetDefaultNamespace(Namespace)

STATUS_CODES = {
    0: ResultStatus.Active,
    1: ResultStatus.OK,
    2: ResultStatus.Disqualified,
    3: ResultStatus.DidNotFinish,
    4: ResultStatus.DidNotStart,
    5: ResultStatus.MissingPunch
}

STATUS_CODES_SORT = {
    ResultStatus.OK: 0,
    ResultStatus.Disqualified: 1,
    ResultStatus.DidNotFinish: 1,
    ResultStatus.DidNotStart: 1,
    ResultStatus.MissingPunch: 1
}

OFFICIAL_CATEGORIES = {'M21E': 'Men', 'W21E': 'Women', 'M21E SEEOC': 'Men', 'W21E SEEOC': 'Women'}


def to_person_result(competitor, start_time, stage, winning_time=None):
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
    if 'WREID' in competitor and competitor['WREID']:
        iof_id = Id(int(competitor['WREID']), type="IOF")
        x_result.Person.Id.append(iof_id)
    if competitor['CLUBLONGNAME']:
        x_result.Organisation = Organisation.Factory(
            Name=competitor['CLUBLONGNAME'], ShortName=competitor['CLUBSHORTNAME'])

    x_person_result = PersonRaceResult()
    x_person_result.Status = STATUS_CODES[competitor['FINISHTYPE' + stage]]
    x_person_result.StartTime = start.isoformat() + "+02:00"
    if competitor['COMPETITIONTIME' + stage]:
        x_person_result.Time = competitor['COMPETITIONTIME' + stage] / 100
        if winning_time and competitor['FINISHTYPE' + stage] == 1:
            x_person_result.TimeBehind = (competitor['COMPETITIONTIME' + stage] - winning_time) / 100

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


def to_class_result(category, start_time, stage, use_behind=False):
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
    if use_behind:
        x_class_result.Class = Class.Factory(
            Name=OFFICIAL_CATEGORIES[category[0]['CATEGORYNAME']],
            ShortName=OFFICIAL_CATEGORIES[category[0]['CATEGORYNAME']])
        winning_time = min(
            competitor['COMPETITIONTIME' + stage] for competitor in category if competitor['FINISHTYPE' + stage] == 1)

    if use_behind:
        results = [to_person_result(competitor, start_time, stage, winning_time) for competitor in category]
        # print(results[0].Result[0].Time)
        # exit()
        sorted_results = sorted(results, key=sort_key_results)
        for i, result in enumerate(sorted_results):
            if result.Result[0].Status == ResultStatus.OK:
                result.Result[0].Position = i + 1
        x_class_result.PersonResult.extend(sorted_results)
    else:
        for competitor in category:
            x_class_result.PersonResult.append(to_person_result(competitor, start_time, stage))
    return x_class_result


def sort_key_results(result):
    return STATUS_CODES_SORT[result.Result[0].Status], result.Result[0].Time if result.Result[0].Time else 999999999


def to_official_results(competition, categories, stage):
    categories_data = defaultdict(list)
    for _, competitors in categories.items():
        category = competitors[0]['CATEGORYNAME']
        if category in OFFICIAL_CATEGORIES.keys():
            categories_data[OFFICIAL_CATEGORIES[category]].extend([competitor for competitor in competitors if competitor['WREID']])
    return to_result_list(competition, categories_data, stage, STD_ANON.Complete)


def to_result_list(competition, categories, stage, status=STD_ANON.Snapshot):
    """
    Args:
        competition: competition data from OEVENT db
        categories: competitors data from OEVENT db
        stage: competition stage
        status: results status
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
    x_result_list.creator = "liveScoreApp v0.1"
    x_result_list.status = status

    for _, category in categories.items():
        x_result_list.ClassResult.append(to_class_result(category, start_time, stage, True))

    return x_result_list


def to_xml(conn_fb, conn_sql, stage='1', official=False):
    """
    Connects to the db, retrieves competition data and generates IOF v3 ResultList xml

    Returns:
        str: results in IOF v3 xml format
    """
    competition = get_table(conn_fb, 'OEVCOMPETITION')[0]
    if official:
        competitors = get_wre_table(conn_fb)
    else:
        competitors = get_table(conn_fb, 'OEVLISTSVIEW')

    categories = defaultdict(list)

    punches = query_db(conn_sql, 'SELECT chipNumber, time FROM punches WHERE stationCode = 0 AND stage = ?', (stage,))
    punch_dict = {p[0]: p[1] for p in punches}

    for competitor in competitors:
        if (not competitor['ISVACANT']) and competitor['ISRUNNING' + stage] and competitor['STARTTIME' + stage] is not None:
            if competitor['CHIPNUMBER' + stage] in punch_dict:
                competitor['SPLITTIME'] = (0, punch_dict[competitor['CHIPNUMBER' + stage]])
            categories[competitor['CATEGORYID']].append(competitor)

    if official:
        x_result_list = to_official_results(competition, categories, stage)
    else:
        x_result_list = to_result_list(competition, categories, stage)

    return x_result_list.toxml("utf-8")


def punch_xml(conn, chip_number, station_code, time, stage='1'):
    competitors = get_competitor_by_chip_number(conn, chip_number, stage)
    competition = get_table(conn, "OEVCOMPETITION")[0]

    categories = defaultdict(list)

    for competitor in competitors:
        if (not competitor['ISVACANT']) and competitor['ISRUNNING' + stage]:
            competitor['SPLITTIME'] = (station_code, time)
            categories[competitor['CATEGORYID']].append(competitor)

    x_result_list = to_result_list(competition, categories, stage)

    return x_result_list.toxml("utf-8")
