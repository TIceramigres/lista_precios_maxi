from datetime import date, timedelta, datetime
import calendar
from dateutil.relativedelta import relativedelta

def getDate(fecha = None):
    if fecha == None:
        today = date.today()
        return today
    else:
        return fecha

def getYesterday(fecha):
    yesterday = fecha - timedelta(days=1)
    return yesterday

def tomorrow(fecha):
    yesterday = fecha + timedelta(days=1)
    return yesterday

def getBeforeYesterday(fecha):
    yesterday = fecha - timedelta(days=2)
    return yesterday

def getFirstDayMonth(fecha):
    first_day = date(fecha.year, fecha.month, 1)
    return first_day

def getLastDayMonth(fecha):
    last_day = calendar.monthrange(fecha.year, fecha.month)[1]
    last_day = f"{fecha.year}-{fecha.month:02d}-{last_day:02d}"
    last_day = datetime.strptime(last_day, "%Y-%m-%d").date()
    return last_day

def previousMonth(fecha):
    previous_month = fecha - relativedelta(months=1)
    return previous_month

def sixMonths(fecha):
    six_months = fecha - relativedelta(months=6)
    return six_months

def lastYear(fecha):
    year_last = fecha - relativedelta(days=365)
    return year_last
