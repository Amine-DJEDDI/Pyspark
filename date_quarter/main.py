
import datetime
from datetime import date

import dateutil.rrule as rrule

def previous_quarter(date):
    date = datetime.datetime(date.year, date.month, date.day)
    rr = rrule.rrule(
        rrule.DAILY,
        bymonth=(3,6,9,12),    # the month must be one of these
        bymonthday=-1,         # the day has to be the last of the month
        dtstart = date-datetime.timedelta(days=100))
    result = rr.before(date, inc=False)  # inc=False ensures result < date
    return result.date()


#print(previous_quarter(DT.date(2021, 6, 30)))



def previous_quarter(ref):
    if ref.month < 4:
        list1 = [datetime.date(ref.year - 1, 12, 31),
                            datetime.date(ref.year - 1, 9, 30),
                            datetime.date(ref.year - 1, 6, 30),
                            datetime.date(ref.year - 1, 3, 31)]

        list1 = [i.strftime('%Y%m%d') for i in list1]

        return list1

    elif ref.month < 7:
        return [datetime.date(ref.year, 3, 31),
                datetime.date(ref.year - 1, 12, 31),
                datetime.date(ref.year - 1, 9, 30),
                datetime.date(ref.year - 1, 6, 30)]
    elif ref.month < 10:
        return [datetime.date(ref.year, 6, 30),
                datetime.date(ref.year, 3, 31),
                datetime.date(ref.year - 1, 12, 31),
                datetime.date(ref.year - 1, 9, 30)]

    return [datetime.date(ref.year, 9, 30),
            datetime.date(ref.year, 6, 30),
            datetime.date(ref.year, 3, 30),
            datetime.date(ref.year - 1, 12, 31)]


today = date.today()
#listodate = previous_quarter(today)
listodate = previous_quarter(datetime.date(2013, 3, 31))
print(listodate)


