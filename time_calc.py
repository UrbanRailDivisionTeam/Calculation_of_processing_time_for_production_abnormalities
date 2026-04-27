from typing import Callable
from dataclasses import dataclass
from datetime import datetime,timedelta,date,time,timezone
from pandas import Timestamp

@dataclass
class Interval():
    left:timedelta
    right:timedelta
    @property
    def length(self):
        return self.right-self.left

def normalize_datetime(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

worktime = [
    Interval(timedelta(hours=8, minutes=30, seconds=0),timedelta(hours=12, minutes=0, seconds=0)),
    Interval(timedelta(hours=13, minutes=30, seconds=0),timedelta(hours=17, minutes=30, seconds=0))
]
'''一天内工作时间,0点开始,按小时记'''

is_workday: Callable[[datetime], bool] = lambda date: date.weekday() < 5


def real2work_hour(time_input: timedelta) -> timedelta:
    # assert(time >= timedelta(minutes=0) and time <= timedelta(hours=24))
    # 如果输入大于24小时,则输出为一天最大工作时间
    t = timedelta(microseconds=0)
    for i in worktime:
        if time_input > i.right:
            t += i.length
            continue
        elif time_input < i.left:
            continue
        t += time_input-i.left
    return t


def work2real_hour(duration: timedelta) -> timedelta:
    assert (duration >= timedelta(minutes=0) and
            duration <= real2work_hour(timedelta(hours=24)))
    t = duration
    i: Interval=Interval(timedelta(hours=0),timedelta(hours=0))
    for i in worktime:
        if t <= i.length:
            break
        t -= i.length
    return i.left + t


def real2work(time_input: datetime, base_time:datetime):
    time_input = normalize_datetime(time_input)
    base_time = normalize_datetime(base_time)
    t = base_time
    duration = timedelta(seconds=0)
    while (t < time_input):
        if is_workday(t):
            duration += real2work_hour(time_input-t)
        t += timedelta(days=1)
    return duration


def work2real(duration: timedelta, base_time:datetime):
    base_time = normalize_datetime(base_time)
    t = base_time
    d = duration
    worktime_fullday = real2work_hour(timedelta(hours=24))
    while (d > timedelta(seconds=0)):
        if is_workday(t):
            if d < worktime_fullday:
                break
            d -= worktime_fullday
        t += timedelta(days=1)
    return t + work2real_hour(d)


def worktime_add(time_input: datetime, duration: timedelta):
    '''
    使用输入时间向前取整作为基准时间
    '''
    time_input = normalize_datetime(time_input)
    base_time=datetime.combine( 
        date=time_input.date(),
        time=time(hour=0,minute=0,microsecond=0)
        )
    return work2real(
        real2work(time_input,base_time)+duration,
        base_time)


# def worktime_sub(time_input: datetime, timeOrDuration: timedelta | datetime):
#     '''
#     # 该函数有问题,暂时不要用
#     '''
#     if isinstance(timeOrDuration, timedelta):
#         base_time=datetime.combine( 
#             date=time_input.date(),
#             time=time(hour=0,minute=0,microsecond=0)
#         )
#         return work2real(
#             real2work(time_input,base_time)-timeOrDuration,
#             base_time)
#     elif isinstance(timeOrDuration, datetime):
#         base_time=datetime.combine( 
#             date=timeOrDuration.date(),
#             time=time(hour=0,minute=0,microsecond=0)
#         )
#         return real2work(time_input)-real2work(timeOrDuration)
#     else:
#         raise TypeError(timeOrDuration)
