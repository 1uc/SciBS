def hhmm(t):
    days = t.days
    hours, seconds = divmod(t.seconds, 3600)
    minutes, _ = divmod(seconds, 60)
    return "{:02d}:{:02d}".format(24 * days + hours, minutes)
