# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval


def _split_timedelta(t):
    days = t.days
    hours, seconds = divmod(t.seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    return days, hours, minutes, seconds


def hhmm(t):
    days, hours, minutes, _ = _split_timedelta(t)
    return "{:02d}:{:02d}".format(24 * days + hours, minutes)


def hhmmss(t):
    days, hours, minutes, seconds = _split_timedelta(t)
    return "{:02d}:{:02d}:{:02d}".format(24 * days + hours, minutes, seconds)
