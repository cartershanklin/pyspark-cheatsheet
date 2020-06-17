#!/usr/bin/env python3

import calendar
import csv
import dateparser
import datetime
import itertools
import scipy.stats

import numpy as np

def date_examples():
    with open("date_examples_dates.txt") as fd:
        all_dates = fd.read().split("\n")[:-1]
    with open("date_examples_times.txt") as fd:
        all_times = fd.read().split("\n")[:-1]

    with open("date_examples.csv", "w") as fd:
        writer = csv.writer(fd)
        writer.writerow(["date"])
        for entry in itertools.product(all_dates, all_times):
            text = ' '.join(entry)
            writer.writerow([text])

def customer_spend():
    number_customers = 100
    start_date = "2020-01-01"
    end_date = "2023-01-01"

    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    np.random.seed(seed=0)

    with open("customer_spend.csv", "w") as fd:
        writer = csv.writer(fd)
        writer.writerow(["date", "customer_id", "spend_dollars"])
        current_date = start
        while current_date < end:
            first, last = calendar.monthrange(current_date.year, current_date.month)
            if current_date.day == last:
                date_string = current_date.strftime("%Y-%m-%d")
                for customer_id in range(number_customers):
                    total_days = (current_date - start).days
                    if total_days < customer_id * 7:
                        continue
                    multiplier = (((customer_id % 45) + 1) / 5) ** 2
                    spend_dollars = (scipy.stats.f.rvs(4, 5) - 0.5) * multiplier
                    if spend_dollars <= 0:
                        continue
                    spend_dollars = '${:,.2f}'.format(spend_dollars)
                    record = [ date_string, customer_id, spend_dollars ]
                    writer.writerow(record)
            current_date += datetime.timedelta(days=1)

customer_spend()
date_examples()
