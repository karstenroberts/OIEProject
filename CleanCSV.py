import csv
from datetime import datetime
from enum import Enum
import random

class Cols(Enum):
    ID = 0
    CALL_RECIEVED = 2
    OFFICER_DISPATCHED = 3
    OFFICER_ARRIVED = 4
    CALL_RESOLVED = 5
    PRIORITY = 6


SAMPLE_SIZE = 6000


def main():
    with open('raw_police_data.csv') as raw_csv:

        #Make the cleaned csv without calls with empty cols
        clean_csv = make_clean_csv(raw_csv)

        # Count hourly call distribution
        hour_count = dict()
        call_times = list()
        call_wait_times_raw = list()
        officer_travel_times_raw = list()
        call_duration_times_raw = list()
        for row in clean_csv:
            time_call_received = datetime.strptime(row[Cols.CALL_RECIEVED.value], '%d %b %Y %H:%M:%S')
            time_officer_dispatched = datetime.strptime(row[Cols.OFFICER_DISPATCHED.value], '%d %b %Y %H:%M:%S')
            time_officer_arrived = datetime.strptime(row[Cols.OFFICER_ARRIVED.value], '%d %b %Y %H:%M:%S')
            time_call_resolved = datetime.strptime(row[Cols.CALL_RESOLVED.value], '%d %b %Y %H:%M:%S')
            if time_call_received.hour in hour_count:
                hour_count[time_call_received.hour] = hour_count[time_call_received.hour] + 1
            else:
                hour_count[time_call_received.hour] = 1
            call_times.append("{:d}".format(time_call_received.hour))
            call_wait_times_raw.append(int((time_officer_dispatched - time_call_received).total_seconds()/60))
            officer_travel_times_raw.append(int((time_officer_arrived - time_officer_dispatched).total_seconds()/60))
            call_duration_times_raw.append(int((time_call_resolved - time_officer_arrived).total_seconds()/60))


        # call_duration_times = reservoir_sampling(clean_list(call_duration_times_raw), SAMPLE_SIZE)
        # call_wait_times = reservoir_sampling(clean_list(call_wait_times_raw), SAMPLE_SIZE)
        # officer_travel_times = reservoir_sampling(clean_list(officer_travel_times_raw), SAMPLE_SIZE)
        # call_duration_times_zeroes = reservoir_sampling(call_duration_times_raw, SAMPLE_SIZE)
        # call_wait_times_zeroes = reservoir_sampling(call_wait_times_raw, SAMPLE_SIZE)
        # officer_travel_times_zeroes = reservoir_sampling(officer_travel_times_raw, SAMPLE_SIZE)
        call_wait_times = list(clean_list(call_wait_times_raw))
        officer_travel_times = list(clean_list(officer_travel_times_raw))
        call_duration_times = list(clean_list(call_duration_times_raw))

        gen_input_file(call_times, "call_time_dist.txt")
        gen_input_file(call_wait_times, "call_wait_time_dist.txt")
        gen_input_file(officer_travel_times, "officer_travel_time_dist.txt")
        gen_input_file(call_duration_times, "call_duration_time_dist.txt")
        # gen_input_file(call_duration_times_zeroes, "call_duration_times_zeroes.txt")
        # gen_input_file(call_wait_times_zeroes, "call_wait_times_zeroes.txt")
        # gen_input_file(officer_travel_times_zeroes, "officer_travel_times_zeroes")

        # Order hours by decreasing call volume
        call_volume_hourly = order_call_vol(hour_count)

        with open('clean_police_data.csv', mode='w') as cleaned_csv:
            csv_writer = csv.writer(cleaned_csv, delimiter=',')
            csv_writer.writerows(clean_csv)

        print("Decreasing order of call quantities:")
        with open('hour_order.csv', mode='w') as hour_order_csv:
            csv_writer = csv.writer(hour_order_csv, delimiter=',')
            for row in call_volume_hourly:
                print("Hour: {}, Calls: {}".format(row[0], row[1]))
                csv_writer.writerow(row)
        print("Done!")


# Modified from http://data-analytics-tools.blogspot.com/2009/09/reservoir-sampling-algorithm-in-perl.html
def reservoir_sampling(raw_list, desired_len):
    sample = list()
    for i, line in enumerate(raw_list):
        if i < desired_len:
            sample.append(line)
        elif i >= desired_len and random.random() < desired_len / float(i + 1):
            replace = random.randint(0, len(sample) - 1)
            sample[replace] = line
    return sample

def clean_list(raw_list):
    return filter(lambda a: (a >= 0), raw_list)

def make_clean_csv(raw_csv):
    csv_reader = csv.reader(raw_csv, delimiter='\t', dialect='excel')
    # Create cleaned data
    # clean_csv columns: id, call recieved, call recieved, dispatch time, officer arrival time, clear time, priority
    clean_csv = list()
    curr_row = 0
    header = list()
    for row in csv_reader:
        if curr_row == 0:
            header = row
        else:
            if valid_call(row):
                clean_csv.append(row)
        curr_row += 1
    print(curr_row)
    return clean_csv

def order_call_vol(hour_count):
    call_volume_hourly = list()
    removed = list()
    for entry in hour_count:
        max_hour = 0
        max_calls = -1
        for key, value in hour_count.items():
            if key not in removed:
                if max_calls < value:
                    max_calls = value
                    max_hour = key
        removed.append(max_hour)
        call_volume_hourly.append((max_hour, max_calls))
    return call_volume_hourly


def gen_input_file(times, filename):
    with open(filename, mode='w') as time_dist:
        for time in times:
            time_dist.write("{}\n".format(time))
    time_dist.close()


def valid_call(row):
    for col in row:
        if col == '':
            return False
    return True


if __name__ == "__main__":
    main()