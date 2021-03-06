#!/usr/bin/env python

import json
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

p_value_significance_threshold = 0.001
min_iterations = 10
min_runtime_ns = 59 * 1000 * 1000 * 1000

def format_diff(diff):
    if diff >= 0:
        return colored('+' + "{0:.0%}".format(diff), 'green')
    else:
        return colored("{0:.0%}".format(diff), 'red')

def get_iteration_durations(iterations):
    # Sum up the parsing/optimization/execution/... durations of all statement of a query iteration
    # to a single entry in the result list.

    iteration_durations = []
    for iteration in iterations:
        iteration_duration = 0
        for statement in iteration["statements"]:
            iteration_duration += statement["sql_translation_duration"] + statement["optimization_duration"] + \
                                  statement["lqp_translation_duration"] + statement["plan_execution_duration"]
        iteration_duration += iteration["parse_duration"]

        iteration_durations.append(iteration_duration)

    return iteration_durations

def calculate_and_format_p_value(old, new):
    old_iteration_durations = get_iteration_durations(old["metrics"])
    new_iteration_durations = get_iteration_durations(new["metrics"])

    p_value = ttest_ind(array('d', old_iteration_durations), array('d', new_iteration_durations))[1]
    is_significant = p_value < p_value_significance_threshold

    notes = ""
    old_runtime = sum(runtime for runtime in old_iteration_durations)
    new_runtime = sum(runtime for runtime in new_iteration_durations)
    if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
        is_significant = False
        notes += "(run time too short) "

    if (len(old_iteration_durations) < min_iterations or len(new_iteration_durations) < min_iterations):
        is_significant = False
        notes += "(not enough runs) "

    color = 'green' if is_significant else 'white'
    return colored(notes + "{0:.4f}".format(p_value), color)


if(len(sys.argv) != 3):
    print("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json")
    exit()

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

table_data = []
table_data.append(["Benchmark", "prev. iter/s", "runs", "new iter/s", "runs", "change", "p-value (significant if <" + str(p_value_significance_threshold) + ")"])

average_diff_sum = 0.0

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
    name = old['name']
    if old['name'] != new['name']:
        name += ' -> ' + new['name']
    if float(old['items_per_second']) > 0.0:
        diff = float(new['items_per_second']) / float(old['items_per_second']) - 1
        average_diff_sum += diff
    else:
        diff = float('nan')

    diff_formatted = format_diff(diff)
    p_value_formatted = calculate_and_format_p_value(old, new)

    table_data.append([name, str(old['items_per_second']), str(len(old['metrics'])), str(new['items_per_second']), str(len(new['metrics'])), diff_formatted, p_value_formatted])

table_data.append(['average', '', '', '', '', format_diff(average_diff_sum / len(old_data['benchmarks'])), ''])

table = AsciiTable(table_data)
table.justify_columns[6] = 'right'

print("")
print(table.table)
print("")
