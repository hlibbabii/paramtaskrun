#!/usr/bin/env python

from collections import OrderedDict, defaultdict
import json
import logging
import os
import pickle
import subprocess
from deepdiff import DeepDiff
import time

__author__ = 'hlib'

logging.basicConfig(level=logging.DEBUG)


class UniqueList(list):
    def append(self, p_object):
        if p_object in self:
            self.remove(p_object)
        super(UniqueList, self).append(p_object)


class Command(object):
    def __init__(self, file):
        self.file = file

    def execute(self, params, non_tunable_params):
        command_with_replaced_params = self.__replace_params_in_command(self.file, non_tunable_params)
        command_string_list = command_with_replaced_params.split()
        for param, value in params.items():
            command_string_list.extend(["--" + param, str(value)])
        logging.debug("Running: {}".format(" ".join(command_string_list)))
        try:
            subprocess.check_call(command_string_list)
        except subprocess.CalledProcessError:
            return False
        return True

    @staticmethod
    def __replace_params_in_command(command, non_tunable_params):
        for i in range(5):
            command = command.format(**non_tunable_params)
        return command


class Task(object):
    def __init__(self, id, description, command, params):
        self.id = id
        self.description = description
        self.dependent_tasks = []
        self.command = self.__parse_command(command)
        self.params = params

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(str(self))

    def run(self, param_values, non_tunable_param_values):
        param_values_for_this_task = {param: param_values[param] for param in self.params}
        return self.command.execute(param_values_for_this_task, non_tunable_param_values)

    @staticmethod
    def __parse_command(command):
        return Command(command)

    def __str__(self):
        return self.id

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return self.__str__()


class TaskRunStats(object):
    def __init__(self):
        self._successful = None
        self._rerun_needed = False
        self._why_rerun = None
        self.time_seconds = 0

    @property
    def successful(self):
        return self._successful

    @successful.setter
    def successful(self, successful):
        self._successful = successful

    @property
    def rerun_needed(self):
        return self._rerun_needed

    @rerun_needed.setter
    def rerun_needed(self, rerun_needed):
        self._rerun_needed = rerun_needed

    @property
    def why_rerun(self):
        return self._why_rerun

    @why_rerun.setter
    def why_rerun(self, why_rerun):
        self._why_rerun = why_rerun

    def __str__(self):
        return str(self.successful)

    def __repr__(self):
        return self.__str__()


class ProjectRun(object):
    def __init__(self, tasks_in_execution_order, param_values, non_tunable_param_values):
        self.taskRunStats = OrderedDict([(task, TaskRunStats()) for task in tasks_in_execution_order])

        self._successful = None
        self._param_values = param_values
        self._non_tunable_params = non_tunable_param_values

    @classmethod
    def get_dummy_project_run(cls):
        project_run = cls([], {}, {})
        project_run.successful = True
        return project_run

    def mark_tasks_as_needed_to_run(self, tasks):
        for task, why_rerun in tasks:
            runStats = self.taskRunStats[task]
            runStats.rerun_needed = True
            runStats.why_rerun = why_rerun

    @property
    def successful(self):
        return self._successful

    @successful.setter
    def successful(self, successful):
        self._successful = successful

    def get_tasks(self):
        return self.taskRunStats.keys()

    def get_failed_tasks(self):
        return filter(lambda k: not self.taskRunStats[k].successful, self.taskRunStats.keys())

    def get_params(self):
        return self._param_values

    def execute(self):
        n_tasks = len(self.taskRunStats)
        params = self.get_params()
        non_tunable_params = self.get_non_tunable_params()
        project_run_succeded = True
        for index, (task, task_stats) in enumerate(self.taskRunStats.items()):
            if task_stats.rerun_needed:
                if project_run_succeded:
                    logging.info("Task {} out of {} [{}]({})".format(str(index + 1), str(n_tasks), task.id, task_stats.why_rerun))
                    start_time = time.time()
                    success = task.run(params, non_tunable_params)
                    task_stats.time_seconds = time.time() - start_time
                    task_stats.successful = success
                    result_str = "success" if success else "failure"
                    logging.info("Task {} out of {} [{}]({}): {} [{:d} s]".format(str(index + 1),
                                                                                  str(n_tasks),
                                                                                  task.id,
                                                                                  task_stats.why_rerun,
                                                                                  result_str,
                                                                                  int(task_stats.time_seconds)))
                    if not success:
                        project_run_succeded = False
                else:
                    task_stats.successful = True
            else:
                task_stats.successful = True
                logging.info("Task {} out of {} [{}]-- up-to-date".format(str(index + 1), str(n_tasks), task.id))

        self.successful = project_run_succeded

    def get_non_tunable_params(self):
        return self._non_tunable_params

    def __str__(self):
        return str(self.taskRunStats)

    def __repr__(self):
        return self.__str__()


class CyclicDependencyError(Exception):
    pass


class DuplicateIdException(Exception):
    pass


class UnknownParamError(Exception):
    pass


class Project(object):
    def __init__(self, config_file, prev_runs_file):
        self._prev_runs_file = prev_runs_file
        self._init_project_from_config(config_file)
        self.runs = self._load_prev_runs(prev_runs_file)
        if not self.runs:
            self.runs.append(ProjectRun.get_dummy_project_run())
        self.were_successful_runs_after_last_config_refresh = self.last_run().successful

    def __str__(self):
        return str(self.tasks_in_execution_order[:5])

    def last_run(self):
        return self.runs[-1]

    def __get_task_execution_order(self):
        execution_order = UniqueList()
        for starting_node in self.starting_nodes:
            queue = [starting_node]
            while queue:
                current_node = queue.pop(0)
                execution_order.append(current_node)
                queue.extend(current_node.dependent_tasks)
        return execution_order

    def run(self, params_file,  non_tunable_params_file, tasks_to_rerun=[], force_rerun_all=False):
        with open(params_file, 'r') as f:
            params_values = json.load(f)
        with open(non_tunable_params_file, 'r') as f:
            non_tunable_params_values = json.load(f)
        project_run = ProjectRun(self.tasks_in_execution_order, params_values, non_tunable_params_values)

        if force_rerun_all:
            tasks_needed_to_rerun = [(task,'rerun-all') for task in self.tasks_in_execution_order]
        else:
            affected_tasks = []
            affected_tasks.extend([(task, 'failed during last run') for task in self.tasks_in_execution_order if task in self.last_run().get_failed_tasks()])
            new_tasks = self.__get_new_tasks()
            if new_tasks:
                logging.info("Found new task(s): " + str(new_tasks))
                affected_tasks.extend([(task, 'new') for task in new_tasks])
            if tasks_to_rerun:
                logging.info("Rerunning of the following tasks requested by user: " + str(tasks_to_rerun))
                affected_tasks.extend([(task, 'requested-rerun') for task in self.tasks_in_execution_order if task.id in tasks_to_rerun])
            changed_params = self.__get_changed_params(params_values, self.last_run().get_params())
            affected_tasks.extend([(task, 'param-change') for param in changed_params for task in self.param_to_affected_tasks[param]])

            affected_tasks_wo_reasons = [t for t, why in affected_tasks]
            transitively_affected_tasks = self.__get_transitively_affected_tasks(affected_tasks_wo_reasons)
            only_trans_affected_tasks = transitively_affected_tasks.difference(affected_tasks_wo_reasons)

            tasks_needed_to_rerun = affected_tasks
            tasks_needed_to_rerun.extend([(task, 'transit') for task in only_trans_affected_tasks])

        project_run.mark_tasks_as_needed_to_run(tasks_needed_to_rerun)
        project_run.execute()

        self.runs.append(project_run)
        with open(self._prev_runs_file, 'wb') as f:
            pickle.dump(self.runs, f)
        return project_run

    def run_all(self):
        self.run([], force_rerun_all=True)

    @staticmethod
    def __get_transitively_affected_tasks(tasks):
        affected_tasks = set()
        queue = [task for task in tasks]
        while queue:
            current_node = queue.pop(0)
            affected_tasks.add(current_node)
            queue.extend(current_node.dependent_tasks)
        return affected_tasks

    def __get_new_tasks(self):
        return [task for task in self.tasks_in_execution_order if task not in self.last_run().get_tasks()]

    def _init_project_from_config(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        self.starting_nodes = self.__build_task_graph(config)
        if not self.starting_nodes:
            raise CyclicDependencyError("No independent tasks -> Cyclic dependency is present")
        self.__check_for_cyclic_dependencies(self.starting_nodes)
        self.tasks_in_execution_order = self.__get_task_execution_order()

    def __build_task_graph(self, config):
        logging.debug("Building task graph")
        task_list = []
        pending_dependencies = defaultdict(list)
        starting_nodes = []
        self.param_to_affected_tasks = defaultdict(list)
        for json_task in config['tasks']:
            if 'enabled' in json_task and json_task['enabled'].lower() == 'false':
                continue
            current_task = Task(json_task['id'], json_task['description'], json_task['command'], json_task['params'])
            for param in current_task.params:
                self.param_to_affected_tasks[param].append(current_task)
            depends_on_list = json_task['depends-on']
            if not depends_on_list:
                starting_nodes.append(current_task)
            if current_task.id in pending_dependencies:
                dependencies_for_current_task = pending_dependencies[current_task.id]
                current_task.dependent_tasks.extend(dependencies_for_current_task)
                del pending_dependencies[current_task.id]
            for task in task_list:
                if task.id == current_task.id:
                    raise DuplicateIdException("There are at least 2 tasks with the same id: {}".format(task.id))
                if task.id in depends_on_list:
                    depends_on_list.remove(task.id)
                    task.dependent_tasks.append(current_task)
            for depends_on in depends_on_list:
                pending_dependencies[depends_on].append(current_task)
            task_list.append(current_task)
        pending_dependencies.default_factory = None
        if pending_dependencies:
            raise CyclicDependencyError("Some tasks depend on the following non-existent tasks or on themselves: " + ",".join(pending_dependencies.keys()))
        logging.debug("Task graph build successfully. The graph contains " + str(len(task_list)) + " tasks")
        return starting_nodes

    @staticmethod
    def _load_prev_runs(prev_runs_file):
        if not os.path.exists(prev_runs_file):
            logging.info("Previous runs file " + prev_runs_file + " not found")
            return []
        else:
            logging.debug("Loading previous runs from " + prev_runs_file)
            with open(prev_runs_file, 'rb') as f:
                return pickle.load(f)

    @staticmethod
    def __get_changed_params(params_values, last_param_values):
        changed_params = []
        all_params = set(params_values.keys()).union(set(last_param_values.keys()))
        for param in all_params:
            if param not in last_param_values or \
                    (param in params_values and
                     DeepDiff(params_values[param], last_param_values[param]) != {}):
                changed_params.append(param)
        return changed_params

    @staticmethod
    def __check_for_cyclic_dependencies(starting_nodes):
        visited = set()
        non_cyclic = set()
        stack = [s for s in starting_nodes]
        while stack:
            current = stack[-1]
            visited.add(current)
            not_visited_nexts = []
            for next in current.dependent_tasks:
                if next in visited:
                    if next not in non_cyclic:
                        raise CyclicDependencyError(current)
                else:
                    not_visited_nexts.append(next)
            if not not_visited_nexts:
                non_cyclic.add(current)
                stack.pop()
            else:
                stack.extend(not_visited_nexts)

