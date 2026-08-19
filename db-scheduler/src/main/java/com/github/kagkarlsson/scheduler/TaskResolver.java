/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import static java.util.function.Function.identity;

import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class TaskResolver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskResolver.class);
  private final SchedulerListeners schedulerListeners;
  private final Clock clock;
  private final String namespace;
  private final Map<String, Task> taskMap;
  private final Map<String, UnresolvedTask> unresolvedTasks = new ConcurrentHashMap<>();

  public TaskResolver(
      SchedulerListeners schedulerListeners, Clock clock, List<Task<?>> knownTasks) {
    this(schedulerListeners, clock, "", knownTasks);
  }

  /**
   * @param namespace task-name prefix this scheduler owns, or empty for the whole table
   */
  public TaskResolver(
      SchedulerListeners schedulerListeners,
      Clock clock,
      String namespace,
      List<Task<?>> knownTasks) {
    validateNamespace(namespace, knownTasks);
    this.schedulerListeners = schedulerListeners;
    this.clock = clock;
    this.namespace = namespace;
    this.taskMap = knownTasks.stream().collect(Collectors.toMap(Task::getName, identity()));
  }

  /**
   * Rejects a namespace that cannot be used as a SQL {@code like}-prefix, and task-names that fall
   * outside it. A task named outside its scheduler's namespace would silently never execute.
   */
  private static void validateNamespace(String namespace, List<Task<?>> knownTasks) {
    if (namespace.isEmpty()) {
      return;
    }
    if (namespace.contains("%") || namespace.contains("_") || namespace.contains("\\")) {
      throw new IllegalArgumentException(
          "Task-namespace '"
              + namespace
              + "' must not contain the SQL like-wildcards '%', '_' or the escape-character '\\'.");
    }
    if (Character.isLetterOrDigit(namespace.charAt(namespace.length() - 1))) {
      // Without a separator, namespace 'reports' would also own 'reportsarchive/...' and delete
      // those executions once they had been unresolved for deleteUnresolvedAfter.
      throw new IllegalArgumentException(
          "Task-namespace '"
              + namespace
              + "' must end with a separator character, e.g. '"
              + namespace
              + "/', so that it cannot also match task-names of a namespace it merely prefixes.");
    }
    List<String> outside =
        knownTasks.stream()
            .map(Task::getName)
            .filter(name -> !name.startsWith(namespace))
            .collect(Collectors.toList());
    if (!outside.isEmpty()) {
      throw new IllegalArgumentException(
          "Scheduler has task-namespace '"
              + namespace
              + "', but these tasks are named outside it and would never execute: "
              + outside
              + ". Either rename them to start with the namespace, or remove the namespace.");
    }
  }

  /**
   * The task-name prefix this scheduler owns, or empty for the whole table.
   *
   * <p>Because every query is restricted to the namespace, an unresolvable task-name can only ever
   * be one inside it. That is what makes deletion of unresolved executions safe when several
   * schedulers share a table.
   */
  public String getNamespace() {
    return namespace;
  }

  public Optional<Task> resolve(Resolvable resolvable) {
    return resolve(resolvable, true);
  }

  public Optional<Task> resolve(Resolvable resolvable, boolean addUnresolvedToExclusionFilter) {
    String taskName = resolvable.getTaskName();

    Task task = taskMap.get(taskName);
    if (task == null && addUnresolvedToExclusionFilter) {
      addUnresolved(taskName);
      schedulerListeners.onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
      LOG.info(
          "Found execution with unknown task-name '{}'. Adding it to the list of known unresolved task-names.",
          taskName);
    }
    return Optional.ofNullable(task);
  }

  private void addUnresolved(String taskName) {
    // Retention is counted from when the task-name was first seen to be unresolvable, not from the
    // execution-time, which for a due execution always lies in the past.
    unresolvedTasks.putIfAbsent(taskName, new UnresolvedTask(taskName, clock.now()));
  }

  public void addTask(Task task) {
    taskMap.put(task.getName(), task);
    clearUnresolved(task.getName());
  }

  public List<UnresolvedTask> getUnresolved() {
    return new ArrayList<>(unresolvedTasks.values());
  }

  public List<String> getUnresolvedTaskNames(Duration unresolvedFor) {
    return unresolvedTasks.values().stream()
        .filter(
            unresolved ->
                Duration.between(unresolved.firstUnresolved, clock.now()).toMillis()
                    > unresolvedFor.toMillis())
        .map(UnresolvedTask::getTaskName)
        .collect(Collectors.toList());
  }

  public boolean isUnresolved(String taskName) {
    return unresolvedTasks.containsKey(taskName);
  }

  public void clearUnresolved(String taskName) {
    unresolvedTasks.remove(taskName);
  }

  public class UnresolvedTask {

    private final String taskName;
    private final Instant firstUnresolved;

    public UnresolvedTask(String taskName, Instant firstUnresolved) {
      this.taskName = taskName;
      this.firstUnresolved = firstUnresolved;
    }

    public String getTaskName() {
      return taskName;
    }
  }
}
