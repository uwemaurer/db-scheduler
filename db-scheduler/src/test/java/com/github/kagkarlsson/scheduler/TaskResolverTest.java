package com.github.kagkarlsson.scheduler;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThatList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.github.kagkarlsson.scheduler.TaskResolver.UnresolvedTask;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener.SchedulerEventType;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskResolverTest {

  private final SchedulerListener mockScheduleListener = mock(SchedulerListener.class);
  private final TaskResolver taskResolver =
      new TaskResolver(new SchedulerListeners(mockScheduleListener), new SystemClock(), List.of());

  @Test
  void shouldProduceUnresolvedTaskEventWhenTheTaskResolverIsUnableToResolveTheTask() {
    taskResolver.resolve(Resolvable.of("unresolved", Instant.now()));
    verify(mockScheduleListener).onSchedulerEvent(SchedulerEventType.UNRESOLVED_TASK);
  }

  @Test
  void shouldReturnUnresolvedTasksWhenRequested() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved1", now));
    taskResolver.resolve(Resolvable.of("unresolved2", now.plus(ofSeconds(10))));

    final List<UnresolvedTask> unresolved = taskResolver.getUnresolved();
    assertThat(unresolved, hasSize(2));
    assertThat(
        unresolved.stream().map(TaskResolver.UnresolvedTask::getTaskName).toList(),
        contains("unresolved1", "unresolved2"));
  }

  @Test
  void shouldReturnUnresolvedTasksOlderThanSpecifiedDuration() {
    final SettableClock clock = new SettableClock();
    final TaskResolver resolver =
        new TaskResolver(new SchedulerListeners(mockScheduleListener), clock, List.of());

    // Retention runs from when the task-name was first seen unresolvable, not from its
    // execution-time, which for a due execution always lies in the past.
    resolver.resolve(Resolvable.of("old-unresolved", clock.now().minus(ofSeconds(3600))));
    clock.tick(ofSeconds(15));
    resolver.resolve(Resolvable.of("new-unresolved", clock.now().minus(ofSeconds(3600))));

    assertThatList(resolver.getUnresolvedTaskNames(ofSeconds(10)))
        .containsExactly("old-unresolved");
  }

  @Test
  void shouldNotDeemATaskUnresolvedJustBecauseItsExecutionTimeIsOld() {
    final SettableClock clock = new SettableClock();
    final TaskResolver resolver =
        new TaskResolver(new SchedulerListeners(mockScheduleListener), clock, List.of());

    resolver.resolve(Resolvable.of("unresolved", clock.now().minus(ofSeconds(3600))));

    assertThatList(resolver.getUnresolvedTaskNames(ofSeconds(10))).isEmpty();
  }

  @Test
  void shouldStopExcludingATaskOnceItIsRegistered() {
    taskResolver.resolve(Resolvable.of("late-task", Instant.now()));
    assertThatList(taskResolver.getUnresolved()).hasSize(1);

    taskResolver.addTask(Tasks.oneTime("late-task").execute((inst, ctx) -> {}));

    assertThatList(taskResolver.getUnresolved()).isEmpty();
    assertThat(taskResolver.isUnresolved("late-task"), is(false));
  }

  @Test
  void namespaceDefaultsToTheWholeTable() {
    assertThat(taskResolver.getNamespace(), is(""));
  }

  @Test
  void rejectsTasksNamedOutsideTheNamespace() {
    assertThatThrownBy(
            () ->
                new TaskResolver(
                    new SchedulerListeners(mockScheduleListener),
                    new SystemClock(),
                    "emails/",
                    List.of(Tasks.oneTime("reports/x").execute((inst, ctx) -> {}))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reports/x")
        .hasMessageContaining("would never execute");
  }

  @Test
  void rejectsANamespaceContainingLikeWildcards() {
    assertThatThrownBy(
            () ->
                new TaskResolver(
                    new SchedulerListeners(mockScheduleListener),
                    new SystemClock(),
                    "e%/",
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("like-wildcards");
  }

  @Test
  void rejectsANamespaceNotEndingInASeparator() {
    assertThatThrownBy(
            () ->
                new TaskResolver(
                    new SchedulerListeners(mockScheduleListener),
                    new SystemClock(),
                    "reports",
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with a separator");
  }

  @Test
  void acceptsTasksNamedInsideTheNamespace() {
    final TaskResolver resolver =
        new TaskResolver(
            new SchedulerListeners(mockScheduleListener),
            new SystemClock(),
            "emails/",
            List.of(Tasks.oneTime("emails/welcome").execute((inst, ctx) -> {})));
    assertThat(resolver.getNamespace(), is("emails/"));
  }

  @Test
  void shouldClearUnresolvedTaskWhenRequested() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved", now));
    assertThat(taskResolver.getUnresolved(), hasSize(1));

    taskResolver.clearUnresolved("unresolved");
    assertThat(taskResolver.getUnresolved(), empty());
  }

  @Test
  void shouldNotClearOtherUnresolvedTasksWhenClearingOne() {
    final Instant now = Instant.now();
    taskResolver.resolve(Resolvable.of("unresolved1", now));
    taskResolver.resolve(Resolvable.of("unresolved2", now));

    taskResolver.clearUnresolved("unresolved1");

    final List<UnresolvedTask> remaining = taskResolver.getUnresolved();
    assertThat(remaining, hasSize(1));
    assertThat(remaining.get(0).getTaskName(), is("unresolved2"));
  }
}
