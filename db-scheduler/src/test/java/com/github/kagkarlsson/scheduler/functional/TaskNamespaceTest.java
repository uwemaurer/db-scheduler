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
package com.github.kagkarlsson.scheduler.functional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.Mappers;
import com.github.kagkarlsson.scheduler.DbUtils;
import com.github.kagkarlsson.scheduler.EmbeddedPostgresqlExtension;
import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.TestTasks;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.ManualScheduler;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Two independent schedulers sharing one table, each owning a task-namespace. The "emails"
 * scheduler must never read, pick or delete executions belonging to the "reports" scheduler, but
 * must still clean up obsolete executions inside its own namespace.
 */
public class TaskNamespaceTest {

  private static final String EMAILS = "emails/";

  @RegisterExtension
  public EmbeddedPostgresqlExtension postgres = new EmbeddedPostgresqlExtension();

  private final OneTimeTask<Void> foreignTask =
      Tasks.oneTime("reports/monthly").execute(TestTasks.DO_NOTHING);
  private final AtomicInteger ownRuns = new AtomicInteger();
  private final OneTimeTask<Void> ownTask =
      Tasks.oneTime("emails/welcome").execute((inst, ctx) -> ownRuns.incrementAndGet());

  private SettableClock clock;

  @BeforeEach
  public void setUp() {
    clock = new SettableClock();
    clock.set(Instant.parse("2024-01-15T08:00:00Z"));
  }

  private ManualScheduler emailScheduler(int threads) {
    var builder =
        TestHelper.createManualScheduler(postgres.getDataSource(), ownTask)
            .clock(clock)
            .taskNamespace(EMAILS);
    builder.threads(threads);
    return builder.build();
  }

  @Test
  public void does_not_delete_executions_outside_its_namespace() {
    ManualScheduler emails = emailScheduler(10);
    emails.schedule(foreignTask.instance("id1"), clock.now().minus(Duration.ofDays(20)));

    emails.runAnyDueExecutions();
    clock.set(clock.now().plus(Duration.ofDays(30)));
    emails.runDeadExecutionDetection();

    assertThat(DbUtils.countExecutions(postgres.getDataSource())).isEqualTo(1);
  }

  /** The whole point of the namespace: cleanup still works, scoped to what the scheduler owns. */
  @Test
  public void still_deletes_obsolete_executions_inside_its_namespace() {
    ManualScheduler emails = emailScheduler(10);
    OneTimeTask<Void> decommissioned =
        Tasks.oneTime("emails/decommissioned").execute(TestTasks.DO_NOTHING);
    emails.schedule(decommissioned.instance("id1"), clock.now());
    emails.schedule(foreignTask.instance("id1"), clock.now());

    emails.runAnyDueExecutions();
    clock.set(clock.now().plus(Duration.ofDays(15)));
    emails.runDeadExecutionDetection();

    assertThat(DbUtils.countExecutions(postgres.getDataSource()))
        .as("obsolete emails/ execution deleted, reports/ execution untouched")
        .isEqualTo(1);
    assertThat(emails.getScheduledExecution(foreignTask.instance("id1"))).isPresent();
  }

  @Test
  public void retention_is_counted_from_first_seen_not_from_execution_time() {
    ManualScheduler emails = emailScheduler(10);
    OneTimeTask<Void> decommissioned =
        Tasks.oneTime("emails/decommissioned").execute(TestTasks.DO_NOTHING);
    emails.schedule(decommissioned.instance("id1"), clock.now().minus(Duration.ofDays(20)));

    emails.runAnyDueExecutions();
    emails.runDeadExecutionDetection();
    assertThat(DbUtils.countExecutions(postgres.getDataSource())).isEqualTo(1);

    clock.set(clock.now().plus(Duration.ofDays(15)));
    emails.runDeadExecutionDetection();
    assertThat(DbUtils.countExecutions(postgres.getDataSource())).isZero();
  }

  /** Dead-execution handling is a write path, so it must be namespace-scoped too. */
  @Test
  public void does_not_handle_dead_executions_outside_its_namespace() {
    ManualScheduler emails = emailScheduler(10);
    emails.schedule(foreignTask.instance("id1"), clock.now());
    markPicked("reports/monthly", "id1");

    clock.set(clock.now().plus(Duration.ofDays(1)));
    emails.runDeadExecutionDetection();

    assertThat(emails.getScheduledExecution(foreignTask.instance("id1")).orElseThrow().isPicked())
        .as("foreign dead execution was not revived by this scheduler")
        .isTrue();
  }

  @Test
  public void own_task_is_executed_in_the_first_poll_despite_foreign_backlog() {
    ManualScheduler emails = emailScheduler(2); // upper fetch limit = 3 * 2 = 6
    for (int i = 0; i < 10; i++) {
      emails.schedule(foreignTask.instance("r" + i), clock.now().minusSeconds(60));
    }
    emails.schedule(ownTask.instance("e1"), clock.now().minusSeconds(1));

    emails.runAnyDueExecutions();

    assertThat(ownRuns.get()).isEqualTo(1);
  }

  /**
   * lock-and-fetch picks rows in the same statement that selects them, so a foreign row that is not
   * excluded in SQL gets picked and then unpicked again. The version bump is the only lasting
   * trace, so assert on that rather than on the picked-flag.
   */
  @Test
  public void lock_and_fetch_never_touches_an_execution_outside_its_namespace() {
    var builder =
        TestHelper.createManualScheduler(postgres.getDataSource(), ownTask)
            .clock(clock)
            .taskNamespace(EMAILS)
            .pollingStrategy(PollingStrategyConfig.DEFAULT_SELECT_FOR_UPDATE);
    ManualScheduler emails = builder.build();

    emails.schedule(foreignTask.instance("id1"), clock.now().minusSeconds(60));
    emails.schedule(ownTask.instance("e1"), clock.now().minusSeconds(60));
    long versionBefore = versionOf("reports/monthly", "id1");

    emails.runAnyDueExecutions();

    assertThat(ownRuns.get()).as("still runs its own task").isEqualTo(1);
    assertThat(versionOf("reports/monthly", "id1"))
        .as("foreign row was never picked, not even transiently")
        .isEqualTo(versionBefore);
  }

  @Test
  public void rejects_tasks_named_outside_the_namespace() {
    var builder =
        TestHelper.createManualScheduler(postgres.getDataSource(), foreignTask)
            .clock(clock)
            .taskNamespace(EMAILS);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reports/monthly")
        .hasMessageContaining("would never execute");
  }

  @Test
  public void rejects_a_namespace_containing_like_wildcards() {
    var builder =
        TestHelper.createManualScheduler(postgres.getDataSource())
            .clock(clock)
            .taskNamespace("e%/");

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("like-wildcards");
  }

  /** Pretend the reports-scheduler picked this execution and then died mid-execution. */
  private void markPicked(String taskName, String instanceId) {
    new JdbcRunner(postgres.getDataSource(), true)
        .execute(
            "update scheduled_tasks set picked = ?, picked_by = ? "
                + "where task_name = ? and task_instance = ?",
            ps -> {
              ps.setBoolean(1, true);
              ps.setString(2, "the-reports-scheduler");
              ps.setString(3, taskName);
              ps.setString(4, instanceId);
            });
  }

  /** A transient pick-then-unpick leaves no trace other than the bumped version. */
  private long versionOf(String taskName, String instanceId) {
    return new JdbcRunner(postgres.getDataSource())
        .query(
            "select version from scheduled_tasks where task_name = ? and task_instance = ?",
            ps -> {
              ps.setString(1, taskName);
              ps.setString(2, instanceId);
            },
            Mappers.SINGLE_LONG);
  }

  @Test
  public void empty_namespace_keeps_whole_table_behaviour() {
    var builder = TestHelper.createManualScheduler(postgres.getDataSource(), ownTask).clock(clock);
    ManualScheduler all = builder.build();

    all.schedule(ownTask.instance("e1"), clock.now().minusSeconds(60));
    all.runAnyDueExecutions();

    assertThat(ownRuns.get()).isEqualTo(1);
  }
}
