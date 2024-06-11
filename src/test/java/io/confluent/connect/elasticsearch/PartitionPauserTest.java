package io.confluent.connect.elasticsearch;

import com.google.common.collect.ImmutableSet;
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask.PartitionPauser;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PartitionPauserTest {

  @Test
  public void partitionPauserTest() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    AtomicBoolean pauseCondition = new AtomicBoolean();
    AtomicBoolean resumeCondition = new AtomicBoolean();
    PartitionPauser partitionPauser = new PartitionPauser(context,
            pauseCondition::get,
            resumeCondition::get);

    TopicPartition tp = new TopicPartition("test-topic", 0);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    partitionPauser.maybePausePartitions();
    verifyNoMoreInteractions(context);

    partitionPauser.maybeResumePartitions();
    verifyNoMoreInteractions(context);

    pauseCondition.set(true);
    partitionPauser.maybePausePartitions();
    verify(context).assignment();
    verify(context).pause(tp);
    verify(context).timeout(100);
    verifyNoMoreInteractions(context);

    clearInvocations(context);
    partitionPauser.maybePausePartitions();
    verifyNoMoreInteractions(context);

    partitionPauser.maybeResumePartitions();
    verify(context).timeout(100);
    verifyNoMoreInteractions(context);

    resumeCondition.set(true);
    clearInvocations(context);
    partitionPauser.maybeResumePartitions();
    verify(context).assignment();
    verify(context).resume(tp);
    verifyNoMoreInteractions(context);

    clearInvocations(context);
    partitionPauser.maybeResumePartitions();
    verifyNoMoreInteractions(context);
  }
}
