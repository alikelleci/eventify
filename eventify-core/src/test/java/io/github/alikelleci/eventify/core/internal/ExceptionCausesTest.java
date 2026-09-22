package io.github.alikelleci.eventify.core.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionCausesTest {

  @Test
  void returnsTheExceptionItselfWhenThereIsNoNestedCause() {
    IllegalArgumentException exception = new IllegalArgumentException("invalid");

    assertThat(ExceptionCauses.rootCauseOrSelf(exception)).isSameAs(exception);
  }

  @Test
  void returnsTheInnermostCauseWhenThereIsOne() {
    IllegalArgumentException cause = new IllegalArgumentException("invalid");

    assertThat(ExceptionCauses.rootCauseOrSelf(new IllegalStateException("wrapper", cause))).isSameAs(cause);
  }
}
