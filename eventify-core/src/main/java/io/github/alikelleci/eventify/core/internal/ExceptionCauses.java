package io.github.alikelleci.eventify.core.internal;

import org.apache.commons.lang3.exception.ExceptionUtils;

/** The innermost cause when there is one, otherwise the exception itself. */
public final class ExceptionCauses {

  private ExceptionCauses() {
  }

  public static Throwable rootCauseOrSelf(Throwable exception) {
    Throwable rootCause = ExceptionUtils.getRootCause(exception);
    return rootCause != null ? rootCause : exception;
  }
}
