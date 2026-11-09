package io.github.alikelleci.eventify.test;

/** Why something failed, written as Eventify writes it: "Exception: message" of the root cause. */
final class Causes {

  private Causes() {
  }

  /** The root cause of this failure, as "Exception: message". */
  static String of(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return shortName(root.getClass()) + ": " + (root.getMessage() != null ? root.getMessage() : "");
  }

  /** Whether this cause is of this exception. */
  static boolean isOf(String cause, Class<? extends Throwable> exception) {
    return cause != null && cause.startsWith(shortName(exception) + ":");
  }

  /** This exception with this message, as a cause. */
  static String of(Class<? extends Throwable> exception, String message) {
    return shortName(exception) + ": " + message;
  }

  /** Without the package, with dots between nested classes. */
  static String shortName(Class<?> exception) {
    String packageName = exception.getPackageName();
    return exception.getName().substring(packageName.isEmpty() ? 0 : packageName.length() + 1).replace('$', '.');
  }
}
