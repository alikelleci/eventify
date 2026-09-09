package io.github.alikelleci.eventify.core.plugin;

import io.github.alikelleci.eventify.core.Eventify;

public interface EventifyPlugin {

  void onStart(Eventify eventify);

  void onStop(Eventify eventify);
}
