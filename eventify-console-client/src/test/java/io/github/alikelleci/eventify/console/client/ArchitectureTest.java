package io.github.alikelleci.eventify.console.client;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

/**
 * This module uses eventify-core like an application does: through its public API, never its {@code internal}
 * packages. Frozen: the known violations are kept in {@code archunit_store}, and only a new one fails the build.
 */
@DisplayName("Architecture")
class ArchitectureTest {

  private static final JavaClasses MODULE = new ClassFileImporter()
      .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
      .importPackages("io.github.alikelleci.eventify.console.client");

  @Test
  @DisplayName("Should use only the public API of eventify-core")
  void usesOnlyThePublicApiOfCore() {
    freeze(noClasses().should().dependOnClassesThat().resideInAPackage("io.github.alikelleci.eventify.core..internal.."))
        .check(MODULE);
  }
}
