package io.github.alikelleci.eventify.core;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

/**
 * The package structure of eventify-core: a feature package holds its public API, its annotations in {@code annotation},
 * its exceptions in {@code exception} and its implementation in {@code internal}.
 *
 * <p>Rules that don't hold everywhere yet are frozen: their known violations are kept in {@code archunit_store}, and
 * only a new one fails the build. A violation that is fixed is removed from the store.
 */
@DisplayName("Architecture")
class ArchitectureTest {

  private static final JavaClasses CORE = new ClassFileImporter()
      .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
      .importPackages("io.github.alikelleci.eventify.core");

  @Test
  @DisplayName("Should keep only annotations in annotation packages")
  void annotationPackagesHoldAnnotations() {
    classes().that().resideInAPackage("..core..annotation")
        .should().beAssignableTo(Annotation.class)
        .check(CORE);
  }

  @Test
  @DisplayName("Should keep only exceptions in exception packages")
  void exceptionPackagesHoldExceptions() {
    classes().that().resideInAPackage("..core..exception")
        .should().beAssignableTo(Throwable.class)
        .check(CORE);
  }

  @Test
  @DisplayName("Should keep Kafka Streams processors out of the public API")
  void processorsAreInternal() {
    noClasses().that().resideOutsideOfPackage("..internal..")
        .should().dependOnClassesThat().resideInAPackage("org.apache.kafka.streams.processor.api..")
        .check(CORE);
  }

  @Test
  @DisplayName("Should have no cycles between the feature packages")
  void featuresHaveNoCycles() {
    freeze(slices().matching("io.github.alikelleci.eventify.core.(*)..").should().beFreeOfCycles())
        .check(CORE);
  }
}
