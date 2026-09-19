package io.github.alikelleci.eventify.core;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.kafka.internal.EventifyTopology;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * The package structure of eventify-core: a feature package holds its public API, its annotations in {@code annotation},
 * its exceptions in {@code exception} and its implementation in {@code internal}.
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
        .should(beThrowable())
        .check(CORE);
  }

  /**
   * Checked on the loaded class, not on ArchUnit's view of the hierarchy: ArchUnit can't read the class files of every
   * JDK, and then doesn't know that {@code RuntimeException} is a {@code Throwable}.
   */
  private static ArchCondition<JavaClass> beThrowable() {
    return new ArchCondition<>("be a Throwable") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        if (!Throwable.class.isAssignableFrom(javaClass.reflect())) {
          events.add(SimpleConditionEvent.violated(javaClass, javaClass.getName() + " is not a Throwable"));
        }
      }
    };
  }

  @Test
  @DisplayName("Should keep Kafka Streams processors out of the public API")
  void processorsAreInternal() {
    noClasses().that().resideOutsideOfPackage("..internal..")
        .should().dependOnClassesThat().resideInAPackage("org.apache.kafka.streams.processor.api..")
        .check(CORE);
  }

  /**
   * Except for the classes that, like {@link Eventify}, put the features together: {@link HandlerRegistry} knows each
   * feature's handlers, {@link EventifyTopology} wires the features into Kafka Streams.
   */
  @Test
  @DisplayName("Should have no cycles between the feature packages")
  void featuresHaveNoCycles() {
    slices().matching("io.github.alikelleci.eventify.core.(*)..")
        .should().beFreeOfCycles()
        .ignoreDependency(JavaClass.Predicates.equivalentTo(HandlerRegistry.class), DescribedPredicate.alwaysTrue())
        .ignoreDependency(JavaClass.Predicates.equivalentTo(EventifyTopology.class), DescribedPredicate.alwaysTrue())
        .check(CORE);
  }
}
