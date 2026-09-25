package io.github.alikelleci.eventify.core;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/** A feature package holds its public API, with {@code annotation}, {@code exception} and {@code internal} subpackages. */
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
  @DisplayName("Should keep only Eventify exceptions in exception packages")
  void exceptionPackagesHoldExceptions() {
    classes().that().resideInAPackage("..core..exception")
        .should(beEventifyException())
        .check(CORE);
  }

  /** Checked on the loaded class: ArchUnit can't read the class files of every JDK. */
  private static ArchCondition<JavaClass> beEventifyException() {
    return new ArchCondition<>("be an EventifyException") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        if (!EventifyException.class.isAssignableFrom(javaClass.reflect())) {
          events.add(SimpleConditionEvent.violated(javaClass, javaClass.getName() + " is not an EventifyException"));
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

  /** Except {@code core.internal}, which wires the features together. */
  @Test
  @DisplayName("Should have no cycles between the feature packages")
  void featuresHaveNoCycles() {
    slices().matching("io.github.alikelleci.eventify.core.(*)..")
        .should().beFreeOfCycles()
        .ignoreDependency(JavaClass.Predicates.resideInAPackage("io.github.alikelleci.eventify.core.internal.."), DescribedPredicate.alwaysTrue())
        .check(CORE);
  }
}
