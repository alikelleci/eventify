import { Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DOCS_HOME_URL, GITHUB_URL } from '@eventify/ui/links';
import { ConsoleScreenComponent } from '../console/showcase/console-screen.component';
import { FeatureStoryComponent } from './feature-story.component';
import { CrudVsLogComponent } from './crud-vs-log.component';
import { GetStartedComponent, SetupStep } from '../shared/get-started.component';
import { highlightJava } from '../shared/java-highlight';
import { SiteFooterComponent } from '../shared/site-footer.component';

/** The Eventify framework's home page. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [
    RouterLink, ButtonModule, CrudVsLogComponent, FeatureStoryComponent, ConsoleScreenComponent, GetStartedComponent, SiteFooterComponent,
  ],
  host: { class: 'block h-full' },
})
export class HomeComponent {
  readonly docsUrl = DOCS_HOME_URL;
  readonly githubUrl = GITHUB_URL;

  /** The complete aggregate flow shown in the hero. */
  readonly highlightedHeroCode = highlightJava(`public class OrderHandler {

    @CommandHandler
    OrderPlaced handle(PlaceOrder command, Order state) {
        return new OrderPlaced(command.id(), command.customer());
    }

    @EventSourcingHandler
    Order handle(OrderPlaced event, Order state) {
        return new Order(event.id(), event.customer());
    }
}`);

  /** The grid lines of the hero that events stream along, in px from its top (the grid is 48px). */
  readonly streams = [
    { top: 96, duration: 7, delay: 0 },
    { top: 240, duration: 9, delay: 2.5 },
    { top: 384, duration: 6, delay: 4 },
    { top: 528, duration: 10, delay: 1 },
  ];

  /** Why keep the history. */
  readonly reasons = [
    { icon: 'pi-lock', title: 'An explicit history', text: 'Store changes as immutable events instead of losing the story in an update.' },
    { icon: 'pi-history', title: 'Explain state', text: 'Replay retained history to understand how an aggregate reached its current state.' },
    { icon: 'pi-replay', title: 'Build new views', text: 'Use historical events to build a new projection or recover aggregate state.' },
  ];

  /** Quick-start steps. */
  readonly steps: SetupStep[] = [
    {
      title: 'Add the dependency',
      text: 'Add Eventify to your project. Using Spring Boot? Use <code>eventify-spring-boot-starter</code> instead.',
      label: 'pom.xml', language: 'xml',
      code: `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-core</artifactId>
  <version>x.y.z</version>
</dependency>`,
    },
    {
      title: 'Write your business logic',
      text: 'Plain Java classes with annotated methods. Keep decisions and state transitions close together.',
      label: 'Java', language: 'java',
      code: `public class OrderHandler {

    @CommandHandler
    OrderPlaced handle(PlaceOrder command, Order state) {
        return new OrderPlaced(command.id(), command.customer());
    }

    @EventSourcingHandler
    Order handle(OrderPlaced event, Order state) {
        return new Order(event.id(), event.customer());
    }
}`,
    },
    {
      title: 'Register and start',
      text: 'Configure Eventify, register your handlers, and start.',
      label: 'Java', language: 'java',
      code: `Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerHandler(new OrderHandler())
    .build();

eventify.start();`,
    },
  ];

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
