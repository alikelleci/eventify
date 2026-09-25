import { Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DOCS_HOME_URL, GITHUB_URL } from '@eventify/ui/links';
import { ConsoleScreenComponent } from '../console/showcase/console-screen.component';
import { FeatureStoryComponent } from './feature-story.component';
import { highlightJava } from '../shared/java-highlight';
import { GetStartedComponent, SetupStep } from '../shared/get-started.component';
import { SiteFooterComponent } from '../shared/site-footer.component';

/** The Eventify framework's home page. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [RouterLink, ButtonModule, FeatureStoryComponent, ConsoleScreenComponent, GetStartedComponent, SiteFooterComponent],
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
    Order apply(OrderPlaced event, Order state) {
        return new Order(event.id(), event.customer());
    }
}`);

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
    Order apply(OrderPlaced event, Order state) {
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
