import { Component, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DOCS_HOME_URL, GITHUB_URL } from '@eventify/ui/links';
import { ConsoleScreenComponent } from '../console/showcase/console-screen.component';
import { FeatureStoryComponent } from './feature-story.component';
import { highlightJava } from '../shared/java-highlight';
import { GetStartedComponent, SetupStep } from '../shared/get-started.component';

interface CodeSample {
  label: string;
  code: string;
}

/** The Eventify framework's home page. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [RouterLink, ButtonModule, FeatureStoryComponent, ConsoleScreenComponent, GetStartedComponent],
  host: { class: 'block h-full' },
})
export class HomeComponent {
  readonly docsUrl = DOCS_HOME_URL;
  readonly githubUrl = GITHUB_URL;

  /** The hero's code window: one tab per kind of handler, as described in the docs. */
  readonly samples: CodeSample[] = [
    {
      label: 'Command handler',
      code: `public class OrderCommandHandler {

    @HandleCommand
    public OrderEvent handle(PlaceOrder command, Order state) {
        return OrderPlaced.builder()
            .id(command.getId())
            .customer(command.getCustomer())
            .build();
    }
}`,
    },
    {
      label: 'Event sourcing handler',
      code: `public class OrderEventSourcingHandler {

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
        return Order.builder()
            .id(event.getId())
            .customer(event.getCustomer())
            .build();
    }
}`,
    },
    {
      label: 'Event handler',
      code: `public class OrderEventHandler {

    @HandleEvent
    public void on(OrderPlaced event) {
        // e.g. insert into a read model
    }

    @HandleEvent
    public void on(OrderShipped event) {
        // e.g. notify the customer
    }
}`,
    },
  ];
  readonly activeSample = signal(0);
  // Highlighted once. All samples are rendered on top of each other, so the window is as tall as the longest one
  // and doesn't change height when switching tabs.
  readonly highlightedSamples = this.samples.map(sample => highlightJava(sample.code));

  /** The same steps as the Getting Started page of the documentation. */
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
      text: 'Plain Java classes with annotated methods. No base classes to extend, no interfaces to implement.',
      label: 'Java', language: 'java',
      code: `public class OrderCommandHandler {

    @HandleCommand
    public OrderEvent handle(PlaceOrder command, Order state) {
        return OrderPlaced.builder()
            .id(command.getId())
            .build();
    }
}`,
    },
    {
      title: 'Register and start',
      text: 'Point Eventify at your Kafka broker, register your handlers, and start.',
      label: 'Java', language: 'java',
      code: `Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerHandler(new OrderCommandHandler())
    .build();

eventify.start();`,
    },
  ];

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
