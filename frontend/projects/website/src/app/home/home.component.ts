import { Component, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DOCS_HOME_URL, GITHUB_URL } from '@eventify/ui/links';
import { ConsoleScreenComponent } from '../console/showcase/console-screen.component';
import { highlightJava } from '../shared/java-highlight';

interface CodeSample {
  label: string;
  code: string;
}

interface Feature {
  /** Which small illustration the feature card shows. */
  visual: 'state' | 'snapshots' | 'upcasting' | 'distributed';
  title: string;
  text: string;
}

/** The Eventify framework's home page. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [RouterLink, ButtonModule, ConsoleScreenComponent],
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
        if (state != null) {
            throw new ValidationException("Order already exists.");
        }
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

  // The event sourcing itself, not integrations or tooling.
  readonly features: Feature[] = [
    { visual: 'state', title: 'State from events', text: 'Aggregates are rebuilt from their events, so the event history is the source of truth.' },
    { visual: 'snapshots', title: 'Snapshots', text: 'Long histories are rebuilt from the latest snapshot instead of from the first event.' },
    { visual: 'upcasting', title: 'Event upcasting', text: 'Change the structure of an event, and older events are migrated as they are read.' },
    { visual: 'distributed', title: 'Distributed', text: 'Built on Kafka: aggregates are spread over partitions and shared by all running instances.' },
  ];

  // Example data for the illustrations.
  readonly stateEvents = ['OrderPlaced', 'ItemAdded', 'OrderPaid'];
  readonly timeline = Array.from({ length: 10 }, (_, i) => i);
  readonly snapshotAt = 6;
  readonly instances = [['P0', 'P3'], ['P1', 'P4'], ['P2', 'P5']];

  readonly dependency = `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-core</artifactId>
  <version>x.y.z</version>
</dependency>`;

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
