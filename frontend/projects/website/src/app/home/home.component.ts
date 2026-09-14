import { Component, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ButtonModule } from 'primeng/button';
import { DOCS_HOME_URL, GITHUB_URL, MAVEN_CENTRAL_URL } from '@eventify/ui/links';
import { ConsoleIllustrationComponent } from '../shared/console-illustration.component';
import { SetupStep, SetupStepsComponent } from '../shared/setup-steps.component';
import { highlightJava } from '../shared/java-highlight';

interface CodeSample {
  label: string;
  code: string;
}

interface Feature {
  icon: string;
  title: string;
  text: string;
}

interface FlowStep {
  icon: string;
  title: string;
  text: string;
}

/** The Eventify framework's home page. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [RouterLink, ButtonModule, ConsoleIllustrationComponent, SetupStepsComponent],
  host: { class: 'block h-full' },
})
export class HomeComponent {
  readonly docsUrl = DOCS_HOME_URL;
  readonly githubUrl = GITHUB_URL;
  readonly mavenCentralUrl = MAVEN_CENTRAL_URL;

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

  readonly flow: FlowStep[] = [
    { icon: 'pi pi-send', title: 'Command', text: 'A command is sent to its Kafka topic, for example from your API with the command gateway.' },
    { icon: 'pi pi-code', title: 'Command handler', text: 'Checks the command against the current state of the aggregate, and returns the events that happened or rejects it.' },
    { icon: 'pi pi-database', title: 'Events', text: 'Events are appended to the event store, a Kafka Streams state store backed by a changelog topic, and published.' },
    { icon: 'pi pi-sitemap', title: 'State and reactions', text: 'Event sourcing handlers build the next state; event handlers update read models or start other processes.' },
  ];

  readonly features: Feature[] = [
    { icon: 'pi pi-code', title: 'Plain Java', text: 'Handlers are annotated methods on plain classes. No base classes to extend, no framework interfaces to implement.' },
    { icon: 'pi pi-history', title: 'State from events', text: 'Aggregates are immutable and rebuilt from their event history, so every change is recorded.' },
    { icon: 'pi pi-check-circle', title: 'Validation built in', text: 'Bean Validation annotations on commands are checked before your handler runs.' },
    { icon: 'pi pi-camera', title: 'Snapshotting', text: 'Rebuild long-lived aggregates from a snapshot instead of their whole history, with one annotation.' },
    { icon: 'pi pi-sync', title: 'Event upcasting', text: 'Evolve event schemas: older events are migrated to the latest revision when they are read.' },
    { icon: 'pi pi-arrow-right-arrow-left', title: 'Command gateway', text: 'Send commands from your API and wait for the result, or handle it asynchronously.' },
    { icon: 'pi pi-box', title: 'Spring Boot starter', text: 'Handler beans are discovered and registered automatically, and Eventify starts with your application.' },
    { icon: 'pi pi-verified', title: 'Test without a broker', text: 'Run the complete topology in memory with the Kafka Streams TopologyTestDriver.' },
  ];

  /** Getting started, as described in the docs. */
  readonly setupSteps: SetupStep[] = [
    {
      title: 'Add the dependency',
      text: 'eventify-core, or eventify-spring-boot-starter in a Spring Boot application.',
      code: `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-core</artifactId>
  <version>x.y.z</version>
</dependency>`,
    },
    {
      title: 'Model your domain',
      text: 'Commands and events are immutable value objects. The marker interface declares their Kafka topic.',
      code: `@TopicInfo("commands.order")
public interface OrderCommand {

    @Value
    @Builder
    class PlaceOrder implements OrderCommand {
        @AggregateId
        String id;
        @NotBlank
        String customer;
    }
}`,
    },
    {
      title: 'Register your handlers and start',
      text: 'Kafka Streams configuration and your handler classes. With the Spring Boot starter, handler beans are registered for you.',
      code: `Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerHandler(new OrderCommandHandler())
    .registerHandler(new OrderEventSourcingHandler())
    .build();

eventify.start();`,
    },
  ];

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
