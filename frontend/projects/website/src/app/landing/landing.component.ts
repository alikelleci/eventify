import { Component } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { EventShowcaseComponent } from './showcase/event-showcase.component';
import { CommandShowcaseComponent } from './showcase/command-showcase.component';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';

interface Feature {
  icon: string;
  title: string;
  text: string;
}

interface SetupStep {
  title: string;
  text: string;
  code: string;
}

@Component({
  selector: 'app-landing',
  templateUrl: './landing.component.html',
  standalone: true,
  imports: [DatePipe, ButtonModule, TagModule, TimelineItemComponent, EventShowcaseComponent, CommandShowcaseComponent],
  host: { class: 'block h-full' },
})
export class LandingComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

  /** Example data for the illustration, newest first like the lists in the app. */
  readonly now = Date.now();
  // The failed command is second, so it stays visible above the events card in front.
  readonly illustrationCommands = [
    { type: 'ShipOrder', agoMs: 20_020, failed: false },
    { type: 'ApplyDiscount', agoMs: 90_000, failed: true },
    { type: 'CapturePayment', agoMs: 140_010, failed: false },
    { type: 'PlaceOrder', agoMs: 380_010, failed: false },
  ];
  readonly illustrationEvents = [
    { type: 'OrderShipped', agoMs: 20_000 },
    { type: 'ShipmentLabelCreated', agoMs: 20_012 },
    { type: 'PaymentReceived', agoMs: 140_000 },
    { type: 'OrderPlaced', agoMs: 380_000 },
  ];

  readonly features: Feature[] = [
    { icon: 'pi pi-list', title: 'Event history', text: 'Every event of an aggregate, newest first, with its payload and metadata.' },
    { icon: 'pi pi-database', title: 'State at every event', text: 'See the aggregate as it was after each event, and diff it against the state before.' },
    { icon: 'pi pi-send', title: 'Commands and outcomes', text: 'Follow each command and its result: succeeded, failed with a cause, or retried.' },
    { icon: 'pi pi-refresh', title: 'Trace and retry', text: 'Jump from a command to the events it produced, and retry failed commands in one click.' },
  ];

  /** Example applications for the standalone app switcher illustration; the first one is active. */
  readonly apps = ['orders-service', 'payments-service', 'inventory-service'];

  /** Setup, as described in the console docs. */
  readonly setupSteps: SetupStep[] = [
    {
      title: 'Add the modules',
      text: 'The console server and its UI, next to eventify-core.',
      code: `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-console-server</artifactId>
</dependency>
<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-console-ui</artifactId>
</dependency>`,
    },
    {
      title: 'Register the plugin',
      text: 'With the Spring Boot starter this happens automatically; just set application.server.',
      code: `props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,
          "localhost:8085");

Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder().build())
    .build();`,
    },
    {
      title: 'Open the console',
      text: 'It is served on the host and port of application.server.',
      code: `http://localhost:8085/console/`,
    },
  ];

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
