import { Component } from '@angular/core';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { EventShowcaseComponent } from './showcase/event-showcase.component';
import { CommandShowcaseComponent } from './showcase/command-showcase.component';
import { ConsoleIllustrationComponent } from '../shared/console-illustration.component';
import { SetupStep, SetupStepsComponent } from '../shared/setup-steps.component';

interface Feature {
  /** Which fragment of the console the panel shows. */
  visual: 'history' | 'state' | 'commands' | 'retry';
  title: string;
  text: string;
}

/** The Eventify Console page: what the console shows, how it looks, and how to set it up. */
@Component({
  selector: 'app-console',
  templateUrl: './console.component.html',
  standalone: true,
  imports: [ButtonModule, TagModule, EventShowcaseComponent, CommandShowcaseComponent, ConsoleIllustrationComponent, SetupStepsComponent],
  host: { class: 'block h-full' },
})
export class ConsoleComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

  readonly features: Feature[] = [
    { visual: 'history', title: 'Event history', text: 'Every event of an aggregate, newest first, with its payload and metadata.' },
    { visual: 'state', title: 'State at every event', text: 'See the aggregate as it was after each event, and diff it against the state before.' },
    { visual: 'commands', title: 'Commands and outcomes', text: 'Follow each command and its result: succeeded, failed with a cause, or retried.' },
    { visual: 'retry', title: 'Trace and retry', text: 'Jump from a command to the events it produced, and retry failed commands in one click.' },
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
      text: 'Set application.server to the host and port the console listens on, and register the plugin.',
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
