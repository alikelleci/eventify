import { Component } from '@angular/core';
import { ButtonModule } from 'primeng/button';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { AggregateReplayComponent } from './showcase/aggregate-replay.component';
import { ConsoleScreenComponent } from './showcase/console-screen.component';
import { SetupStep, SetupStepsComponent } from '../shared/setup-steps.component';

/** The Eventify Console page: a real screen of the console, what it shows, a replay of one order, and how to set it up. */
@Component({
  selector: 'app-console',
  templateUrl: './console.component.html',
  standalone: true,
  imports: [ButtonModule, ConsoleScreenComponent, AggregateReplayComponent, SetupStepsComponent],
  host: { class: 'block h-full' },
})
export class ConsoleComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

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
