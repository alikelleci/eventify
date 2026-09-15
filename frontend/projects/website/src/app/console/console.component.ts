import { Component } from '@angular/core';
import { ButtonModule } from 'primeng/button';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { AggregateReplayComponent } from './showcase/aggregate-replay.component';
import { CommandTraceComponent } from './showcase/command-trace.component';
import { ConsoleScreenComponent } from './showcase/console-screen.component';
import { GetStartedComponent, SetupHub, SetupStep } from '../shared/get-started.component';

/** The Eventify Console page: a real screen of the console, the event timeline and the commands of one order, and how to run it. */
@Component({
  selector: 'app-console',
  templateUrl: './console.component.html',
  standalone: true,
  imports: [ButtonModule, ConsoleScreenComponent, AggregateReplayComponent, CommandTraceComponent, GetStartedComponent],
  host: { class: 'block h-full' },
})
export class ConsoleComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

  /** The same steps as "Running the console" and "Connecting an application" in the console's documentation. */
  readonly steps: SetupStep[] = [
    {
      title: 'Run the console',
      text: 'A single container, nothing else to install. Then open <code>localhost:8080</code>.',
      label: 'Terminal', language: 'shell',
      code: 'docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest',
    },
    {
      title: 'Add the plugin',
      text: 'Add the console plugin to your application.',
      label: 'pom.xml', language: 'xml',
      code: `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-console-plugin</artifactId>
  <version>x.y.z</version>
</dependency>`,
    },
    {
      title: 'Connect your application',
      text: 'Point it at the console, and you’re done.',
      label: 'Java', language: 'java',
      code: `Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder()
        .url("http://localhost:8080")
        .build())
    .build();`,
    },
  ];
  readonly hub: SetupHub = { label: 'Eventify Console', icon: 'pi-bolt' };

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
