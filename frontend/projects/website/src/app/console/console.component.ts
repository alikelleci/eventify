import { Component } from '@angular/core';
import { ButtonModule } from 'primeng/button';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { AggregateReplayComponent } from './showcase/aggregate-replay.component';
import { CommandTraceComponent } from './showcase/command-trace.component';
import { ConsoleScreenComponent } from './showcase/console-screen.component';

/** The Eventify Console page: a real screen of the console, the event timeline and the commands of one order, and how to run it. */
@Component({
  selector: 'app-console',
  templateUrl: './console.component.html',
  standalone: true,
  imports: [ButtonModule, ConsoleScreenComponent, AggregateReplayComponent, CommandTraceComponent],
  host: { class: 'block h-full' },
})
export class ConsoleComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
