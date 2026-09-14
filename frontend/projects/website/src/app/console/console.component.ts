import { Component } from '@angular/core';
import { ButtonModule } from 'primeng/button';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';
import { AggregateReplayComponent } from './showcase/aggregate-replay.component';
import { ConsoleScreenComponent } from './showcase/console-screen.component';
import { ConsoleQuestionsComponent } from './showcase/console-questions.component';

/** The Eventify Console page: a real screen of the console, what it shows, a replay of one order, and how to run it. */
@Component({
  selector: 'app-console',
  templateUrl: './console.component.html',
  standalone: true,
  imports: [ButtonModule, ConsoleScreenComponent, ConsoleQuestionsComponent, AggregateReplayComponent],
  host: { class: 'block h-full' },
})
export class ConsoleComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;

  openDocs() {
    window.open(this.docsUrl, '_blank', 'noopener');
  }
}
