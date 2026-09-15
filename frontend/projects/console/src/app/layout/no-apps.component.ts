import { Component, inject } from '@angular/core';
import { DOCS_URL } from '@eventify/ui/links';
import { SessionService } from '../services/session.service';

/** Shown instead of the console while no application is connected to it. */
@Component({
  selector: 'app-no-apps',
  standalone: true,
  host: { class: 'flex h-full items-center justify-center overflow-y-auto px-6 py-10' },
  template: `
    <div class="w-full max-w-lg">
      <div class="flex h-11 w-11 items-center justify-center rounded-lg bg-primary-50 text-primary-600 dark:bg-primary-950 dark:text-primary-400">
        <i class="pi pi-server"></i>
      </div>
      <h1 class="mt-5 text-xl font-semibold text-surface-900 dark:text-surface-0">No applications connected</h1>
      <p class="mt-2 text-sm leading-relaxed text-surface-500 dark:text-surface-400">
        Applications connect to this console by themselves and show up here as soon as they start.
        Add <code class="font-mono text-surface-700 dark:text-surface-200">eventify-console-plugin</code> to an application
        and register it with this console's address:
      </p>
      <pre class="mt-5 overflow-x-auto rounded-lg border border-surface-200 bg-surface-50 px-4 py-3 font-mono text-xs leading-relaxed text-surface-700 dark:border-surface-700 dark:bg-surface-800 dark:text-surface-200">{{ example }}</pre>
      <p class="mt-3 text-xs text-surface-400">The applications open the connection, so they don't need a port of their own.</p>
      <a [href]="docsUrl" target="_blank" rel="noopener"
         class="mt-6 inline-flex items-center gap-2 text-sm font-medium text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300">
        Read the setup guide <i class="pi pi-arrow-up-right text-xs"></i>
      </a>
    </div>
  `,
})
export class NoAppsComponent {
  readonly docsUrl = DOCS_URL;
  private readonly session = inject(SessionService);

  // The token line only when this console requires one; its value is a secret, so it comes from the environment.
  get example(): string {
    const token = this.session.session().appTokenRequired ? `\n        .token(System.getenv("EVENTIFY_CONSOLE_TOKEN"))` : '';
    return `Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder()
        .url("${location.origin}")${token}
        .build())
    .build();`;
  }
}
