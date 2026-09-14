import { Component } from '@angular/core';
import { RouterLink, RouterOutlet } from '@angular/router';
import { DOCS_URL, GITHUB_URL } from '@eventify/ui/links';

/** The website shell: the same dark header as the console, with links instead of search and the app switcher. */
@Component({
  selector: 'app-website-layout',
  standalone: true,
  imports: [RouterOutlet, RouterLink],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="shrink-0 flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900">
        <a routerLink="/" class="flex items-center gap-3 rounded outline-none focus-visible:ring-2 focus-visible:ring-primary-400">
          <i class="pi pi-bolt text-primary-400 text-xl"></i>
          <span class="text-lg tracking-tight"><span class="font-semibold text-white">Eventify</span><span class="font-light text-slate-300 ml-1.5">Console</span></span>
        </a>
        <nav class="ml-auto flex items-center gap-5 text-sm">
          <a [href]="docsUrl" target="_blank" rel="noopener" class="text-slate-300 hover:text-white transition-colors">Docs</a>
          <a [href]="githubUrl" target="_blank" rel="noopener" class="flex items-center gap-2 text-slate-300 hover:text-white transition-colors">
            <i class="pi pi-github"></i><span class="hidden sm:inline">GitHub</span>
          </a>
        </nav>
      </header>
      <main class="flex-1 overflow-hidden">
        <router-outlet />
      </main>
    </div>
  `,
})
export class WebsiteLayoutComponent {
  readonly docsUrl = DOCS_URL;
  readonly githubUrl = GITHUB_URL;
}
