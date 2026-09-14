import { Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { DOCS_HOME_URL, GITHUB_URL } from '@eventify/ui/links';

/** The website shell: a dark header with the site's pages and outside links, above the routed page. */
@Component({
  selector: 'app-website-layout',
  standalone: true,
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="shrink-0 border-b border-white/10 bg-slate-950">
        <div class="mx-auto flex max-w-6xl items-center gap-2 px-4 py-3 sm:gap-6 sm:px-6">
          <a routerLink="/" class="flex items-center gap-2.5 rounded outline-none focus-visible:ring-2 focus-visible:ring-primary-400">
            <i class="pi pi-bolt text-primary-400 text-xl"></i>
            <span class="text-lg font-semibold tracking-tight text-white">Eventify</span>
          </a>
          <nav class="flex items-center gap-1 text-sm font-medium" aria-label="Pages">
            <a routerLink="/" routerLinkActive="active" [routerLinkActiveOptions]="{ exact: true }" class="nav-link">Home</a>
            <a routerLink="/console" routerLinkActive="active" class="nav-link">Console</a>
          </nav>
          <nav class="ml-auto flex items-center gap-1 text-sm font-medium" aria-label="Links">
            <a [href]="docsUrl" target="_blank" rel="noopener" class="nav-link">Docs</a>
            <a [href]="githubUrl" target="_blank" rel="noopener" class="nav-link flex items-center gap-2" aria-label="GitHub">
              <i class="pi pi-github"></i><span class="hidden sm:inline">GitHub</span>
            </a>
          </nav>
        </div>
      </header>
      <main class="flex-1 overflow-hidden">
        <router-outlet />
      </main>
    </div>
  `,
  styles: `
    .nav-link {
      border-radius: 0.375rem;
      padding: 0.375rem 0.5rem;
      color: var(--color-slate-300);
      transition: color 0.15s, background-color 0.15s;
    }
    @media (min-width: 640px) { .nav-link { padding: 0.375rem 0.75rem; } }
    .nav-link:hover { color: #fff; }
    .nav-link.active { color: #fff; background: rgb(255 255 255 / 0.1); }
  `,
})
export class WebsiteLayoutComponent {
  readonly docsUrl = DOCS_HOME_URL;
  readonly githubUrl = GITHUB_URL;
}
