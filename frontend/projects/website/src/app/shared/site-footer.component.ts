import { Component } from '@angular/core';

/** Shared footer for the public Eventify pages. */
@Component({
  selector: 'app-site-footer',
  standalone: true,
  template: `
    <footer class="border-t border-white/10 bg-slate-950">
      <div class="mx-auto flex max-w-6xl flex-col gap-4 px-6 py-7 text-sm text-slate-400 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex items-center gap-2">
          <i class="pi pi-bolt text-primary-400"></i>
          <span>Eventify · Event sourcing for Java</span>
        </div>
        <nav class="flex items-center gap-5" aria-label="Footer links">
          <a href="https://alikelleci.github.io/eventify/docs/" target="_blank" rel="noopener" class="hover:text-white">Docs</a>
          <a href="https://github.com/alikelleci/eventify" target="_blank" rel="noopener" class="hover:text-white">GitHub</a>
          <a href="https://github.com/alikelleci/eventify/blob/main/LICENSE" target="_blank" rel="noopener" class="hover:text-white">Apache 2.0</a>
        </nav>
      </div>
    </footer>
  `,
})
export class SiteFooterComponent {}
