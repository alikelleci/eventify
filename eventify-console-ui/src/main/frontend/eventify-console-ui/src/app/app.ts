import { Component, inject, ViewChild } from '@angular/core';
import { RouterOutlet, RouterLink, RouterLinkActive } from '@angular/router';
import { PopoverModule } from 'primeng/popover';
import { Popover } from 'primeng/popover';
import { BackendService } from './backend.service';
import { AppEntry } from './config.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, RouterLink, RouterLinkActive, PopoverModule],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900 shrink-0">
        <i class="pi pi-bolt text-primary-400 text-xl"></i>
        <span class="text-lg tracking-tight"><span class="font-semibold text-white">Eventify</span><span class="font-light text-slate-300 ml-1.5">Console</span></span>
        <span class="w-px h-4 bg-slate-600"></span>

        <div class="flex items-center gap-1">
          <a routerLink="/events" [queryParams]="{ eventId: null, commandId: null }" queryParamsHandling="merge"
             routerLinkActive="bg-slate-700 text-white"
             [routerLinkActiveOptions]="{ exact: false }"
             class="flex items-center h-7 px-3 rounded text-sm text-slate-400 hover:text-slate-200 transition-colors cursor-pointer outline-none">
            Events
          </a>
          <a routerLink="/commands" [queryParams]="{ eventId: null, commandId: null }" queryParamsHandling="merge"
             routerLinkActive="bg-slate-700 text-white"
             [routerLinkActiveOptions]="{ exact: false }"
             class="flex items-center h-7 px-3 rounded text-sm text-slate-400 hover:text-slate-200 transition-colors cursor-pointer outline-none">
            Commands
          </a>
        </div>

        @if (!backend.isEmbedded() && backend.apps().length > 0) {
          <div class="ml-auto">
            <button
              (click)="op.toggle($event)"
              class="flex items-center gap-2 h-8 px-3 min-w-40 rounded border border-slate-600 bg-slate-800 hover:bg-slate-700 transition-colors text-sm text-slate-200 cursor-pointer outline-none">
              <span class="flex-1 text-left truncate">{{ backend.activeApp()?.name }}</span>
            </button>

            <p-popover #op>
              <div class="flex flex-col" style="min-width: 220px">
                <div class="px-4 py-3 border-b border-surface-200 dark:border-surface-700">
                  <span class="text-xs font-semibold text-surface-400 uppercase tracking-widest">Applications</span>
                </div>
                <div class="max-h-64 overflow-y-auto app-list py-1">
                  @for (app of backend.apps(); track app.url) {
                    <div
                      (click)="selectApp(app, op)"
                      class="flex flex-col px-4 py-2.5 cursor-pointer transition-colors border-l-2"
                      [class.border-primary-500]="backend.activeApp()?.url === app.url"
                      [class.border-transparent]="backend.activeApp()?.url !== app.url"
                      [class.bg-primary-50]="backend.activeApp()?.url === app.url"
                      [class.dark:bg-primary-950]="backend.activeApp()?.url === app.url"
                      [class.hover:bg-surface-50]="backend.activeApp()?.url !== app.url"
                      [class.dark:hover:bg-surface-800]="backend.activeApp()?.url !== app.url">
                      <span class="text-sm leading-snug"
                        [class.font-medium]="backend.activeApp()?.url === app.url"
                        [class.text-primary-600]="backend.activeApp()?.url === app.url">{{ app.name }}</span>
                      <span class="text-xs text-surface-400 mt-0.5 truncate">{{ app.url }}</span>
                    </div>
                  }
                </div>
              </div>
            </p-popover>
          </div>
        }
      </header>
      <main class="flex-1 overflow-hidden">
        <router-outlet />
      </main>
    </div>
  `,
})
export class App {
  readonly backend = inject(BackendService);

  @ViewChild('op') op!: Popover;

  selectApp(app: AppEntry, popover: Popover): void {
    const index = this.backend.apps().findIndex(a => a.url === app.url);
    if (index >= 0) this.backend.setActive(index);
    popover.hide();
  }
}
