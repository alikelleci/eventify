import { Component, inject, ViewChild } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { PopoverModule } from 'primeng/popover';
import { Popover } from 'primeng/popover';
import { BackendService } from './backend.service';
import { AppEntry } from './config.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, PopoverModule],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900 shrink-0">
        <i class="pi pi-bolt text-primary-400 text-xl"></i>
        <span class="font-semibold text-lg tracking-tight text-white">Eventify</span>
        <span class="w-px h-4 bg-slate-600"></span>
        <span class="text-sm text-slate-300">Console</span>

        @if (!backend.isEmbedded() && backend.apps().length > 0) {
          <div class="ml-auto">
            <button
              (click)="op.toggle($event)"
              class="flex items-center gap-2 h-8 px-3 min-w-40 rounded border border-slate-600 bg-slate-800 hover:bg-slate-700 transition-colors text-sm text-slate-200 cursor-pointer outline-none">
              <span class="flex-1 text-left truncate">{{ backend.activeApp()?.name }}</span>
              <i class="pi pi-chevron-down text-slate-400 text-xs"></i>
            </button>

            <p-popover #op>
              <div class="flex flex-col min-w-48">
                <div class="px-3 py-2 border-b border-surface-200 dark:border-surface-700">
                  <span class="text-xs font-semibold text-surface-400 uppercase tracking-widest">Applications</span>
                </div>
                <div class="max-h-64 overflow-y-auto app-list">
                  @for (app of backend.apps(); track app.url) {
                    <div
                      (click)="selectApp(app, op)"
                      class="flex items-center gap-2 px-3 py-2 cursor-pointer hover:bg-surface-100 dark:hover:bg-surface-800 transition-colors">
                      <div class="w-3 shrink-0">
                        @if (backend.activeApp()?.url === app.url) {
                          <i class="pi pi-check text-primary-500 text-xs"></i>
                        }
                      </div>
                      <div class="flex flex-col min-w-0 flex-1">
                        <span class="text-sm truncate" [class.text-primary-500]="backend.activeApp()?.url === app.url">{{ app.name }}</span>
                        <span class="text-xs text-surface-400 truncate">{{ app.url }}</span>
                      </div>
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
