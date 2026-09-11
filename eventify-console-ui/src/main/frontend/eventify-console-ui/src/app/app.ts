import { Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { SelectModule } from 'primeng/select';
import { FormsModule } from '@angular/forms';
import { BackendService } from './backend.service';
import { AppEntry } from './config.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, SelectModule, FormsModule],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900 shrink-0">
        <i class="pi pi-bolt text-primary-400 text-xl"></i>
        <span class="font-semibold text-lg tracking-tight text-white">Eventify</span>
        <span class="w-px h-4 bg-slate-600"></span>
        <span class="text-sm text-slate-300">Console</span>
        @if (!backend.isEmbedded()) {
          <div class="ml-auto">
            <p-select
              [options]="backend.apps()"
              [ngModel]="backend.activeApp()"
              (ngModelChange)="onAppChange($event)"
              optionLabel="name"
              placeholder="Select app"
            />
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

  onAppChange(app: AppEntry): void {
    const index = this.backend.apps().findIndex(a => a.url === app.url);
    if (index >= 0) this.backend.setActive(index);
  }
}
