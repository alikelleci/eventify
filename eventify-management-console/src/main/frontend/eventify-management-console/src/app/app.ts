import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="flex items-center gap-3 px-5 py-3 border-b border-surface-200 dark:border-surface-700 bg-surface-0 dark:bg-surface-900 shrink-0">
        <i class="pi pi-bolt text-primary-500 text-xl"></i>
        <span class="font-semibold text-lg tracking-tight">Eventify</span>
        <span class="text-xs text-surface-400 ml-1">Management Console</span>
      </header>
      <main class="flex-1 overflow-hidden">
        <router-outlet />
      </main>
    </div>
  `,
})
export class App {}
