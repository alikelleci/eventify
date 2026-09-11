import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet],
  template: `
    <div class="flex flex-col h-dvh bg-surface-0 dark:bg-surface-900 text-surface-900 dark:text-surface-100">
      <header class="flex items-center gap-3 px-5 py-3 border-b border-slate-700 bg-slate-900 shrink-0">
        <i class="pi pi-bolt text-primary-400 text-xl"></i>
        <span class="font-semibold text-lg tracking-tight text-white">Eventify</span>
        <span class="w-px h-4 bg-slate-600"></span>
        <span class="text-sm text-slate-300">Console</span>
      </header>
      <main class="flex-1 overflow-hidden">
        <router-outlet />
      </main>
    </div>
  `,
})
export class App {}
