import { Component, input } from '@angular/core';

export interface SetupStep {
  title: string;
  text: string;
  code: string;
}

/** Numbered setup steps: the explanation on the left, its code on the right. */
@Component({
  selector: 'app-setup-steps',
  standalone: true,
  template: `
    <!-- One row per step: explanation left, code right, so the code has room -->
    <div class="space-y-4">
      @for (step of steps(); track step.title; let i = $index) {
        <div class="grid items-start gap-5 rounded-2xl border border-surface-200 bg-surface-0 p-6 md:grid-cols-[minmax(0,1fr)_minmax(0,1.7fr)] md:gap-8 dark:border-surface-700 dark:bg-surface-900">
          <div>
            <div class="flex items-center gap-3">
              <span class="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary-50 text-sm font-semibold text-primary-600 dark:bg-primary-950 dark:text-primary-400">{{ i + 1 }}</span>
              <h3 class="text-sm font-semibold text-surface-900 dark:text-surface-0">{{ step.title }}</h3>
            </div>
            <p class="mt-3 text-sm leading-relaxed text-surface-500 dark:text-surface-400">{{ step.text }}</p>
          </div>
          <pre class="overflow-x-auto rounded-lg bg-slate-900 px-4 py-3 font-mono text-xs leading-relaxed text-slate-200"><code>{{ step.code }}</code></pre>
        </div>
      }
    </div>
  `,
})
export class SetupStepsComponent {
  steps = input.required<SetupStep[]>();
}
