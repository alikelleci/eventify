import { Component, DestroyRef, computed, inject, input, signal } from '@angular/core';
import { copyToClipboard } from '@eventify/ui/clipboard';
import { highlightJava } from './java-highlight';

export interface SetupStep {
  title: string;
  /** HTML, so names can be set in <code>. */
  text: string;
  /** Shown above the code, like a file or window name. */
  label: string;
  language: 'shell' | 'xml' | 'java';
  code: string;
}

const escapeHtml = (code: string) => code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** Tags in one colour, the values between them in the default one. */
const highlightXml = (code: string) =>
  escapeHtml(code).replace(/(&lt;\/?[\w.-]+&gt;)/g, '<span class="text-sky-300">$1</span>');

/** The command in one colour, its options in another. */
const highlightShell = (code: string) =>
  escapeHtml(code).replace(/^(docker run)/, '<span class="text-primary-300">$1</span>').replace(/(\s-[\w-]+)/g, '<span class="text-slate-400">$1</span>');

const HIGHLIGHT = { shell: highlightShell, xml: highlightXml, java: highlightJava };

/**
 * The setup steps on the website's pages, numbered on a line, each with its copyable code.
 * On a wide screen a step's text sits beside its code, so the steps stay short.
 * Kept light on purpose: the pages advertise, the docs explain how it works.
 */
@Component({
  selector: 'app-get-started',
  standalone: true,
  template: `
    <ol>
      @for (step of highlighted(); track step.title; let i = $index) {
        <li class="relative grid grid-cols-[2rem_minmax(0,1fr)] gap-x-4 lg:grid-cols-[2rem_minmax(0,1fr)_minmax(0,1.7fr)] lg:gap-x-8" [class.pb-10]="!$last">
          @if (!$last) { <span class="absolute top-9 bottom-1 left-[calc(1rem-0.5px)] w-px bg-surface-200 dark:bg-surface-700" aria-hidden="true"></span> }
          <span class="flex h-8 w-8 items-center justify-center rounded-full border border-primary-200 bg-primary-50 text-sm font-semibold text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300">{{ i + 1 }}</span>
          <div class="min-w-0">
            <h3 class="pt-1 text-base font-semibold text-surface-900 dark:text-surface-0">{{ step.title }}</h3>
            <p class="step-text mt-1 text-sm leading-relaxed text-surface-500 dark:text-surface-400" [innerHTML]="step.text"></p>
          </div>

          <div class="col-start-2 mt-4 min-w-0 overflow-hidden rounded-xl border border-slate-800 bg-slate-900 shadow-sm lg:col-start-3 lg:mt-0">
            <div class="flex items-center justify-between border-b border-white/10 py-1.5 pr-1.5 pl-4">
              <span class="text-xs font-medium text-slate-400">{{ step.label }}</span>
              <button type="button" (click)="copy(i)" [attr.aria-label]="copied() === i ? 'Copied' : 'Copy ' + step.label"
                      class="inline-flex cursor-pointer items-center gap-1.5 rounded-md px-2 py-1 text-xs text-slate-400 transition-colors hover:bg-white/5 hover:text-slate-100">
                <i class="pi text-xs" [class]="copied() === i ? 'pi-check text-primary-400' : 'pi-copy'"></i>
                <span aria-live="polite">{{ copied() === i ? 'Copied' : 'Copy' }}</span>
              </button>
            </div>
            <pre class="overflow-x-auto px-4 py-3.5 font-mono text-[0.8rem] leading-relaxed text-slate-200"><code [innerHTML]="step.html"></code></pre>
          </div>
        </li>
      }
    </ol>
  `,
  styles: `
    .step-text ::ng-deep code { font-family: var(--font-mono); font-size: 0.8rem; color: var(--p-surface-700); }
    @media (prefers-color-scheme: dark) { .step-text ::ng-deep code { color: var(--p-surface-200); } }
  `,
})
export class GetStartedComponent {
  steps = input.required<SetupStep[]>();

  /** The steps with their code highlighted, once per change of steps. */
  readonly highlighted = computed(() => this.steps().map(step => ({ ...step, html: HIGHLIGHT[step.language](step.code) })));

  /** The step whose code was just copied, for a moment. */
  readonly copied = signal<number | null>(null);
  private resetCopied?: ReturnType<typeof setTimeout>;

  constructor() {
    inject(DestroyRef).onDestroy(() => clearTimeout(this.resetCopied));
  }

  async copy(index: number) {
    if (!await copyToClipboard(this.steps()[index].code)) return;
    this.copied.set(index);
    clearTimeout(this.resetCopied);
    this.resetCopied = setTimeout(() => this.copied.set(null), 2000);
  }
}
