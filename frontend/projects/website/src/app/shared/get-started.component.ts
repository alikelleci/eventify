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

/** What the applications connect to in the picture next to the steps. */
export interface SetupHub {
  label: string;
  /** A PrimeIcons class, e.g. pi-bolt. */
  icon: string;
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
 * The "get started" block of the website's pages: numbered setup steps with copyable code, and next to them
 * a picture of the applications connecting to what the page is about (Kafka on the home page, the console on its page).
 * Kept light on purpose: the pages advertise, the docs explain how it works.
 */
@Component({
  selector: 'app-get-started',
  standalone: true,
  template: `
    <div class="grid items-start gap-10 grid-cols-[minmax(0,1fr)] lg:grid-cols-[minmax(0,1.35fr)_minmax(0,1fr)] lg:gap-14">

      <!-- The steps, numbered on a line -->
      <ol class="relative">
        @for (step of highlighted(); track step.title; let i = $index) {
          <li class="relative grid grid-cols-[2rem_minmax(0,1fr)] gap-x-4" [class.pb-10]="!$last">
            @if (!$last) { <span class="absolute top-9 bottom-1 left-[calc(1rem-0.5px)] w-px bg-surface-200 dark:bg-surface-700" aria-hidden="true"></span> }
            <span class="flex h-8 w-8 items-center justify-center rounded-full border border-primary-200 bg-primary-50 text-sm font-semibold text-primary-700 dark:border-primary-800 dark:bg-primary-950 dark:text-primary-300">{{ i + 1 }}</span>
            <div class="min-w-0">
              <h3 class="pt-1 text-base font-semibold text-surface-900 dark:text-surface-0">{{ step.title }}</h3>
              <p class="step-text mt-1 text-sm leading-relaxed text-surface-500 dark:text-surface-400" [innerHTML]="step.text"></p>

              <div class="mt-4 overflow-hidden rounded-xl border border-slate-800 bg-slate-900 shadow-sm">
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
            </div>
          </li>
        }
      </ol>

      <!-- The applications, connecting to the hub -->
      <figure class="rounded-2xl border border-surface-200 bg-surface-0 p-6 sm:p-8 lg:sticky lg:top-8 dark:border-surface-800 dark:bg-surface-900">
        <div class="flex flex-col items-center" aria-hidden="true">
          <div class="flex w-full max-w-[16rem] items-center gap-3 rounded-xl border border-primary-300 bg-primary-50 px-4 py-3 shadow-[0_0_0_4px_color-mix(in_srgb,var(--p-primary-500)_12%,transparent)] dark:border-primary-700 dark:bg-primary-950">
            <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary-500 text-white"><i class="pi text-sm" [class]="hub().icon"></i></span>
            <span class="text-sm font-semibold text-surface-900 dark:text-surface-0">{{ hub().label }}</span>
          </div>

          <!-- Flowing towards the hub -->
          <div class="relative h-16 w-full">
            <svg class="absolute inset-0 h-full w-full overflow-visible" viewBox="0 0 300 64" preserveAspectRatio="none" fill="none">
              @for (path of connections; track path) {
                <path [attr.d]="path" class="stroke-surface-300 dark:stroke-surface-600" stroke-width="1" vector-effect="non-scaling-stroke" />
                <path [attr.d]="path" class="connection-flow stroke-primary-500" stroke-width="1.5" stroke-dasharray="3 9" stroke-linecap="round" vector-effect="non-scaling-stroke" />
              }
            </svg>
          </div>

          <div class="grid w-full grid-cols-3 gap-2">
            @for (app of apps; track app) {
              <div class="flex min-w-0 items-center justify-center gap-1.5 rounded-lg border border-surface-200 px-1 py-2 dark:border-surface-700">
                <span class="h-1.5 w-1.5 shrink-0 rounded-full bg-primary-500"></span>
                <span class="truncate font-mono text-xs text-surface-700 dark:text-surface-200">{{ app }}</span>
              </div>
            }
          </div>
        </div>
      </figure>
    </div>
  `,
  styles: `
    .step-text ::ng-deep code { font-family: var(--font-mono); font-size: 0.8rem; color: var(--p-surface-700); }
    @media (prefers-color-scheme: dark) { .step-text ::ng-deep code { color: var(--p-surface-200); } }
    .connection-flow { animation: connection-flow 1.2s linear infinite; }
    @keyframes connection-flow { to { stroke-dashoffset: -12; } }
    @media (prefers-reduced-motion: reduce) { .connection-flow { animation: none; } }
  `,
})
export class GetStartedComponent {
  steps = input.required<SetupStep[]>();
  hub = input.required<SetupHub>();

  /** The steps with their code highlighted, once per change of steps. */
  readonly highlighted = computed(() => this.steps().map(step => ({ ...step, html: HIGHLIGHT[step.language](step.code) })));

  /** The same example applications on every page. */
  readonly apps = ['orders', 'payments', 'shipping'];

  /** From the centre of each application up to the hub. */
  readonly connections = ['M50 64 C50 32 150 32 150 0', 'M150 64 L150 0', 'M250 64 C250 32 150 32 150 0'];

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
