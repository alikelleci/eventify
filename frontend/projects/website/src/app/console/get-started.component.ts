import { Component, DestroyRef, inject, signal } from '@angular/core';
import { copyToClipboard } from '@eventify/ui/clipboard';
import { highlightJava } from '../shared/java-highlight';

interface SetupStep {
  title: string;
  /** HTML, so names can be set in <code>. */
  text: string;
  /** Shown above the code, like a file or window name. */
  label: string;
  code: string;
  /** The code as HTML, highlighted once. */
  html: string;
}

const escapeHtml = (code: string) => code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** Tags in one colour, the values between them in the default one. */
const highlightXml = (code: string) =>
  escapeHtml(code).replace(/(&lt;\/?[\w.-]+&gt;)/g, '<span class="text-sky-300">$1</span>');

/** The command in one colour, its options in another. */
const highlightShell = (code: string) =>
  escapeHtml(code).replace(/^(docker run)/, '<span class="text-primary-300">$1</span>').replace(/(\s-[\w-]+)/g, '<span class="text-slate-400">$1</span>');

const DOCKER = 'docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest';

const DEPENDENCY = `<dependency>
  <groupId>io.github.alikelleci</groupId>
  <artifactId>eventify-console-plugin</artifactId>
  <version>x.y.z</version>
</dependency>`;

const REGISTER = `Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder()
        .url("http://localhost:8080")
        .build())
    .build();`;

/** The same three steps as in the console's documentation. */
const STEPS: SetupStep[] = [
  {
    title: 'Run the console',
    text: 'A single container, nothing else to install. Then open <code>localhost:8080</code>.',
    label: 'Terminal', code: DOCKER, html: highlightShell(DOCKER),
  },
  {
    title: 'Add the plugin',
    text: 'Add the console plugin to your application.',
    label: 'pom.xml', code: DEPENDENCY, html: highlightXml(DEPENDENCY),
  },
  {
    title: 'Connect your application',
    text: 'Point it at the console, and you’re done.',
    label: 'Java', code: REGISTER, html: highlightJava(REGISTER),
  },
];

/**
 * How to run the console: the setup steps with their code, and next to them the applications connecting to it.
 * Kept light on purpose: the page advertises the console, the docs explain how it works.
 */
@Component({
  selector: 'app-get-started',
  standalone: true,
  template: `
    <div class="grid items-start gap-10 grid-cols-[minmax(0,1fr)] lg:grid-cols-[minmax(0,1.35fr)_minmax(0,1fr)] lg:gap-14">

      <!-- The steps, numbered on a line -->
      <ol class="relative">
        @for (step of steps; track step.title; let i = $index) {
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

      <!-- Your applications connect to the console by themselves -->
      <figure class="rounded-2xl border border-surface-200 bg-surface-0 p-6 sm:p-8 lg:sticky lg:top-8 dark:border-surface-800 dark:bg-surface-900">
        <div class="flex flex-col items-center" aria-hidden="true">
          <div class="flex w-full max-w-[16rem] items-center gap-3 rounded-xl border border-primary-300 bg-primary-50 px-4 py-3 shadow-[0_0_0_4px_color-mix(in_srgb,var(--p-primary-500)_12%,transparent)] dark:border-primary-700 dark:bg-primary-950">
            <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary-500 text-white"><i class="pi pi-bolt text-sm"></i></span>
            <span class="text-sm font-semibold text-surface-900 dark:text-surface-0">Eventify Console</span>
          </div>

          <!-- Flowing towards the console -->
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

        <figcaption class="mt-8 text-center">
          <span class="block text-base font-semibold text-surface-900 dark:text-surface-0">Your applications show up by themselves</span>
          <span class="mt-1.5 block text-sm leading-relaxed text-surface-500 dark:text-surface-400">Start an application and it's in the console. No ports to open, nothing else to configure.</span>
        </figcaption>
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
  readonly steps = STEPS;
  readonly apps = ['orders', 'payments', 'shipping'];

  /** From the centre of each application up to the console. */
  readonly connections = ['M50 64 C50 32 150 32 150 0', 'M150 64 L150 0', 'M250 64 C250 32 150 32 150 0'];

  /** The step whose code was just copied, for a moment. */
  readonly copied = signal<number | null>(null);
  private resetCopied?: ReturnType<typeof setTimeout>;

  constructor() {
    inject(DestroyRef).onDestroy(() => clearTimeout(this.resetCopied));
  }

  async copy(index: number) {
    if (!await copyToClipboard(this.steps[index].code)) return;
    this.copied.set(index);
    clearTimeout(this.resetCopied);
    this.resetCopied = setTimeout(() => this.copied.set(null), 2000);
  }
}
