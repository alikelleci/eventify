import { Component, ElementRef, HostListener, ViewChild, computed, effect, inject, signal, viewChildren } from '@angular/core';
import { TooltipModule, Tooltip } from 'primeng/tooltip';
import { AppEntry, BackendService } from '@eventify/ui/services/backend.service';
import { InstanceListComponent } from '@eventify/ui/components/instance-list.component';
import { TOOLTIP_DELAY_MS, appState, dotClass } from '@eventify/ui/status';

/** From this many applications on, the dropdown has a filter at the top. */
const FILTER_FROM = 7;

/**
 * Picks the application to inspect, from the ones connected to the console. The dropdown looks and works like the recent searches:
 * the same panel, the border marks the active app, and the arrow keys move through the list; with many applications a filter
 * narrows the list. Each application is one line, with a dot in the colour of its instance worst off. The highlighted row, by
 * mouse or by arrow keys, shows how each of its instances is doing.
 */
@Component({
  selector: 'app-app-switcher',
  standalone: true,
  imports: [TooltipModule, InstanceListComponent],
  host: { class: 'relative block', '(focusout)': 'onFocusout($event)' },
  template: `
    <button #button type="button" aria-haspopup="listbox" [attr.aria-expanded]="open()" aria-controls="app-switcher-list"
            class="flex items-center gap-2 h-8 w-full pl-3 pr-2.5 rounded border text-sm text-slate-100 transition-colors cursor-pointer outline-none focus-visible:border-slate-500 focus-visible:bg-slate-700/60"
            [class]="open() ? 'border-slate-500 bg-slate-700/60' : 'border-slate-700 bg-slate-800 hover:border-slate-600'"
            (click)="toggle()" (keydown)="onKeydown($event)">
      <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(activeState().tone)"></span>
      <span class="flex-1 text-left truncate">{{ backend.activeApp()?.name }}</span>
      <i class="pi pi-chevron-down text-xs text-slate-400 transition-transform" [class.rotate-180]="open()"></i>
    </button>

    @if (open()) {
      <div class="absolute top-full right-0 mt-1 min-w-full w-80 bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
        <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Applications</div>

        @if (showFilter()) {
          <div class="relative border-b border-surface-100 dark:border-surface-800">
            <i class="pi pi-search pointer-events-none absolute top-1/2 left-3 -translate-y-1/2 text-xs text-surface-400"></i>
            <input #filter type="text" placeholder="Filter applications" aria-label="Filter applications" aria-controls="app-switcher-list"
                   [value]="query()" (input)="onQuery($any($event.target).value)" (keydown)="onKeydown($event)"
                   class="w-full h-9 pl-8 pr-3 bg-transparent text-sm text-surface-900 dark:text-surface-0 placeholder:text-surface-400 outline-none" />
          </div>
        }

        <div id="app-switcher-list" role="listbox" aria-label="Applications" class="max-h-80 overflow-y-auto"
             (mouseleave)="highlightedIndex.set(-1)">
          @for (app of filtered(); track app.name) {
            @let state = appState(app.nodes);
            <!-- One line: the dot in the colour of the instance worst off, the name, and the number of instances. The instances
                 themselves are in the tooltip. -->
            <div role="option" [attr.aria-selected]="isActive(app)" [attr.aria-label]="app.name + ', ' + state.text + ', ' + instancesLabel(app)"
                 class="flex items-center gap-2.5 px-3 py-2 border-l-2 cursor-pointer transition-colors"
                 [class]="rowClass(app, $index)"
                 [pTooltip]="instances" tooltipEvent="focus" tooltipPosition="left" tooltipStyleClass="max-w-none" [showDelay]="tooltipDelay"
                 (mouseenter)="highlightedIndex.set($index)"
                 (mousedown)="$event.preventDefault(); select(app)">
              <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(state.tone)"></span>
              <span class="text-sm truncate"
                    [class]="isActive(app) ? 'text-primary-600 dark:text-primary-400 font-medium' : 'text-surface-900 dark:text-surface-100'">{{ app.name }}</span>
              <span class="ml-auto pl-2 shrink-0 text-xs tabular-nums text-surface-400">{{ instancesLabel(app) }}</span>
            </div>
            <ng-template #instances><app-instance-list [instances]="app.nodes" /></ng-template>
          } @empty {
            <div class="px-3 py-6 text-center text-sm text-surface-400">No applications match “{{ query() }}”</div>
          }
        </div>
      </div>
    }
  `,
})
export class AppSwitcherComponent {
  readonly backend = inject(BackendService);
  private readonly host: ElementRef<HTMLElement> = inject(ElementRef);
  readonly appState = appState;
  readonly dotClass = dotClass;

  /** "1 instance", "3 instances". */
  instancesLabel(app: AppEntry): string {
    return app.nodes.length === 1 ? '1 instance' : `${app.nodes.length} instances`;
  }

  /** The chosen application as last refreshed: activeApp() only changes when another one is picked. */
  readonly activeState = computed(() => appState(this.backend.apps().find(app => app.name === this.backend.activeApp()?.name)?.nodes ?? []));

  /** Moving over the list shows no tooltips on the way, only where the mouse or the arrow keys stop. */
  readonly tooltipDelay = TOOLTIP_DELAY_MS;

  /** The rows' tooltips, in the order of the rows. */
  private readonly tooltips = viewChildren(Tooltip);

  constructor() {
    // The highlighted row shows its instances, whether the mouse or the arrow keys moved there; the other rows don't.
    effect(() => {
      const highlighted = this.highlightedIndex();
      this.tooltips().forEach((tooltip, index) => index === highlighted ? tooltip.activate() : tooltip.deactivate());
    });
  }

  @ViewChild('button') button!: ElementRef<HTMLButtonElement>;
  @ViewChild('filter') filterInput?: ElementRef<HTMLInputElement>;

  open = signal(false);
  /** The row under the mouse or the arrow keys: what Enter picks. Not the active app, which is backend.activeApp(). */
  readonly highlightedIndex = signal(-1);

  readonly query = signal('');
  readonly showFilter = computed(() => this.backend.apps().length >= FILTER_FROM);
  readonly filtered = computed(() => {
    const query = this.query().trim().toLowerCase();
    return query ? this.backend.apps().filter(app => app.name.toLowerCase().includes(query)) : this.backend.apps();
  });

  toggle() {
    if (this.open()) {
      this.open.set(false);
    } else {
      // Like the recent searches: nothing highlighted until the mouse or the arrow keys move onto a row.
      this.highlightedIndex.set(-1);
      this.query.set('');
      this.open.set(true);
      // With a filter, typing goes straight into it.
      if (this.showFilter()) setTimeout(() => this.filterInput?.nativeElement.focus());
    }
  }

  onQuery(value: string) {
    this.query.set(value);
    // Enter picks the first match.
    this.highlightedIndex.set(this.filtered().length ? 0 : -1);
  }

  // Clicking anywhere outside closes the dropdown (also where a click doesn't focus the button, as in Safari).
  @HostListener('document:mousedown', ['$event'])
  onDocumentMousedown(e: MouseEvent) {
    if (this.open() && !this.host.nativeElement.contains(e.target as Node)) this.open.set(false);
  }

  // Tabbing away closes it; moving between the button and the filter doesn't.
  onFocusout(e: FocusEvent) {
    if (!this.host.nativeElement.contains(e.relatedTarget as Node | null)) this.open.set(false);
  }

  // The event and command lists listen for the arrow keys (and the page for Escape) on the whole window.
  // A key handled here stops at the switcher, so it doesn't also move the list or clear its selection.
  onKeydown(e: KeyboardEvent) {
    const apps = this.filtered();
    switch (e.key) {
      case 'ArrowDown':
        if (!this.open()) this.toggle();
        else this.highlightedIndex.update(i => Math.min(i + 1, apps.length - 1));
        break;
      case 'ArrowUp':
        if (!this.open()) this.toggle();
        else this.highlightedIndex.update(i => Math.max(i - 1, 0));
        break;
      case 'Enter':
      case ' ':
        if (!this.open()) return;  // the button's own click opens it
        if (e.key === ' ' && e.target === this.filterInput?.nativeElement) return;  // a space in the filter is just typed
        this.select(apps[this.highlightedIndex()]);
        break;
      case 'Escape':
        if (!this.open()) return;  // nothing to close: Escape keeps its page-wide meaning
        this.open.set(false);
        this.button.nativeElement.focus();
        break;
      default:
        return;
    }
    e.preventDefault();
    e.stopPropagation();
  }

  // The green border and text mark the active app; grey marks the highlighted row, also on the active app.
  rowClass(app: AppEntry, index: number): string {
    const current = this.isActive(app);
    const highlighted = index === this.highlightedIndex();
    return (current ? 'border-l-primary-500 ' : 'border-l-transparent ')
      + (highlighted ? 'bg-surface-100 dark:bg-surface-800' : current ? 'bg-primary-50 dark:bg-primary-950' : '');
  }

  // By name: the list is refreshed every few seconds with new objects.
  isActive(app: AppEntry): boolean {
    return app.name === this.backend.activeApp()?.name;
  }

  select(app: AppEntry | undefined) {
    if (app) this.backend.setActiveApp(app);
    this.open.set(false);
    this.button.nativeElement.focus();
  }
}
