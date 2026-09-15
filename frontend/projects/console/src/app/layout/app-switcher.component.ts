import { Component, ElementRef, HostListener, ViewChild, inject, signal } from '@angular/core';
import { AppEntry, BackendService } from '@eventify/ui/services/backend.service';

/**
 * Picks the application to inspect, from the ones connected to the console. The dropdown looks and works like the recent searches:
 * the same panel, the border marks the active app, and the arrow keys move through the list.
 */
@Component({
  selector: 'app-app-switcher',
  standalone: true,
  host: { class: 'relative block' },
  template: `
    <button #button type="button" aria-haspopup="listbox" [attr.aria-expanded]="open()" aria-controls="app-switcher-list"
            class="flex items-center gap-2 h-8 w-full pl-3 pr-2.5 rounded border text-sm text-slate-100 transition-colors cursor-pointer outline-none focus-visible:border-slate-500 focus-visible:bg-slate-700/60"
            [class]="open() ? 'border-slate-500 bg-slate-700/60' : 'border-slate-700 bg-slate-800 hover:border-slate-600'"
            (click)="toggle()" (keydown)="onKeydown($event)" (blur)="open.set(false)">
      <span class="flex-1 text-left truncate">{{ backend.activeApp()?.name }}</span>
      <i class="pi pi-chevron-down text-xs text-slate-400 transition-transform" [class.rotate-180]="open()"></i>
    </button>

    @if (open()) {
      <div id="app-switcher-list" role="listbox" aria-label="Applications"
           class="absolute top-full right-0 mt-1 min-w-full w-64 bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
        <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Applications</div>
        <div class="max-h-64 overflow-y-auto">
          @for (app of backend.apps(); track app.name) {
            <!-- min-h-10: the same height as a recent search, whose remove button makes that row taller -->
            <div role="option" [attr.aria-selected]="isActive(app)"
                 class="flex items-center gap-2 min-h-10 px-3 py-2 border-l-2 cursor-pointer transition-colors"
                 [class]="rowClass(app, $index)" [title]="instancesLabel(app)"
                 (mouseenter)="highlightedIndex.set($index)"
                 (mousedown)="$event.preventDefault(); select(app)">
              <i class="pi pi-server text-surface-400 text-xs shrink-0"></i>
              <span class="flex-1 text-sm truncate"
                    [class]="isActive(app) ? 'text-primary-600 dark:text-primary-400' : 'text-surface-900 dark:text-surface-100'">{{ app.name }}</span>
              <!-- The instances connected right now; none while the application restarts -->
              <span class="text-xs shrink-0" [class]="app.nodes.length ? 'text-surface-400' : 'text-orange-500'">{{ app.nodes.length || 'offline' }}</span>
            </div>
          }
        </div>
      </div>
    }
  `,
})
export class AppSwitcherComponent {
  readonly backend = inject(BackendService);
  private readonly host: ElementRef<HTMLElement> = inject(ElementRef);

  @ViewChild('button') button!: ElementRef<HTMLButtonElement>;

  open = signal(false);
  /** The row under the mouse or the arrow keys: what Enter picks. Not the active app, which is backend.activeApp(). */
  highlightedIndex = signal(-1);

  toggle() {
    if (this.open()) {
      this.open.set(false);
    } else {
      // Like the recent searches: nothing highlighted until the mouse or the arrow keys move onto a row.
      this.highlightedIndex.set(-1);
      this.open.set(true);
    }
  }

  // Clicking anywhere outside closes the dropdown (also where a click doesn't focus the button, as in Safari).
  @HostListener('document:mousedown', ['$event'])
  onDocumentMousedown(e: MouseEvent) {
    if (this.open() && !this.host.nativeElement.contains(e.target as Node)) this.open.set(false);
  }

  // The event and command lists listen for the arrow keys (and the page for Escape) on the whole window.
  // A key handled here stops at the switcher, so it doesn't also move the list or clear its selection.
  onKeydown(e: KeyboardEvent) {
    const apps = this.backend.apps();
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
        this.select(apps[this.highlightedIndex()]);
        break;
      case 'Escape':
        if (!this.open()) return;  // nothing to close: Escape keeps its page-wide meaning
        this.open.set(false);
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
    return (current ? 'border-primary-500 ' : 'border-transparent ')
      + (highlighted ? 'bg-surface-100 dark:bg-surface-800' : current ? 'bg-primary-50 dark:bg-primary-950' : '');
  }

  // By name: the list is refreshed every few seconds with new objects.
  isActive(app: AppEntry): boolean {
    return app.name === this.backend.activeApp()?.name;
  }

  instancesLabel(app: AppEntry): string {
    const count = app.nodes.length;
    return count === 0 ? 'No instances connected' : count === 1 ? '1 instance connected' : `${count} instances connected`;
  }

  select(app: AppEntry | undefined) {
    if (app) this.backend.setActiveApp(app);
    this.open.set(false);
    this.button.nativeElement.focus();
  }
}
