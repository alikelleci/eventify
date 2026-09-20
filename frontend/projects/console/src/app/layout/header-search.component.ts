import { Component, ElementRef, HostListener, ViewChild, computed, effect, inject, input, signal, untracked } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { AggregateRef, SearchService } from '../services/search.service';
import { BackendService } from '@eventify/ui/services/backend.service';

@Component({
  selector: 'app-header-search',
  standalone: true,
  imports: [FormsModule],
  template: `
    <div class="relative">
      <div class="flex items-stretch">
        @if (types().length > 1) {
          <!-- One application can hold several aggregates, and two of them can have the same identifier. -->
          <div class="relative">
            <button #typeButton type="button" aria-haspopup="listbox" [attr.aria-expanded]="typesOpen()" [attr.aria-controls]="typeListId()"
                    class="flex items-center justify-between gap-1.5 shrink-0 rounded-l border border-r-0 transition-colors cursor-pointer outline-none"
                    [class]="dark()
                      ? 'h-8 w-24 pl-2.5 pr-2 text-sm border-slate-700 bg-slate-800 text-slate-300 hover:border-slate-600'
                      : 'h-11 w-28 pl-3.5 pr-2 text-base border-surface-200 bg-surface-50 text-surface-600 hover:border-surface-300 dark:border-surface-700 dark:bg-surface-800 dark:text-surface-300'"
                    (click)="typesOpen.set(!typesOpen())" (blur)="typesOpen.set(false)"
                    (keydown.escape)="typesOpen.set(false)">
              <span class="truncate">{{ selectedType() }}</span>
              <i class="pi pi-chevron-down text-[10px] transition-transform" [class.rotate-180]="typesOpen()"
                 [class]="dark() ? 'text-slate-400' : 'text-surface-400'"></i>
            </button>

            @if (typesOpen()) {
              <div [id]="typeListId()" role="listbox" aria-label="Aggregate"
                   class="absolute top-full left-0 mt-1 min-w-full w-44 bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
                <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Aggregate</div>
                @for (type of types(); track type) {
                  <div role="option" [attr.aria-selected]="type === selectedType()"
                       class="px-3 py-2 border-l-2 cursor-pointer transition-colors"
                       [class]="type === selectedType() ? 'border-primary-500 bg-primary-50 dark:bg-primary-950' : 'border-transparent hover:bg-surface-100 dark:hover:bg-surface-800'"
                       (mousedown)="$event.preventDefault(); chooseType(type)">
                    <span class="text-sm"
                          [class]="type === selectedType() ? 'text-primary-600 dark:text-primary-400 font-medium' : 'text-surface-900 dark:text-surface-100'">{{ type }}</span>
                  </div>
                }
              </div>
            }
          </div>
        }
        <div class="relative flex-1 min-w-0">
          <i class="absolute top-1/2 -translate-y-1/2 pointer-events-none"
             [class]="(search.loading() ? 'pi pi-spin pi-spinner ' : 'pi pi-search ') + (dark() ? 'left-2.5 text-xs text-slate-400' : 'left-3.5 text-sm text-surface-400')"></i>
          <input #input type="text" spellcheck="false" autocomplete="off" placeholder="Search aggregate ID…"
                 role="combobox" aria-autocomplete="list" [attr.aria-expanded]="dropdownOpen()" [attr.aria-controls]="listId()"
                 [attr.aria-activedescendant]="highlightedIndex() >= 0 ? listId() + '-' + highlightedIndex() : null"
                 class="w-full border focus:outline-none transition-colors"
                 [class]="(types().length > 1 ? 'rounded-r ' : 'rounded ') + (dark()
                   ? 'h-8 pl-8 pr-8 text-sm border-slate-700 bg-slate-800 text-slate-100 placeholder:text-slate-400 hover:border-slate-600 focus:border-slate-500 focus:bg-slate-700/60'
                   : 'h-11 pl-10 pr-3 text-base rounded-md border-surface-200 bg-surface-50 text-surface-900 placeholder:text-surface-400 hover:border-surface-300 focus:border-primary-500 dark:border-surface-700 dark:bg-surface-800 dark:text-surface-100')"
                 [(ngModel)]="query" (input)="highlightedIndex.set(-1)" (keydown)="onInputKeydown($event)"
                 (focus)="focused.set(true); highlightedIndex.set(-1)" (blur)="focused.set(false)" />
          @if (!focused() && dark()) {
            <kbd class="absolute right-2 top-1/2 -translate-y-1/2 px-1.5 pb-[1.5px] rounded border border-slate-600 text-[10px] leading-4 font-sans text-slate-400 pointer-events-none">/</kbd>
          }
        </div>
      </div>

      @if (dropdownOpen()) {
        <div [id]="listId()" role="listbox" class="absolute top-full inset-x-0 mt-1 bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
          <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Recent</div>
          @for (aggregate of search.recent(); track aggregate.type + '/' + aggregate.id) {
            <!-- The green border and text mark the open aggregate (like the active app in the app switcher); grey marks the keyboard/mouse position -->
            <div [id]="listId() + '-' + $index" role="option" [attr.aria-selected]="$index === highlightedIndex()"
                 class="flex items-center justify-between px-3 py-2 border-l-2 cursor-pointer transition-colors"
                 [class]="rowClass(aggregate, $index)"
                 (mouseenter)="highlightedIndex.set($index)"
                 (mousedown)="$event.preventDefault(); open(aggregate.id, aggregate.type)">
              <div class="flex items-center gap-2 min-w-0">
                <i class="pi pi-history text-surface-400 text-xs shrink-0"></i>
                <span class="text-sm truncate"
                      [class]="isOpen(aggregate) ? 'text-primary-600 dark:text-primary-400' : 'text-surface-900 dark:text-surface-100'">{{ aggregate.id }}</span>
                @if (types().length > 1) {
                  <span class="text-xs text-surface-400 shrink-0">{{ aggregate.type }}</span>
                }
              </div>
              <button class="text-surface-300 hover:text-surface-500 dark:text-surface-600 dark:hover:text-surface-400 ml-2 shrink-0 cursor-pointer"
                      (mousedown)="$event.preventDefault(); $event.stopPropagation(); remove(aggregate)">
                <i class="pi pi-times text-xs"></i>
              </button>
            </div>
          }
        </div>
      }
    </div>
  `,
})
export class HeaderSearchComponent {
  readonly search = inject(SearchService);
  private readonly backend = inject(BackendService);

  /** 'dark' sits in the dark app header; 'light' in the white search bar shown below it on mobile. */
  variant = input<'dark' | 'light'>('dark');
  dark = computed(() => this.variant() === 'dark');

  @ViewChild('input') input!: ElementRef<HTMLInputElement>;

  query = signal('');
  focused = signal(false);
  /** Highlighted recent search, moved with the arrow keys or the mouse; -1 means the text in the box. */
  highlightedIndex = signal(-1);
  dropdownOpen = computed(() => this.focused() && this.search.recent().length > 0);
  listId = computed(() => `recent-searches-${this.variant()}`);

  /** The aggregates this application holds; the box searches in the chosen one. */
  readonly types = computed(() => this.backend.activeApp()?.aggregateTypes ?? []);
  readonly selectedType = signal('');
  readonly typesOpen = signal(false);
  readonly typeListId = computed(() => `aggregate-types-${this.variant()}`);

  constructor() {
    // Show the open aggregate in the box whenever the URL changes, and search in its aggregate.
    effect(() => {
      const open = this.search.current();
      this.query.set(open?.id ?? '');
      if (open) this.selectedType.set(open.type);
    });
    // Follow the application: with one aggregate there is nothing to choose.
    effect(() => {
      const types = this.types();
      if (types.length && !types.includes(untracked(() => this.selectedType()))) this.selectedType.set(types[0]);
    });
    this.search.focus$.pipe(takeUntilDestroyed()).subscribe(() => this.focusIfVisible());
  }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA' && this.focusIfVisible()) e.preventDefault();
  }

  onInputKeydown(e: KeyboardEvent) {
    const recent = this.search.recent();
    switch (e.key) {
      case 'ArrowDown':
        if (!recent.length) return;
        e.preventDefault();
        this.highlightedIndex.update(i => Math.min(i + 1, recent.length - 1));
        break;
      case 'ArrowUp':
        if (!recent.length) return;
        e.preventDefault();
        this.highlightedIndex.update(i => Math.max(i - 1, -1));  // above the first item: back to the typed text
        break;
      case 'Enter': {
        e.preventDefault();
        const highlighted = this.highlightedIndex() >= 0 ? recent[this.highlightedIndex()] : null;
        this.open(highlighted?.id ?? this.query(), highlighted?.type);
        break;
      }
      case 'Escape':
        this.input.nativeElement.blur();
        break;
    }
  }

  isOpen(aggregate: AggregateRef): boolean {
    const current = this.search.current();
    return !!current && current.type === aggregate.type && current.id === aggregate.id;
  }

  rowClass(aggregate: AggregateRef, index: number): string {
    const open = this.isOpen(aggregate);
    const highlighted = index === this.highlightedIndex();
    return (open ? 'border-primary-500 ' : 'border-transparent ')
      + (highlighted ? 'bg-surface-100 dark:bg-surface-800' : open ? 'bg-primary-50 dark:bg-primary-950' : '');
  }

  remove(aggregate: AggregateRef) {
    this.search.removeRecent(aggregate);
    this.highlightedIndex.update(i => Math.min(i, this.search.recent().length - 1));
  }

  /** Both variants are rendered (one hidden by breakpoint); only the visible one takes focus. */
  private focusIfVisible(): boolean {
    const el = this.input.nativeElement;
    if (!el.offsetParent) return false;
    el.focus();
    el.select();
    return true;
  }

  /**
   * Which aggregate the box searches in; the open page stays as it is until something is searched. The focus stays on
   * the button: moving it to the search box would open the recent searches right after choosing.
   */
  chooseType(type: string) {
    this.selectedType.set(type);
    this.typesOpen.set(false);
  }

  open(id: string, type = this.selectedType()) {
    this.search.open(type, id);
    this.input.nativeElement.blur();
  }
}
