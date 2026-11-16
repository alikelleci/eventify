import { Component, ElementRef, HostListener, ViewChild, computed, effect, inject, input, signal, untracked } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { AggregateRef, SearchService } from '../services/search.service';
import { BackendService } from '@eventify/ui/services/backend.service';

@Component({
  selector: 'app-header-search',
  standalone: true,
  imports: [FormsModule],
  // Only the segments: the page puts them in a bar after the application, so the three read as one address. The bar is the
  // positioned box the dropdowns hang from.
  host: { class: 'flex min-w-0 flex-1 items-stretch' },
  template: `
    @if (types().length > 1) {
      <!-- One application can hold several aggregates, and two of them can have the same identifier. -->
      <div class="relative flex shrink-0">
        <button #typeButton type="button" aria-haspopup="listbox" [attr.aria-expanded]="typesOpen()" [attr.aria-controls]="typeListId()"
                [attr.aria-label]="'Aggregate type ' + selectedType()"
                class="flex items-center gap-1.5 transition-colors cursor-pointer outline-none" [class]="segmentClass()"
                (click)="typesOpen.set(!typesOpen())" (blur)="typesOpen.set(false)"
                (keydown.escape)="typesOpen.set(false)">
          <span class="truncate">{{ selectedType() }}</span>
          <i class="pi pi-chevron-down text-[10px] opacity-50 transition-transform" [class.rotate-180]="typesOpen()"></i>
        </button>

        @if (typesOpen()) {
          <div [id]="typeListId()" role="listbox" aria-label="Aggregate type"
               class="absolute top-full left-0 mt-2 min-w-full w-44 font-sans bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
            <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Aggregate type</div>
            @for (type of types(); track type) {
              <div role="option" [attr.aria-selected]="type === selectedType()"
                   class="px-3 py-2 border-l-2 cursor-pointer transition-colors"
                   [class]="type === selectedType() ? 'border-primary-500 bg-primary-50 dark:bg-primary-950' : 'border-transparent hover:bg-surface-100 dark:hover:bg-surface-800'"
                   (mousedown)="$event.preventDefault(); chooseType(type)">
                <span class="text-sm font-mono"
                      [class]="type === selectedType() ? 'text-primary-600 dark:text-primary-400 font-medium' : 'text-surface-900 dark:text-surface-100'">{{ type }}</span>
              </div>
            }
          </div>
        }
      </div>
    } @else {
      <!-- Nothing to choose, but still part of the address -->
      <span class="flex shrink-0 items-center" [class]="staticClass()">{{ selectedType() }}</span>
    }

    <span class="flex items-center select-none" [class]="dark() ? 'text-slate-600' : 'text-surface-300 dark:text-surface-600'" aria-hidden="true">/</span>

    <input #input type="text" spellcheck="false" autocomplete="off" placeholder="aggregate-id"
           [attr.aria-label]="'ID of the ' + selectedType() + ' to open'"
           role="combobox" aria-autocomplete="list" [attr.aria-expanded]="dropdownOpen()" [attr.aria-controls]="listId()"
           [attr.aria-activedescendant]="highlightedIndex() >= 0 ? listId() + '-' + highlightedIndex() : null"
           class="min-w-0 flex-1 bg-transparent outline-none" [class]="inputClass()"
           [(ngModel)]="query" (input)="highlightedIndex.set(-1)" (keydown)="onInputKeydown($event)"
           (focus)="focused.set(true); highlightedIndex.set(-1)" (blur)="focused.set(false)" />

    <!-- What Enter does, or how to get here -->
    @if (hero()) {
      <button type="button" (click)="open(query())" [disabled]="!query().trim()"
              class="my-2 mr-2 hidden sm:flex items-center gap-2 rounded-lg px-4 font-sans text-sm font-medium transition-colors outline-none focus-visible:ring-2 focus-visible:ring-primary-400 enabled:cursor-pointer"
              [class]="query().trim() ? 'bg-primary-500 text-white hover:bg-primary-600' : 'bg-surface-100 text-surface-400 dark:bg-surface-800'">
        Open <span class="text-xs opacity-70">↵</span>
      </button>
    } @else if (search.loading()) {
      <i class="pi pi-spin pi-spinner self-center pr-3 text-xs" [class]="dark() ? 'text-slate-400' : 'text-surface-400'"></i>
    } @else if (focused() ? changed() : dark()) {
      <kbd class="mr-2.5 self-center px-1.5 rounded border text-[10px] leading-4 font-sans pointer-events-none"
           [class]="dark() ? 'border-slate-600 text-slate-400' : 'border-surface-300 text-surface-400 dark:border-surface-600'">{{ focused() ? '↵' : '/' }}</kbd>
    }

    @if (dropdownOpen()) {
      <div [id]="listId()" role="listbox" class="absolute top-full -inset-x-px mt-2 font-sans bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
        <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Recently opened</div>
        @for (aggregate of search.recent(); track aggregate.app + '/' + aggregate.type + '/' + aggregate.id) {
          <!-- The green border and text mark the open aggregate (like the active app in the app switcher); gray marks the keyboard/mouse position -->
          <div [id]="listId() + '-' + $index" role="option" [attr.aria-selected]="$index === highlightedIndex()"
               class="group flex items-center gap-2 px-3 py-2 border-l-2 cursor-pointer transition-colors"
               [class]="rowClass(aggregate, $index)"
               (mouseenter)="highlightedIndex.set($index)"
               (mousedown)="$event.preventDefault(); open(aggregate.id, aggregate.type, aggregate.app)">
            <span class="text-xs font-mono text-surface-400 shrink-0">{{ aggregate.app }} / {{ aggregate.type }} /</span>
            <span class="text-sm font-mono truncate"
                  [class]="isOpen(aggregate) ? 'text-primary-600 dark:text-primary-400' : 'text-surface-900 dark:text-surface-100'">{{ aggregate.id }}</span>
            <button class="ml-auto pl-2 shrink-0 text-surface-300 opacity-0 group-hover:opacity-100 hover:text-surface-500 dark:text-surface-600 dark:hover:text-surface-400 cursor-pointer"
                    aria-label="Remove from recently opened"
                    (mousedown)="$event.preventDefault(); $event.stopPropagation(); remove(aggregate)">
              <i class="pi pi-times text-xs"></i>
            </button>
          </div>
        }
      </div>
    }
  `,
})
export class HeaderSearchComponent {
  readonly search = inject(SearchService);
  private readonly backend = inject(BackendService);

  /** 'dark' sits in the dark app header; 'light' in the white bar shown below it on mobile; 'hero' is the large one on the start page. */
  variant = input<'dark' | 'light' | 'hero'>('dark');
  dark = computed(() => this.variant() === 'dark');
  hero = computed(() => this.variant() === 'hero');

  @ViewChild('input') input!: ElementRef<HTMLInputElement>;

  query = signal('');
  focused = signal(false);
  /** Highlighted recent search, moved with the arrow keys or the mouse; -1 means the text in the box. */
  highlightedIndex = signal(-1);
  /** The start page lists the recent aggregates below its box, so it has no dropdown. */
  dropdownOpen = computed(() => !this.hero() && this.focused() && this.search.recent().length > 0);
  /** Whether Enter would open something other than what is open now. */
  changed = computed(() => {
    const id = this.query().trim();
    const current = this.search.current();
    return !!id && (current?.id !== id || current?.type !== this.selectedType() || current?.app !== this.backend.activeApp()?.name);
  });
  listId = computed(() => `recent-searches-${this.variant()}`);

  /** The aggregates this application holds; the box searches in the chosen one. */
  readonly types = computed(() => this.backend.activeApp()?.aggregateTypes ?? []);
  readonly selectedType = signal('');
  readonly typesOpen = signal(false);
  readonly typeListId = computed(() => `aggregate-types-${this.variant()}`);
  readonly segmentClass = computed(() => {
    switch (this.variant()) {
      case 'dark': return 'h-full px-3 text-slate-300 hover:bg-slate-700/50';
      case 'light': return 'h-full px-3 text-surface-500 hover:bg-surface-100 dark:hover:bg-surface-800';
      default: return 'h-full px-2.5 sm:px-4 text-surface-500 hover:bg-surface-100 dark:hover:bg-surface-800';
    }
  });
  /** A segment without a choice: the same, without the hover. */
  readonly staticClass = computed(() => this.segmentClass().split(' ').filter(c => !c.startsWith('hover:') && !c.startsWith('dark:hover:')).join(' '));
  readonly inputClass = computed(() => {
    switch (this.variant()) {
      case 'dark': return 'pl-3 pr-2 text-slate-50 placeholder:text-slate-500';
      case 'light': return 'pl-3 pr-3 text-surface-900 placeholder:text-surface-400 dark:text-surface-0';
      default: return 'pl-2.5 pr-3 sm:pl-4 text-base sm:text-lg text-surface-900 placeholder:text-surface-300 dark:text-surface-0 dark:placeholder:text-surface-600';
    }
  });

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
        this.open(highlighted?.id ?? this.query(), highlighted?.type, highlighted?.app);
        break;
      }
      case 'Escape':
        this.input.nativeElement.blur();
        break;
    }
  }

  isOpen(aggregate: AggregateRef): boolean {
    const current = this.search.current();
    return !!current && current.app === aggregate.app && current.type === aggregate.type && current.id === aggregate.id;
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

  open(id: string, type = this.selectedType(), app = this.backend.activeApp()?.name ?? '') {
    this.search.open(app, type, id);
    this.input.nativeElement.blur();
  }
}
