import { Component, ElementRef, HostListener, ViewChild, computed, effect, inject, input, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { SearchService } from '../services/search.service';

@Component({
  selector: 'app-header-search',
  standalone: true,
  imports: [FormsModule],
  template: `
    <div class="relative">
      <i class="absolute top-1/2 -translate-y-1/2 pointer-events-none"
         [class]="(search.loading() ? 'pi pi-spin pi-spinner ' : 'pi pi-search ') + (dark() ? 'left-2.5 text-xs text-slate-400' : 'left-3.5 text-sm text-surface-400')"></i>
      <input #input type="text" spellcheck="false" autocomplete="off" placeholder="Search aggregate ID…"
             role="combobox" aria-autocomplete="list" [attr.aria-expanded]="dropdownOpen()" [attr.aria-controls]="listId()"
             [attr.aria-activedescendant]="highlightedIndex() >= 0 ? listId() + '-' + highlightedIndex() : null"
             class="w-full rounded border focus:outline-none transition-colors"
             [class]="dark()
               ? 'h-8 pl-8 pr-8 text-sm border-slate-700 bg-slate-800 text-slate-100 placeholder:text-slate-400 hover:border-slate-600 focus:border-slate-500 focus:bg-slate-700/60'
               : 'h-11 pl-10 pr-3 text-base rounded-md border-surface-200 bg-surface-50 text-surface-900 placeholder:text-surface-400 hover:border-surface-300 focus:border-primary-500 dark:border-surface-700 dark:bg-surface-800 dark:text-surface-100'"
             [(ngModel)]="query" (input)="highlightedIndex.set(-1)" (keydown)="onInputKeydown($event)"
             (focus)="focused.set(true); highlightedIndex.set(-1)" (blur)="focused.set(false)" />
      @if (!focused() && dark()) {
        <kbd class="absolute right-2 top-1/2 -translate-y-1/2 px-1.5 pb-[1.5px] rounded border border-slate-600 text-[10px] leading-4 font-sans text-slate-400 pointer-events-none">/</kbd>
      }

      @if (dropdownOpen()) {
        <div [id]="listId()" role="listbox" class="absolute top-full inset-x-0 mt-1 bg-surface-0 dark:bg-surface-900 border border-surface-200 dark:border-surface-700 rounded-lg shadow-lg z-50 overflow-hidden">
          <div class="px-3 py-2 text-xs font-medium text-surface-400 uppercase tracking-widest border-b border-surface-100 dark:border-surface-800">Recent</div>
          @for (id of search.recent(); track id) {
            <!-- The green border and text mark the open aggregate (like the active app in the app switcher); grey marks the keyboard/mouse position -->
            <div [id]="listId() + '-' + $index" role="option" [attr.aria-selected]="$index === highlightedIndex()"
                 class="flex items-center justify-between px-3 py-2 border-l-2 cursor-pointer transition-colors"
                 [class]="rowClass(id, $index)"
                 (mouseenter)="highlightedIndex.set($index)"
                 (mousedown)="$event.preventDefault(); open(id)">
              <div class="flex items-center gap-2 min-w-0">
                <i class="pi pi-history text-surface-400 text-xs shrink-0"></i>
                <span class="text-sm truncate"
                      [class]="id === search.currentId() ? 'text-primary-600 dark:text-primary-400' : 'text-surface-900 dark:text-surface-100'">{{ id }}</span>
              </div>
              <button class="text-surface-300 hover:text-surface-500 dark:text-surface-600 dark:hover:text-surface-400 ml-2 shrink-0 cursor-pointer"
                      (mousedown)="$event.preventDefault(); $event.stopPropagation(); remove(id)">
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

  constructor() {
    // Show the open aggregate in the box whenever the URL changes.
    effect(() => this.query.set(this.search.currentId()));
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
      case 'Enter':
        e.preventDefault();
        this.open(this.highlightedIndex() >= 0 ? recent[this.highlightedIndex()] : this.query());
        break;
      case 'Escape':
        this.input.nativeElement.blur();
        break;
    }
  }

  rowClass(id: string, index: number): string {
    const open = id === this.search.currentId();
    const highlighted = index === this.highlightedIndex();
    return (open ? 'border-primary-500 ' : 'border-transparent ')
      + (highlighted ? 'bg-surface-100 dark:bg-surface-800' : open ? 'bg-primary-50 dark:bg-primary-950' : '');
  }

  remove(id: string) {
    this.search.removeRecent(id);
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

  open(id: string) {
    this.search.open(id);
    this.input.nativeElement.blur();
  }
}
