import { Component, inject, signal, computed, HostListener, DestroyRef, ElementRef, ViewChild, NgZone } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule, NgTemplateOutlet, DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';

import { InputTextModule } from 'primeng/inputtext';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { SkeletonModule } from 'primeng/skeleton';
import { TagModule } from 'primeng/tag';
import { ToastModule } from 'primeng/toast';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { MessageService } from 'primeng/api';

import { EventifyService } from '../eventify.service';
import { AggregateState, EventMessage } from '../models';
import { JsonHighlightPipe } from '../shared/json-highlight.pipe';

const RECENT_KEY = 'eventify.recentSearches';
const MAX_RECENT = 8;

@Component({
  selector: 'app-events',
  templateUrl: './events.component.html',
  standalone: true,
  imports: [
    CommonModule, NgTemplateOutlet, FormsModule, DatePipe,
    InputTextModule, ButtonModule, DrawerModule,
    SkeletonModule, TagModule, ToastModule, TabsModule, TooltipModule,
    JsonHighlightPipe,
  ],
  providers: [MessageService],
})
export class EventsComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly messageService = inject(MessageService);
  private readonly zone = inject(NgZone);

  copiedKey = signal<string | null>(null);

  copy(text: string, key: string) {
    navigator.clipboard.writeText(text).then(() => {
      this.copiedKey.set(key);
      this.zone.runOutsideAngular(() =>
        setTimeout(() => this.zone.run(() => this.copiedKey.set(null)), 1500)
      );
    });
  }

  @ViewChild('searchInput') searchInput!: ElementRef<HTMLInputElement>;
  @ViewChild('listContainer') listContainer!: ElementRef<HTMLDivElement>;

  readonly skeletonRows = Array(8);

  aggregateId = signal('');
  events = signal<EventMessage[]>([]);
  nextCursor = signal<string | null>(null);
  loading = signal(false);
  loadingMore = signal(false);
  selectedEvent = signal<EventMessage | null>(null);
  aggregateState = signal<AggregateState | null>(null);
  loadingState = signal(false);
  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  activeTab = signal('event');
  recentSearches = signal<string[]>(this.loadRecent());
  showRecent = signal(false);

  hasResults = computed(() => this.events().length > 0);

  @HostListener('window:resize')
  onResize() {
    this.isMobile.set(window.innerWidth < 1024);
  }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA') {
      e.preventDefault();
      this.searchInput?.nativeElement.focus();
    }
  }

  onSearchFocus() {
    if (this.recentSearches().length > 0) this.showRecent.set(true);
  }

  onSearchBlur() {
    this.zone.runOutsideAngular(() =>
      setTimeout(() => this.zone.run(() => this.showRecent.set(false)), 150)
    );
  }

  selectRecent(id: string) {
    this.aggregateId.set(id);
    this.showRecent.set(false);
    this.doSearch(id);
  }

  removeRecent(id: string, e: MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    const updated = this.recentSearches().filter(r => r !== id);
    this.recentSearches.set(updated);
    localStorage.setItem(RECENT_KEY, JSON.stringify(updated));
    if (updated.length === 0) this.showRecent.set(false);
  }

  onListScroll(el: HTMLDivElement) {
    if (!this.nextCursor() || this.loadingMore()) return;
    const threshold = 100;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - threshold) {
      this.loadMore();
    }
  }

  eventTypeName(event: EventMessage): string {
    const t = (event.payload?.['@type'] ?? event.payload?.['@class']) as string | undefined;
    if (!t) return event.type ?? 'Unknown';
    const parts = t.split(/[.$]/);
    return parts[parts.length - 1];
  }

  allMetadataEntries(metadata: Record<string, string>): { key: string; value: string }[] {
    return Object.entries(metadata ?? {}).map(([key, value]) => ({ key, value }));
  }

  search() {
    const id = this.aggregateId().trim();
    if (!id) return;
    this.showRecent.set(false);
    this.doSearch(id);
  }

  loadMore() {
    const id = this.aggregateId().trim();
    if (!id || !this.nextCursor()) return;
    this.loadPage(id, this.nextCursor(), true);
  }

  selectEvent(event: EventMessage) {
    this.selectedEvent.set(event);
    this.aggregateState.set(null);
    this.activeTab.set('event');
    if (this.isMobile()) this.drawerVisible.set(true);
    this.loadingState.set(true);
    this.svc.getState(this.aggregateId().trim(), event.id)
      .pipe(takeUntilDestroyed(this.destroyRef), catchError(() => {
        this.loadingState.set(false);
        return EMPTY;
      }))
      .subscribe(state => {
        this.aggregateState.set(state);
        this.loadingState.set(false);
      });
  }

  formatJson(obj: unknown): string {
    const cleaned = { ...obj as Record<string, unknown> };
    delete cleaned['@class'];
    delete cleaned['@type'];
    return JSON.stringify(cleaned, null, 2);
  }

  private doSearch(id: string) {
    this.events.set([]);
    this.nextCursor.set(null);
    this.selectedEvent.set(null);
    this.aggregateState.set(null);
    this.drawerVisible.set(false);
    this.loadPage(id, null, false);
  }

  private saveRecent(id: string) {
    const current = this.recentSearches().filter(r => r !== id);
    const updated = [id, ...current].slice(0, MAX_RECENT);
    this.recentSearches.set(updated);
    localStorage.setItem(RECENT_KEY, JSON.stringify(updated));
  }

  private loadRecent(): string[] {
    try {
      return JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]');
    } catch {
      return [];
    }
  }

  private loadPage(id: string, cursor: string | null, append: boolean) {
    if (append) this.loadingMore.set(true);
    else this.loading.set(true);

    this.svc.getEvents(id, cursor)
      .pipe(takeUntilDestroyed(this.destroyRef), catchError(err => {
        this.loading.set(false);
        this.loadingMore.set(false);
        const msg = err.status === 404 ? 'Aggregate not found.' : 'Failed to load events.';
        this.messageService.add({ severity: 'error', summary: 'Error', detail: msg });
        return EMPTY;
      }))
      .subscribe(page => {
        if (!append) this.saveRecent(id);
        this.events.update(prev => append ? [...prev, ...page.events] : page.events);
        this.nextCursor.set(page.nextCursor);
        this.loading.set(false);
        this.loadingMore.set(false);
      });
  }
}
