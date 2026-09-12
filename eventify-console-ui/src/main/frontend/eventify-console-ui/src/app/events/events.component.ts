import { Component, inject, signal, computed, HostListener, DestroyRef, ElementRef, ViewChild, NgZone } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule, DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { catchError, distinctUntilChanged, EMPTY, map } from 'rxjs';

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
import { EventDetail, EventMessage } from '../models';
import { JsonHighlightPipe } from '../shared/json-highlight.pipe';
import { JsonDiffPipe } from '../shared/json-diff.pipe';

const RECENT_KEY = 'eventify.recentSearches';
const MAX_RECENT = 8;

@Component({
  selector: 'app-events',
  templateUrl: './events.component.html',
  standalone: true,
  imports: [
    CommonModule, FormsModule, DatePipe,
    InputTextModule, ButtonModule, DrawerModule,
    SkeletonModule, TagModule, ToastModule, TabsModule, TooltipModule,
    JsonHighlightPipe, JsonDiffPipe,
  ],
  providers: [MessageService],
})
export class EventsComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly messageService = inject(MessageService);
  private readonly zone = inject(NgZone);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);

  @ViewChild('searchInput') searchInput!: ElementRef<HTMLInputElement>;

  readonly skeletonRows = Array(8);

  aggregateId = signal('');
  events = signal<EventMessage[]>([]);
  nextCursor = signal<string | null>(null);
  loading = signal(false);
  loadingMore = signal(false);
  selectedEvent = signal<EventMessage | null>(null);
  eventDetail = signal<EventDetail | null>(null);
  loadingDetail = signal(false);
  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  activeTab = signal('event');
  recentSearches = signal<string[]>(this.loadRecent());
  showRecent = signal(false);
  showDiff = signal(false);
  copiedKey = signal<string | null>(null);
  hasResults = computed(() => this.events().length > 0);

  private readonly MIN_SKELETON_MS = 300;
  private minElapsed = { loading: false, loadingMore: false, loadingDetail: false };
  private dataReady = { loading: false, loadingMore: false, loadingDetail: false };

  constructor() {
    // When aggregate ID changes → reload list
    this.route.queryParams.pipe(
      map(p => (p['id'] ?? '').trim()),
      distinctUntilChanged(),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(id => {
      this.aggregateId.set(id);
      this.events.set([]);
      this.nextCursor.set(null);
      this.selectedEvent.set(null);
      this.eventDetail.set(null);
      this.drawerVisible.set(false);
      if (id) this.loadPage(id, null, false);
    });

    // When eventId changes → load detail
    this.route.queryParams.pipe(
      map(p => ({ id: (p['id'] ?? '').trim(), eventId: p['eventId'] ?? null })),
      distinctUntilChanged((a, b) => a.eventId === b.eventId),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(({ id, eventId }) => {
      if (!eventId) {
        this.selectedEvent.set(null);
        this.eventDetail.set(null);
        this.drawerVisible.set(false);
        return;
      }
      if (this.isMobile()) this.drawerVisible.set(true);
      this.setLoadingDetail(true);
      this.svc.getEventDetail(id, eventId).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(() => { this.setLoadingDetail(false); return EMPTY; }),
      ).subscribe(detail => {
        this.selectedEvent.set(detail.event);
        this.eventDetail.set(detail);
        this.activeTab.set('event');
        this.showDiff.set(false);
        this.setLoadingDetail(false);
      });
    });
  }

  @HostListener('window:resize')
  onResize() { this.isMobile.set(window.innerWidth < 1024); }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA') {
      e.preventDefault();
      this.searchInput?.nativeElement.focus();
      return;
    }
    const list = this.events();
    if (!list.length) return;
    const current = this.selectedEvent();
    const idx = current ? list.findIndex(ev => ev.id === current.id) : -1;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      this.selectEvent(list[Math.min(idx + 1, list.length - 1)]);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      this.selectEvent(list[Math.max(idx - 1, 0)]);
    } else if (e.key === 'Escape') {
      this.router.navigate([], { queryParams: { id: this.aggregateId() }, replaceUrl: true });
    }
  }

  search() {
    const id = this.aggregateId().trim();
    if (!id) return;
    this.showRecent.set(false);
    this.saveRecent(id);
    this.router.navigate([], { queryParams: { id }, replaceUrl: true });
  }

  selectEvent(event: EventMessage) {
    this.router.navigate([], { queryParams: { id: this.aggregateId(), eventId: event.id }, replaceUrl: true });
  }

  loadMore() {
    const id = this.aggregateId().trim();
    if (!id || !this.nextCursor()) return;
    this.loadPage(id, this.nextCursor(), true);
  }

  onListScroll(el: HTMLDivElement) {
    if (!this.nextCursor() || this.loadingMore()) return;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 100) this.loadMore();
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
    this.showRecent.set(false);
    this.saveRecent(id);
    this.router.navigate([], { queryParams: { id }, replaceUrl: true });
  }

  removeRecent(id: string, e: MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    const updated = this.recentSearches().filter(r => r !== id);
    this.recentSearches.set(updated);
    localStorage.setItem(RECENT_KEY, JSON.stringify(updated));
    if (updated.length === 0) this.showRecent.set(false);
  }

  copy(text: string, key: string) {
    navigator.clipboard.writeText(text).then(() => {
      this.copiedKey.set(key);
      this.zone.runOutsideAngular(() =>
        setTimeout(() => this.zone.run(() => this.copiedKey.set(null)), 1500)
      );
    });
  }

  allMetadataEntries(metadata: Record<string, string>): { key: string; value: string }[] {
    return Object.entries(metadata ?? {}).map(([key, value]) => ({ key, value }));
  }

  cleanPayload(obj: Record<string, unknown>): Record<string, unknown> {
    const cleaned = { ...obj };
    delete cleaned['@class'];
    return cleaned;
  }

  formatJson(obj: unknown): string {
    return JSON.stringify(this.cleanPayload(obj as Record<string, unknown>), null, 2);
  }

  private loadPage(id: string, cursor: string | null, append: boolean) {
    if (append) this.setLoadingMore(true);
    else this.setLoading(true);

    this.svc.getEvents(id, cursor).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(err => {
        if (append) this.setLoadingMore(false); else this.setLoading(false);
        const msg = err.status === 404 ? 'Aggregate not found.' : 'Failed to load events.';
        this.messageService.add({ severity: 'error', summary: 'Error', detail: msg });
        return EMPTY;
      }),
    ).subscribe(page => {
      this.events.update(prev => append ? [...prev, ...page.events] : page.events);
      this.nextCursor.set(page.nextCursor);
      if (append) this.setLoadingMore(false); else this.setLoading(false);
    });
  }

  private saveRecent(id: string) {
    const updated = [id, ...this.recentSearches().filter(r => r !== id)].slice(0, MAX_RECENT);
    this.recentSearches.set(updated);
    localStorage.setItem(RECENT_KEY, JSON.stringify(updated));
  }

  private loadRecent(): string[] {
    try { return JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]'); }
    catch { return []; }
  }

  private setLoadingState(key: 'loading' | 'loadingMore' | 'loadingDetail', value: boolean) {
    if (value) {
      this.minElapsed[key] = false;
      this.dataReady[key] = false;
      this[key].set(true);
      setTimeout(() => {
        this.minElapsed[key] = true;
        if (this.dataReady[key]) this[key].set(false);
      }, this.MIN_SKELETON_MS);
    } else {
      this.dataReady[key] = true;
      if (this.minElapsed[key]) this[key].set(false);
    }
  }

  private setLoading(v: boolean) { this.setLoadingState('loading', v); }
  private setLoadingMore(v: boolean) { this.setLoadingState('loadingMore', v); }
  private setLoadingDetail(v: boolean) { this.setLoadingState('loadingDetail', v); }
}
