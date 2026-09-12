import { Component, inject, signal, computed, HostListener, DestroyRef, ElementRef, ViewChild, NgZone } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
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
import { CommandMessage, EventMessage } from '../models';
import { EventDetailComponent } from './event-detail/event-detail.component';
import { CommandDetailComponent } from './command-detail/command-detail.component';

const RECENT_KEY = 'eventify.recentSearches';
const MAX_RECENT = 8;
const MIN_SKELETON_MS = 300;

@Component({
  selector: 'app-aggregate',
  templateUrl: './aggregate.component.html',
  standalone: true,
  imports: [
    CommonModule, FormsModule, DatePipe,
    InputTextModule, ButtonModule, DrawerModule,
    SkeletonModule, TagModule, ToastModule, TabsModule, TooltipModule,
    EventDetailComponent, CommandDetailComponent,
  ],
  providers: [MessageService],
})
export class AggregateComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly messageService = inject(MessageService);
  private readonly zone = inject(NgZone);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);

  @ViewChild('searchInput') searchInput!: ElementRef<HTMLInputElement>;

  readonly skeletonRows = Array(8);

  aggregateId = signal('');
  recentSearches = signal<string[]>(this.loadRecent());
  showRecent = signal(false);
  mainTab = signal<'events' | 'commands'>('events');

  // Events
  events = signal<EventMessage[]>([]);
  nextCursor = signal<string | null>(null);
  loadingEvents = signal(false);
  loadingMoreEvents = signal(false);

  // Commands
  commands = signal<CommandMessage[]>([]);
  loadingCommands = signal(false);

  selected = signal<EventMessage | CommandMessage | null>(null);

  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  hasEventResults = computed(() => this.events().length > 0);
  hasCommandResults = computed(() => this.commands().length > 0);
  selectedEvent = computed(() => this.isEvent(this.selected()) ? this.selected() as EventMessage : null);
  selectedCommand = computed(() => this.isCommand(this.selected()) ? this.selected() as CommandMessage : null);

  isEvent(item: EventMessage | CommandMessage | null): item is EventMessage {
    return item != null && 'revision' in item;
  }

  isCommand(item: EventMessage | CommandMessage | null): item is CommandMessage {
    return item != null && !('revision' in item);
  }

  private minElapsed: Record<string, boolean> = {};
  private dataReady: Record<string, boolean> = {};

  constructor() {
    this.route.queryParams.pipe(
      map(p => (p['id'] ?? '').trim()),
      distinctUntilChanged(),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(id => {
      this.aggregateId.set(id);
      this.resetAll();
      if (id) {
        this.loadEvents(id, null, false);
        this.loadCommands(id);
      }
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
    if (e.key === 'Escape') {
      this.selected.set(null);
      this.drawerVisible.set(false);
      return;
    }
    if (this.mainTab() === 'events') {
      const list = this.events();
      if (!list.length) return;
      const cur = this.selected();
      const idx = cur && this.isEvent(cur) ? list.findIndex(ev => ev.id === cur.id) : -1;
      if (e.key === 'ArrowDown') { e.preventDefault(); this.selectItem(list[Math.min(idx + 1, list.length - 1)]); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); this.selectItem(list[Math.max(idx - 1, 0)]); }
    } else {
      const list = this.commands();
      if (!list.length) return;
      const cur = this.selected();
      const idx = cur && this.isCommand(cur) ? list.findIndex(c => c.id === cur.id) : -1;
      if (e.key === 'ArrowDown') { e.preventDefault(); this.selectItem(list[Math.min(idx + 1, list.length - 1)]); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); this.selectItem(list[Math.max(idx - 1, 0)]); }
    }
  }

  search() {
    const id = this.aggregateId().trim();
    if (!id) return;
    this.showRecent.set(false);
    this.saveRecent(id);
    const currentId = (this.route.snapshot.queryParams['id'] ?? '').trim();
    if (id === currentId) {
      this.resetAll();
      this.loadEvents(id, null, false);
      this.loadCommands(id);
    } else {
      this.router.navigate([], { queryParams: { id }, replaceUrl: true });
    }
  }

  selectItem(item: EventMessage | CommandMessage) {
    this.selected.set(item);
    if (this.isMobile()) this.drawerVisible.set(true);
  }

  onListScroll(el: HTMLDivElement) {
    if (!this.nextCursor() || this.loadingMoreEvents()) return;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 100)
      this.loadEvents(this.aggregateId(), this.nextCursor(), true);
  }

  onSearchFocus() { if (this.recentSearches().length > 0) this.showRecent.set(true); }

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

  result(command: CommandMessage): 'success' | 'failure' | null {
    const r = command.metadata['$result'];
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : null;
  }

  isRetry(command: CommandMessage): boolean {
    return command.metadata['$retry'] === 'true';
  }

  private resetAll() {
    this.events.set([]); this.nextCursor.set(null);
    this.commands.set([]);
    this.selected.set(null);
    this.drawerVisible.set(false);
  }

  private loadEvents(id: string, cursor: string | null, append: boolean) {
    const key = append ? 'loadingMoreEvents' : 'loadingEvents';
    this.setLoading(key, true);
    this.svc.getEvents(id, cursor).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(err => {
        this.setLoading(key, false);
        this.messageService.add({ severity: 'error', summary: 'Error', detail: err.status === 404 ? 'Aggregate not found.' : 'Failed to load events.' });
        return EMPTY;
      }),
    ).subscribe(page => {
      this.events.update(prev => append ? [...prev, ...page.events] : page.events);
      this.nextCursor.set(page.nextCursor);
      this.setLoading(key, false);
    });
  }

  private loadCommands(id: string) {
    this.setLoading('loadingCommands', true);
    this.svc.getCommands(id).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(() => {
        this.setLoading('loadingCommands', false);
        this.messageService.add({ severity: 'error', summary: 'Error', detail: 'Failed to load commands.' });
        return EMPTY;
      }),
    ).subscribe(page => {
      this.commands.set(page.commands);
      this.setLoading('loadingCommands', false);
    });
  }

  private setLoading(key: string, value: boolean) {
    const sig = this[key as keyof this] as ReturnType<typeof signal<boolean>>;
    if (value) {
      this.minElapsed[key] = false;
      this.dataReady[key] = false;
      sig.set(true);
      setTimeout(() => {
        this.minElapsed[key] = true;
        if (this.dataReady[key]) sig.set(false);
      }, MIN_SKELETON_MS);
    } else {
      this.dataReady[key] = true;
      if (this.minElapsed[key]) sig.set(false);
    }
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
}
