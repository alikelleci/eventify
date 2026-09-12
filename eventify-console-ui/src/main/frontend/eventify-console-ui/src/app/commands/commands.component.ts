import { Component, inject, signal, computed, HostListener, DestroyRef, NgZone, ElementRef, ViewChild } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, distinctUntilChanged, EMPTY, map } from 'rxjs';
import { ActivatedRoute, Router } from '@angular/router';

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
import { JsonHighlightPipe } from '../shared/json-highlight.pipe';

const RECENT_KEY = 'eventify.recentCommandSearches';
const MAX_RECENT = 8;

@Component({
  selector: 'app-commands',
  templateUrl: './commands.component.html',
  standalone: true,
  imports: [CommonModule, FormsModule, DatePipe, InputTextModule, ButtonModule, DrawerModule, SkeletonModule, TagModule, ToastModule, TabsModule, TooltipModule, JsonHighlightPipe],
  providers: [MessageService],
})
export class CommandsComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly messageService = inject(MessageService);
  private readonly zone = inject(NgZone);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);

  @ViewChild('searchInput') searchInput!: ElementRef<HTMLInputElement>;

  readonly skeletonRows = Array(8);

  aggregateId = signal('');
  commands = signal<CommandMessage[]>([]);
  loading = signal(false);
  selectedCommand = signal<CommandMessage | null>(null);
  producedEvents = signal<EventMessage[] | null>(null);
  loadingDetail = signal(false);
  activeTab = signal('command');
  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  copiedKey = signal<string | null>(null);
  hasResults = computed(() => this.commands().length > 0);
  recentSearches = signal<string[]>(this.loadRecent());
  showRecent = signal(false);

  private readonly MIN_SKELETON_MS = 300;
  private minElapsed = false;
  private dataReady = false;
  private minElapsedDetail = false;
  private dataReadyDetail = false;

  constructor() {
    // When aggregate ID changes → reload list
    this.route.queryParams.pipe(
      map(p => (p['id'] ?? '').trim()),
      distinctUntilChanged(),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(id => {
      this.aggregateId.set(id);
      this.commands.set([]);
      this.selectedCommand.set(null);
      this.producedEvents.set(null);
      this.loadingDetail.set(false);
      this.drawerVisible.set(false);
      if (id) this.load(id);
    });

    // When commandId changes → select from list (commands are already in memory)
    this.route.queryParams.pipe(
      map(p => p['commandId'] ?? null),
      distinctUntilChanged(),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(commandId => {
      if (!commandId) {
        this.selectedCommand.set(null);
        this.producedEvents.set(null);
        this.loadingDetail.set(false);
        this.drawerVisible.set(false);
        return;
      }
      const found = this.commands().find(c => c.id === commandId) ?? null;
      this.selectedCommand.set(found);
      this.producedEvents.set(null);
      this.activeTab.set('command');
      if (found) {
        if (this.isMobile()) this.drawerVisible.set(true);
        this.loadDetail(found);
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
    const list = this.commands();
    if (!list.length) return;
    const current = this.selectedCommand();
    const idx = current ? list.findIndex(c => c.id === current.id) : -1;
    if (e.key === 'ArrowDown') { e.preventDefault(); this.select(list[Math.min(idx + 1, list.length - 1)]); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); this.select(list[Math.max(idx - 1, 0)]); }
    else if (e.key === 'Escape') { this.router.navigate([], { queryParams: { id: this.aggregateId() }, replaceUrl: true }); }
  }

  search() {
    const id = this.aggregateId().trim();
    if (!id) return;
    this.showRecent.set(false);
    this.saveRecent(id);
    this.router.navigate([], { queryParams: { id }, replaceUrl: true });
  }

  select(command: CommandMessage) {
    this.router.navigate([], { queryParams: { id: this.aggregateId(), commandId: command.id }, replaceUrl: true });
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

  result(command: CommandMessage): 'success' | 'failure' | null {
    const r = command.metadata['$result'];
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : null;
  }

  cause(command: CommandMessage): string | null {
    return command.metadata['$cause'] ?? null;
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

  goToEvent(event: EventMessage) {
    this.router.navigate(['events'], { queryParams: { id: event.aggregateId, eventId: event.id } });
  }

  private load(id: string) {
    this.minElapsed = false;
    this.dataReady = false;
    this.loading.set(true);
    setTimeout(() => {
      this.minElapsed = true;
      if (this.dataReady) this.loading.set(false);
    }, this.MIN_SKELETON_MS);

    this.svc.getCommands(id).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(() => {
        this.dataReady = true;
        if (this.minElapsed) this.loading.set(false);
        this.messageService.add({ severity: 'error', summary: 'Error', detail: 'Failed to load commands.' });
        return EMPTY;
      }),
    ).subscribe(page => {
      this.commands.set(page.commands);
      // If commandId is already in URL (permalink), select it now that list is loaded
      const commandId = this.route.snapshot.queryParamMap.get('commandId');
      if (commandId) {
        const found = page.commands.find(c => c.id === commandId) ?? null;
        this.selectedCommand.set(found);
        this.producedEvents.set(null);
        this.activeTab.set('command');
        if (found) {
          if (this.isMobile()) this.drawerVisible.set(true);
          this.loadDetail(found);
        }
      }
      this.dataReady = true;
      if (this.minElapsed) this.loading.set(false);
    });
  }

  private loadDetail(command: CommandMessage) {
    this.setLoadingDetail(true);
    const correlationId = command.metadata['$correlationId'];
    if (!correlationId) {
      this.producedEvents.set([]);
      this.setLoadingDetail(false);
      return;
    }
    this.svc.getEventsByCorrelation(command.aggregateId, correlationId).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(() => { this.setLoadingDetail(false); return EMPTY; }),
    ).subscribe(page => {
      this.producedEvents.set(page.events);
      this.setLoadingDetail(false);
    });
  }

  private setLoadingDetail(value: boolean) {
    if (value) {
      this.minElapsedDetail = false;
      this.dataReadyDetail = false;
      this.loadingDetail.set(true);
      setTimeout(() => {
        this.minElapsedDetail = true;
        if (this.dataReadyDetail) this.loadingDetail.set(false);
      }, this.MIN_SKELETON_MS);
    } else {
      this.dataReadyDetail = true;
      if (this.minElapsedDetail) this.loadingDetail.set(false);
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
