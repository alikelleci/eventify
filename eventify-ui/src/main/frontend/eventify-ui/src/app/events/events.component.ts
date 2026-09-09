import { Component, inject, signal, computed, HostListener, DestroyRef, ElementRef, ViewChild } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule, NgTemplateOutlet, DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';

import { InputTextModule } from 'primeng/inputtext';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { ProgressSpinnerModule } from 'primeng/progressspinner';
import { TagModule } from 'primeng/tag';
import { ToastModule } from 'primeng/toast';
import { MessageService } from 'primeng/api';

import { EventifyService } from '../eventify.service';
import { AggregateState, EventMessage } from '../models';

@Component({
  selector: 'app-events',
  templateUrl: './events.component.html',
  standalone: true,
  imports: [
    CommonModule, NgTemplateOutlet, FormsModule, DatePipe,
    InputTextModule, ButtonModule, DrawerModule,
    ProgressSpinnerModule, TagModule, ToastModule,
  ],
  providers: [MessageService],
})
export class EventsComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly messageService = inject(MessageService);

  @ViewChild('drawerContainer', { read: ElementRef }) drawerContainer?: ElementRef;

  aggregateId = signal('');
  events = signal<EventMessage[]>([]);
  nextCursor = signal<string | null>(null);
  loading = signal(false);
  loadingMore = signal(false);
  selectedEvent = signal<EventMessage | null>(null);
  selectedState = signal<AggregateState | null>(null);
  loadingState = signal(false);
  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);

  hasResults = computed(() => this.events().length > 0);

  @HostListener('window:resize')
  onResize() {
    this.isMobile.set(window.innerWidth < 1024);
  }

  eventTypeName(event: EventMessage): string {
    const t = (event.payload?.['@type'] ?? event.payload?.['@class']) as string | undefined;
    if (!t) return 'Unknown';
    const parts = t.split(/[.$]/);
    return parts[parts.length - 1];
  }

  search() {
    const id = this.aggregateId().trim();
    if (!id) return;
    this.events.set([]);
    this.nextCursor.set(null);
    this.selectedEvent.set(null);
    this.selectedState.set(null);
    this.drawerVisible.set(false);
    this.loadPage(id, null, false);
  }

  loadMore() {
    const id = this.aggregateId().trim();
    if (!id || !this.nextCursor()) return;
    this.loadPage(id, this.nextCursor(), true);
  }

  selectEvent(event: EventMessage) {
    this.selectedEvent.set(event);
    this.selectedState.set(null);
    this.drawerVisible.set(true);
    this.loadingState.set(true);
    this.svc.getState(this.aggregateId().trim(), event.id)
      .pipe(takeUntilDestroyed(this.destroyRef), catchError(() => {
        this.loadingState.set(false);
        return EMPTY;
      }))
      .subscribe(state => {
        this.selectedState.set(state);
        this.loadingState.set(false);
      });
  }

  formatJson(obj: unknown): string {
    return JSON.stringify(obj, null, 2);
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
        this.events.update(prev => append ? [...prev, ...page.events] : page.events);
        this.nextCursor.set(page.nextCursor);
        this.loading.set(false);
        this.loadingMore.set(false);
      });
  }
}
