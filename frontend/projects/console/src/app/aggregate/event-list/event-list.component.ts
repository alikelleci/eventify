import { Component, DestroyRef, ElementRef, HostListener, OnInit, inject, input, output, signal } from '@angular/core';
import { DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';
import { TagModule } from 'primeng/tag';
import { MessageService } from 'primeng/api';
import { EventifyService } from '@eventify/ui/services/eventify.service';
import { CommandMessage, EventMessage } from '@eventify/ui/models';
import { ListSkeletonComponent } from '@eventify/ui/components/skeletons/list-skeleton.component';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';
import { afterMinLoading } from '@eventify/ui/loading-timing';

/** Loads and shows an aggregate's events, newest first, with more loaded on scroll. */
@Component({
  selector: 'app-event-list',
  templateUrl: './event-list.component.html',
  standalone: true,
  imports: [DatePipe, TagModule, ListSkeletonComponent, TimelineItemComponent],
  host: { class: 'block overflow-y-auto', '(scroll)': 'onScroll()' },
})
export class EventListComponent implements OnInit {
  private readonly svc = inject(EventifyService);
  private readonly messageService = inject(MessageService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly host: ElementRef<HTMLElement> = inject(ElementRef);

  aggregateId = input.required<string>();
  /** Whether this list's tab is open; only then does it respond to the arrow keys. */
  active = input(false);
  selected = input<EventMessage | CommandMessage | null>(null);

  select = output<EventMessage>();
  /** Emits the first page as soon as it arrives, so the page can auto-select before the skeleton lifts. */
  loaded = output<EventMessage[]>();

  events = signal<EventMessage[]>([]);
  nextCursor = signal<string | null>(null);
  loading = signal(true);
  loadingMore = signal(false);

  ngOnInit() {
    this.load(null);
  }

  onScroll() {
    const el = this.host.nativeElement;
    if (!this.nextCursor() || this.loadingMore()) return;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 100) this.load(this.nextCursor());
  }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    const list = this.events();
    if (!this.active() || tag === 'INPUT' || tag === 'TEXTAREA' || !list.length) return;
    const idx = list.indexOf(this.selected() as EventMessage);
    if (e.key === 'ArrowDown') { e.preventDefault(); this.select.emit(list[Math.min(idx + 1, list.length - 1)]); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); this.select.emit(list[Math.max(idx - 1, 0)]); }
  }

  private load(cursor: string | null) {
    const firstPage = cursor === null;
    const flag = firstPage ? this.loading : this.loadingMore;
    const startedAt = Date.now();
    flag.set(true);
    this.svc.getEvents(this.aggregateId(), cursor).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(err => {
        this.messageService.add({ severity: 'error', summary: 'Error', detail: err.status === 404 ? 'Aggregate not found.' : 'Failed to load events.' });
        if (firstPage) this.loaded.emit([]);
        flag.set(false);
        return EMPTY;
      }),
    ).subscribe(page => {
      this.events.update(prev => firstPage ? page.events : [...prev, ...page.events]);
      this.nextCursor.set(page.nextCursor);
      if (firstPage) this.loaded.emit(page.events);
      afterMinLoading(startedAt, () => flag.set(false));
    });
  }
}
