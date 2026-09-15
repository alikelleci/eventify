import { Component, input, output, signal, computed, effect, untracked, inject, DestroyRef } from '@angular/core';
import { DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY, Subscription } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { MessageService } from 'primeng/api';
import { EventDetail, EventMessage } from '../../models';
import { EventifyService } from '../../services/eventify.service';
import { JsonHighlightPipe } from '../../pipes/json-highlight.pipe';
import { DetailSkeletonComponent } from '../skeletons/detail-skeleton.component';
import { afterMinLoading } from '../../loading-timing';
import { copyToClipboard } from '../../clipboard';
import { errorDetail } from '../../errors';
import { cleanPayload, formatPayload, metadataEntries } from '../../payload';
import { JsonDiffPipe } from '../../pipes/json-diff.pipe';

@Component({
  selector: 'app-event-detail',
  templateUrl: './event-detail.component.html',
  standalone: true,
  imports: [
    DatePipe,
    ButtonModule, TagModule, TabsModule, TooltipModule,
    JsonHighlightPipe, JsonDiffPipe, DetailSkeletonComponent,
  ],
})
export class EventDetailComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  // Provided by the aggregate page.
  private readonly messageService = inject(MessageService);

  event = input<EventMessage | null>(null);
  // Loading follows the id: a refreshed copy of the same event doesn't reload it or reset the tabs.
  private readonly eventId = computed(() => this.event()?.id);

  detail = signal<EventDetail | null>(null);
  loading = signal(false);
  /** Emits once the selected item has finished loading (successfully or not). */
  loaded = output<void>();
  activeTab = signal('payload');
  showDiff = signal(false);
  copiedKey = signal<string | null>(null);

  // Derived once per loaded event, not in the template: payloads can be large, and a template expression runs on every
  // change detection, which would format, highlight and diff them again for unrelated updates elsewhere on the page.
  readonly payloadJson = computed(() => { const d = this.detail(); return d ? formatPayload(d.event.payload) : ''; });
  readonly metadata = computed(() => metadataEntries(this.detail()?.event.metadata));
  readonly stateJson = computed(() => { const s = this.detail()?.state; return s ? formatPayload(s.payload) : ''; });
  // The two sides of the diff. Without a state on one side it compares with an empty object.
  readonly diffCurrent = computed(() => cleanPayload(this.detail()?.state?.payload ?? {}));
  readonly diffPrevious = computed(() => cleanPayload(this.detail()?.previousState?.payload ?? {}));

  private request?: Subscription;

  constructor() {
    effect(() => {
      const id = this.eventId();
      const ev = untracked(this.event);
      // A new selection cancels the previous request, so a slower earlier response can't show up for this one.
      this.request?.unsubscribe();
      this.detail.set(null);
      this.activeTab.set('payload');
      this.showDiff.set(false);
      if (!ev) { this.loading.set(false); return; }
      const startedAt = Date.now();
      this.loading.set(true);
      this.request = this.svc.getEventDetail(ev.aggregateId, ev.id).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(err => {
          this.messageService.add({ severity: 'error', summary: 'Error', detail: errorDetail(err, 'Failed to load the event details.') });
          this.finishLoading();
          return EMPTY;
        }),
      ).subscribe(detail => {
        this.detail.set(detail);
        afterMinLoading(startedAt, () => { if (this.eventId() === id) this.finishLoading(); });
      });
    });
  }

  copy(text: string, key: string) {
    copyToClipboard(text).then(copied => {
      if (copied) {
        this.copiedKey.set(key);
        setTimeout(() => this.copiedKey.set(null), 1500);
      } else {
        this.messageService.add({ severity: 'warn', summary: 'Copy failed', detail: 'Your browser blocked copying. Select the text and copy it manually.' });
      }
    });
  }

  private finishLoading() {
    this.loading.set(false);
    this.loaded.emit();
  }
}
