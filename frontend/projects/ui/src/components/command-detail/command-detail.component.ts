import { Component, input, output, signal, computed, effect, untracked, inject, DestroyRef } from '@angular/core';
import { DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY, Subscription } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { TagModule } from 'primeng/tag';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { MessageService } from 'primeng/api';
import { CommandMessage, EventMessage } from '../../models';
import { EventifyService } from '../../services/eventify.service';
import { JsonHighlightPipe } from '../../pipes/json-highlight.pipe';
import { DetailSkeletonComponent } from '../skeletons/detail-skeleton.component';
import { afterMinLoading } from '../../loading-timing';
import { copyToClipboard } from '../../clipboard';
import { formatPayload, metadataEntries } from '../../payload';
import { EventDetailComponent } from '../event-detail/event-detail.component';
import { TimelineItemComponent } from '../timeline-item.component';
import { errorDetail } from '../../errors';

@Component({
  selector: 'app-command-detail',
  templateUrl: './command-detail.component.html',
  standalone: true,
  imports: [
    DatePipe,
    ButtonModule, DrawerModule, TagModule, TabsModule, TooltipModule,
    JsonHighlightPipe, EventDetailComponent, DetailSkeletonComponent, TimelineItemComponent,
  ],
})
export class CommandDetailComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  // Provided by the aggregate page, so retry and copy messages show in its toast.
  private readonly messageService = inject(MessageService);

  command = input<CommandMessage | null>(null);
  // Loading follows the id: a refreshed copy of the same command updates its fields without reloading or resetting the tabs.
  private readonly commandId = computed(() => this.command()?.id);
  // Derived once per command, not in the template: payloads can be large (see EventDetailComponent).
  readonly payloadJson = computed(() => { const c = this.command(); return c ? formatPayload(c.payload) : ''; });
  readonly metadata = computed(() => metadataEntries(this.command()?.metadata));

  producedEvents = signal<EventMessage[] | null>(null);
  loading = signal(false);
  /** Emits once the selected item has finished loading (successfully or not). */
  loaded = output<void>();
  retrying = signal(false);
  activeTab = signal('payload');
  copiedKey = signal<string | null>(null);

  // Produced event drawer
  drawerVisible = signal(false);
  drawerEvent = signal<EventMessage | null>(null);

  private request?: Subscription;

  constructor() {
    effect(() => {
      const id = this.commandId();
      const cmd = untracked(this.command);
      // A new selection cancels the previous request, so a slower earlier response can't show up for this one.
      this.request?.unsubscribe();
      this.producedEvents.set(null);
      this.activeTab.set('payload');
      this.drawerVisible.set(false);
      this.drawerEvent.set(null);
      if (!cmd) { this.loading.set(false); return; }
      const correlationId = cmd.metadata['$correlationId'];
      if (!correlationId) { this.producedEvents.set([]); this.finishLoading(); return; }
      const startedAt = Date.now();
      this.loading.set(true);
      this.request = this.svc.getEventsByCorrelation(cmd.aggregateId, correlationId).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(() => { this.finishLoading(); return EMPTY; }),
      ).subscribe(page => {
        this.producedEvents.set(page.events);
        afterMinLoading(startedAt, () => { if (this.commandId() === id) this.finishLoading(); });
      });
    });
  }

  openEvent(event: EventMessage) {
    this.drawerEvent.set(event);
    this.drawerVisible.set(true);
  }

  retry() {
    const cmd = this.command();
    if (!cmd || this.retrying()) return;
    // Like the skeletons: "Retrying…" stays at least MIN_LOADING_MS, so a fast response doesn't make the button flicker.
    const startedAt = Date.now();
    this.retrying.set(true);
    this.svc.retryCommand(cmd.aggregateId, cmd.id, cmd).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(err => {
        afterMinLoading(startedAt, () => {
          this.retrying.set(false);
          this.messageService.add({ severity: 'error', summary: 'Error', detail: errorDetail(err, 'Failed to retry command.') });
        });
        return EMPTY;
      }),
    ).subscribe(() => afterMinLoading(startedAt, () => {
      this.retrying.set(false);
      this.messageService.add({ severity: 'success', summary: 'Retried', detail: 'Command has been resubmitted.' });
    }));
  }

  result(): 'success' | 'failure' | null {
    const r = this.command()?.metadata['$result'];
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : null;
  }

  cause(): string | null {
    return this.command()?.metadata['$cause'] ?? null;
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
