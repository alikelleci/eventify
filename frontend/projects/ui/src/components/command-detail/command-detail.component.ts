import { Component, input, output, signal, computed, effect, untracked, inject, DestroyRef } from '@angular/core';
import { DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY, Subscription } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { TagModule } from 'primeng/tag';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { ConfirmationService, MessageService } from 'primeng/api';
import { ConfirmDialogModule } from 'primeng/confirmdialog';
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
    ButtonModule, ConfirmDialogModule, DrawerModule, TagModule, TabsModule, TooltipModule,
    JsonHighlightPipe, EventDetailComponent, DetailSkeletonComponent, TimelineItemComponent,
  ],
  providers: [ConfirmationService],
})
export class CommandDetailComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  // Provided by the aggregate page, so retry and copy messages show in its toast.
  private readonly messageService = inject(MessageService);
  private readonly confirmationService = inject(ConfirmationService);

  command = input<CommandMessage | null>(null);
  /** The retries of this command among the loaded commands: the ones whose $retryOf names it. */
  retries = input<CommandMessage[]>([]);
  // Loading follows the id: a refreshed copy of the same command updates its fields without reloading or resetting the tabs.
  private readonly commandId = computed(() => this.command()?.id);
  // Derived once per command, not in the template: payloads can be large (see EventDetailComponent).
  readonly payloadJson = computed(() => { const c = this.command(); return c ? formatPayload(c.payload) : ''; });
  readonly metadata = computed(() => metadataEntries(this.command()?.metadata));

  producedEvents = signal<EventMessage[] | null>(null);
  /** Newest first, like the aggregate's events list; the backend returns them oldest first. */
  readonly producedEventsNewestFirst = computed(() => [...(this.producedEvents() ?? [])].reverse());
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
      const startedAt = Date.now();
      this.loading.set(true);
      this.request = this.svc.getEventsOfCommand(cmd).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(err => {
          this.messageService.add({ severity: 'error', summary: 'Error', detail: errorDetail(err, 'Failed to load the events of this command.') });
          this.finishLoading();
          return EMPTY;
        }),
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

  /**
   * Asks first: a retry sends the command again as a new command, handled against the aggregate as it is now, which may
   * differ from when it failed. Also says when it was retried already, e.g. by someone else.
   */
  confirmRetry() {
    const cmd = this.command();
    if (!cmd || this.retrying()) return;
    const retries = this.retries();
    const warning = retries.length > 0
      ? ` It was already retried ${retries.length === 1 ? 'once' : `${retries.length} times`}, last on ${new Date(retries[0].timestamp).toLocaleString()}.`
      : '';
    this.confirmationService.confirm({
      header: 'Retry command?',
      message: `${cmd.type} failed ${timeAgo(cmd.timestamp)}. It is sent again as a new command and handled against the aggregate as it is now, not as it was then.${warning}`,
      icon: 'pi pi-exclamation-triangle',
      acceptLabel: 'Retry',
      rejectLabel: 'Cancel',
      rejectButtonProps: { severity: 'secondary', text: true },
      acceptButtonProps: { severity: retries.length > 0 ? 'danger' : 'primary' },
      accept: () => this.retry(),
    });
  }

  private retry() {
    const cmd = this.command();
    if (!cmd || this.retrying()) return;
    // Like the skeletons: "Retrying…" stays at least MIN_LOADING_MS, so a fast response doesn't make the button flicker.
    const startedAt = Date.now();
    this.retrying.set(true);
    this.svc.retryCommand(cmd).pipe(
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
      this.messageService.add({ severity: 'success', summary: 'Retried', detail: 'Command has been resubmitted. Refresh to see its result.' });
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

/** "5 minutes ago", "3 days ago": how long ago a command was sent. */
function timeAgo(timestamp: string): string {
  const minutes = Math.max(0, Math.round((Date.now() - new Date(timestamp).getTime()) / 60_000));
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? '' : 's'} ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} hour${hours === 1 ? '' : 's'} ago`;
  const days = Math.round(hours / 24);
  return `${days} day${days === 1 ? '' : 's'} ago`;
}
