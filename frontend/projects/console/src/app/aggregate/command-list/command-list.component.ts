import { Component, DestroyRef, HostListener, OnInit, inject, input, output, signal } from '@angular/core';
import { DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';
import { TagModule } from 'primeng/tag';
import { MessageService } from 'primeng/api';
import { EventifyService } from '@eventify/ui/services/eventify.service';
import { CommandMessage, EventMessage } from '@eventify/ui/models';
import { ListSkeletonComponent } from '@eventify/ui/components/skeletons/list-skeleton.component';
import { afterMinLoading } from '@eventify/ui/loading-timing';
import { errorDetail } from '@eventify/ui/errors';
import { TimelineItemComponent, TimelineTone } from '@eventify/ui/components/timeline-item.component';

/** Loads and shows an aggregate's commands with their outcome. Commands are polled from Kafka, so this is slow. */
@Component({
  selector: 'app-command-list',
  templateUrl: './command-list.component.html',
  standalone: true,
  imports: [DatePipe, TagModule, ListSkeletonComponent, TimelineItemComponent],
  host: { class: 'block overflow-y-auto' },
})
export class CommandListComponent implements OnInit {
  private readonly svc = inject(EventifyService);
  private readonly messageService = inject(MessageService);
  private readonly destroyRef = inject(DestroyRef);

  aggregateId = input.required<string>();
  /** Whether this list's tab is open; only then does it respond to the arrow keys. */
  active = input(false);
  selected = input<EventMessage | CommandMessage | null>(null);

  select = output<CommandMessage>();
  /** Emits the commands as soon as they arrive. */
  loaded = output<CommandMessage[]>();

  commands = signal<CommandMessage[]>([]);
  /** How far back the commands were read, in days; null when the application didn't say (an older console client). */
  lookbackDays = signal<number | null>(null);
  /** Whether older commands were left out because there were more than the page holds. */
  truncated = signal(false);
  loading = signal(true);

  ngOnInit() {
    const startedAt = Date.now();
    this.svc.getCommands(this.aggregateId()).pipe(
      takeUntilDestroyed(this.destroyRef),
      catchError(err => {
        this.messageService.add({ severity: 'error', summary: 'Error', detail: errorDetail(err, 'Failed to load commands.') });
        this.loaded.emit([]);
        this.loading.set(false);
        return EMPTY;
      }),
    ).subscribe(page => {
      this.commands.set(page.commands);
      this.lookbackDays.set(page.lookbackDays ?? null);
      this.truncated.set(page.truncated ?? false);
      this.loaded.emit(page.commands);
      afterMinLoading(startedAt, () => this.loading.set(false));
    });
  }

  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    const list = this.commands();
    if (!this.active() || tag === 'INPUT' || tag === 'TEXTAREA' || !list.length) return;
    const idx = list.indexOf(this.selected() as CommandMessage);
    if (e.key === 'ArrowDown') { e.preventDefault(); this.select.emit(list[Math.min(idx + 1, list.length - 1)]); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); this.select.emit(list[Math.max(idx - 1, 0)]); }
  }

  result(command: CommandMessage): 'success' | 'failure' | null {
    const r = command.metadata['$result'];
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : null;
  }

  tone(command: CommandMessage): TimelineTone {
    const r = this.result(command);
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : 'neutral';
  }

  /** Retried from the console: it names the command it retries. */
  isRetry(command: CommandMessage): boolean {
    return !!command.metadata['$retryOf'];
  }
}
