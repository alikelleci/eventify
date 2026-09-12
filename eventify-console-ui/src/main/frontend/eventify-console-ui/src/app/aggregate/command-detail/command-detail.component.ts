import { Component, input, signal, effect, NgZone, inject, DestroyRef } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { SkeletonModule } from 'primeng/skeleton';
import { TagModule } from 'primeng/tag';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { CommandMessage, EventMessage } from '../../models';
import { EventifyService } from '../../eventify.service';
import { JsonHighlightPipe } from '../../shared/json-highlight.pipe';
import { EventDetailComponent } from '../event-detail/event-detail.component';

const MIN_SKELETON_MS = 300;

@Component({
  selector: 'app-command-detail',
  templateUrl: './command-detail.component.html',
  standalone: true,
  imports: [
    CommonModule, DatePipe,
    ButtonModule, DrawerModule, SkeletonModule, TagModule, TabsModule, TooltipModule,
    JsonHighlightPipe, EventDetailComponent,
  ],
})
export class CommandDetailComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly zone = inject(NgZone);

  command = input<CommandMessage | null>(null);

  producedEvents = signal<EventMessage[] | null>(null);
  loading = signal(false);
  activeTab = signal('payload');
  copiedKey = signal<string | null>(null);

  // Produced event drawer
  drawerVisible = signal(false);
  drawerEvent = signal<EventMessage | null>(null);

  private minElapsed = false;
  private dataReady = false;

  constructor() {
    effect(() => {
      const cmd = this.command();
      this.producedEvents.set(null);
      this.activeTab.set('payload');
      this.drawerVisible.set(false);
      this.drawerEvent.set(null);
      if (!cmd) { this.loading.set(false); return; }
      const correlationId = cmd.metadata['$correlationId'];
      if (!correlationId) { this.producedEvents.set([]); this.loading.set(false); return; }
      this.minElapsed = false;
      this.dataReady = false;
      this.loading.set(true);
      setTimeout(() => {
        this.minElapsed = true;
        if (this.dataReady) this.loading.set(false);
      }, MIN_SKELETON_MS);
      this.svc.getEventsByCorrelation(cmd.aggregateId, correlationId).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(() => { this.loading.set(false); return EMPTY; }),
      ).subscribe(page => {
        this.producedEvents.set(page.events);
        this.dataReady = true;
        if (this.minElapsed) this.loading.set(false);
      });
    });
  }

  openEvent(event: EventMessage) {
    this.drawerEvent.set(event);
    this.drawerVisible.set(true);
  }

  result(): 'success' | 'failure' | null {
    const r = this.command()?.metadata['$result'];
    return r === 'success' ? 'success' : r === 'failure' ? 'failure' : null;
  }

  cause(): string | null {
    return this.command()?.metadata['$cause'] ?? null;
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
}
