import { Component, input, signal, effect, NgZone, inject, DestroyRef } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, EMPTY } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { SkeletonModule } from 'primeng/skeleton';
import { TagModule } from 'primeng/tag';
import { TabsModule } from 'primeng/tabs';
import { TooltipModule } from 'primeng/tooltip';
import { EventDetail, EventMessage } from '../../models';
import { EventifyService } from '../../eventify.service';
import { JsonHighlightPipe } from '../../shared/json-highlight.pipe';
import { JsonDiffPipe } from '../../shared/json-diff.pipe';

const MIN_SKELETON_MS = 300;

@Component({
  selector: 'app-event-detail',
  templateUrl: './event-detail.component.html',
  standalone: true,
  imports: [
    CommonModule, DatePipe,
    ButtonModule, SkeletonModule, TagModule, TabsModule, TooltipModule,
    JsonHighlightPipe, JsonDiffPipe,
  ],
})
export class EventDetailComponent {
  private readonly svc = inject(EventifyService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly zone = inject(NgZone);

  event = input<EventMessage | null>(null);

  detail = signal<EventDetail | null>(null);
  loading = signal(false);
  activeTab = signal('payload');
  showDiff = signal(false);
  copiedKey = signal<string | null>(null);

  private minElapsed = false;
  private dataReady = false;

  constructor() {
    effect(() => {
      const ev = this.event();
      this.detail.set(null);
      this.activeTab.set('payload');
      this.showDiff.set(false);
      if (!ev) { this.loading.set(false); return; }
      this.minElapsed = false;
      this.dataReady = false;
      this.loading.set(true);
      setTimeout(() => {
        this.minElapsed = true;
        if (this.dataReady) this.loading.set(false);
      }, MIN_SKELETON_MS);
      this.svc.getEventDetail(ev.aggregateId, ev.id).pipe(
        takeUntilDestroyed(this.destroyRef),
        catchError(() => { this.loading.set(false); return EMPTY; }),
      ).subscribe(detail => {
        this.detail.set(detail);
        this.dataReady = true;
        if (this.minElapsed) this.loading.set(false);
      });
    });
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
