import { Component } from '@angular/core';
import { DatePipe } from '@angular/common';
import { TagModule } from 'primeng/tag';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';

/** The console at a glance: its commands list with the events list in front, used on the Console page and the home page. */
@Component({
  selector: 'app-console-illustration',
  standalone: true,
  imports: [DatePipe, TagModule, TimelineItemComponent],
  template: `
    <!-- Built from the app's own timeline rows. The front card starts just right of the back card's dots, so its
         timeline stays visible and no text is cut. Static and inert (not focusable or clickable). -->
    <div aria-hidden="true" inert>
      <!-- Back: commands -->
      <div class="mr-12 overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 shadow-[0_12px_32px_-18px_rgba(15,23,42,0.25)] dark:border-surface-700 dark:bg-surface-900">
        @for (command of commands; track command.type) {
          <app-timeline-item [tone]="command.failed ? 'failure' : 'success'" [interactive]="false" [first]="$first" [last]="$last">
            <div class="flex flex-col flex-1 min-w-0">
              <div class="flex items-center gap-2 h-[22px]">
                <span class="font-medium text-sm truncate">{{ command.type }}</span>
                @if (command.failed) { <p-tag value="failure" severity="danger" styleClass="shrink-0" /> }
              </div>
              <span class="text-xs text-surface-400 mt-0.5">{{ now - command.agoMs | date:'MMM d, y · HH:mm:ss' }}</span>
            </div>
            <i class="pi pi-chevron-right text-surface-400 text-sm shrink-0"></i>
          </app-timeline-item>
        }
      </div>

      <!-- Front: events -->
      <div class="relative -mt-[123px] ml-[38px] overflow-hidden rounded-2xl border border-surface-200 bg-surface-0 shadow-[0_32px_64px_-24px_rgba(15,23,42,0.45)] dark:border-surface-700 dark:bg-surface-900">
        @for (event of events; track event.type) {
          <app-timeline-item [selected]="$index === 2" [interactive]="false" [first]="$first" [last]="$last">
            <div class="flex flex-col flex-1 min-w-0">
              <div class="flex items-center gap-2 h-[22px]">
                <span class="font-medium text-sm truncate">{{ event.type }}</span>
              </div>
              <span class="text-xs text-surface-400 mt-0.5">{{ now - event.agoMs | date:'MMM d, y · HH:mm:ss' }}</span>
            </div>
            <i class="pi pi-chevron-right text-surface-400 text-sm shrink-0"></i>
          </app-timeline-item>
        }
      </div>
    </div>
  `,
})
export class ConsoleIllustrationComponent {
  /** Example data, newest first like the lists in the app; the times are relative to now, so they always look recent. */
  readonly now = Date.now();
  // The failed command is second, so it stays visible above the events card in front.
  readonly commands = [
    { type: 'ShipOrder', agoMs: 20_020, failed: false },
    { type: 'ApplyDiscount', agoMs: 90_000, failed: true },
    { type: 'CapturePayment', agoMs: 140_010, failed: false },
    { type: 'PlaceOrder', agoMs: 380_010, failed: false },
  ];
  readonly events = [
    { type: 'OrderShipped', agoMs: 20_000 },
    { type: 'ShipmentLabelCreated', agoMs: 20_012 },
    { type: 'PaymentReceived', agoMs: 140_000 },
    { type: 'OrderPlaced', agoMs: 380_000 },
  ];
}
