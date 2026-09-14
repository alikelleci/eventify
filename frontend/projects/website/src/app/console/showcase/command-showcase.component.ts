import { Component, ViewChild, computed, input } from '@angular/core';
import { of } from 'rxjs';
import { MessageService } from 'primeng/api';
import { CommandMessage, EventMessage } from '@eventify/ui/models';
import { CommandDetailComponent } from '@eventify/ui/components/command-detail/command-detail.component';
import { EventifyService } from '@eventify/ui/services/eventify.service';

const AGGREGATE_ID = 'order-7f3a';
const secondsAgo = (s: number) => new Date(Date.now() - s * 1000).toISOString();

const SHIP_ORDER: CommandMessage = {
  id: `${AGGREGATE_ID}@0000000000006`,
  type: 'ShipOrder',
  timestamp: secondsAgo(20.03),
  aggregateId: AGGREGATE_ID,
  payload: { '@class': 'com.example.OrderCommand$ShipOrder', id: AGGREGATE_ID, carrier: 'DHL' },
  metadata: { '$correlationId': 'corr-ship-order', '$result': 'success', '$replyTo': 'orders.replies' },
};

// The events ShipOrder produced, oldest first (as the console shows them).
const PRODUCED: EventMessage[] = [
  { type: 'ShipmentLabelCreated', seconds: 20.02, extra: { trackingNumber: 'JD014600003SE' } },
  { type: 'OrderShipped', seconds: 20.01, extra: { carrier: 'DHL' } },
].map(({ type, seconds, extra }, i) => ({
  id: `${AGGREGATE_ID}@000000000000${i + 5}`,
  type,
  timestamp: secondsAgo(seconds),
  aggregateId: AGGREGATE_ID,
  revision: 1,
  payload: { '@class': `com.example.OrderEvent$${type}`, id: AGGREGATE_ID, ...extra },
  metadata: { '$correlationId': 'corr-ship-order' },
}));

const APPLY_DISCOUNT: CommandMessage = {
  id: `${AGGREGATE_ID}@0000000000004`,
  type: 'ApplyDiscount',
  timestamp: secondsAgo(90),
  aggregateId: AGGREGATE_ID,
  payload: { '@class': 'com.example.OrderCommand$ApplyDiscount', id: AGGREGATE_ID, code: 'SUMMER24' },
  metadata: {
    '$correlationId': 'corr-apply-discount',
    '$result': 'failure',
    '$cause': 'Discount code SUMMER24 expired on 31 August.',
    '$replyTo': 'orders.replies',
  },
};

/**
 * Landing page showcase: the real command details component, either a failed command (cause and Retry)
 * or a successful one with the events it produced. Its data comes from a stand-in for the API, so it looks
 * exactly like the console and follows any change to that screen.
 */
@Component({
  selector: 'app-command-showcase',
  standalone: true,
  imports: [CommandDetailComponent],
  providers: [
    MessageService,
    {
      provide: EventifyService,
      useValue: {
        getEventsByCorrelation: (_: string, correlationId: string) =>
          of({ events: correlationId === SHIP_ORDER.metadata['$correlationId'] ? PRODUCED : [] }),
        retryCommand: () => of(undefined),  // never called: the showcase is inert, but Retry is part of the component
      },
    },
  ],
  template: `<app-command-detail [command]="command()" (loaded)="openTab()" />`,
})
export class CommandShowcaseComponent {
  @ViewChild(CommandDetailComponent) private detail!: CommandDetailComponent;

  variant = input<'failed' | 'produced'>('failed');
  command = computed(() => this.variant() === 'failed' ? APPLY_DISCOUNT : SHIP_ORDER);

  openTab() {
    this.detail.activeTab.set(this.variant() === 'failed' ? 'payload' : 'events');
  }
}
