import { Component, inject } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { SearchService } from '../services/search.service';
import { TimelineItemComponent } from '@eventify/ui/components/timeline-item.component';
import { DOCS_URL } from '@eventify/ui/links';

interface Feature {
  icon: string;
  title: string;
  text: string;
}

/** The console's start page: what it does and where to begin. The full product page is the website's landing. */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [DatePipe, ButtonModule, TagModule, TimelineItemComponent],
  host: { class: 'block h-full' },
})
export class HomeComponent {
  readonly search = inject(SearchService);

  readonly docsUrl = DOCS_URL;

  /** Example data for the illustration, newest first like the lists in the app. */
  readonly now = Date.now();
  // The failed command is second, so it stays visible above the events card in front.
  readonly illustrationCommands = [
    { type: 'ShipOrder', agoMs: 20_020, failed: false },
    { type: 'ApplyDiscount', agoMs: 90_000, failed: true },
    { type: 'CapturePayment', agoMs: 140_010, failed: false },
    { type: 'PlaceOrder', agoMs: 380_010, failed: false },
  ];
  readonly illustrationEvents = [
    { type: 'OrderShipped', agoMs: 20_000 },
    { type: 'ShipmentLabelCreated', agoMs: 20_012 },
    { type: 'PaymentReceived', agoMs: 140_000 },
    { type: 'OrderPlaced', agoMs: 380_000 },
  ];

  readonly features: Feature[] = [
    { icon: 'pi pi-list', title: 'Event history', text: 'Every event of an aggregate, newest first, with its payload and metadata.' },
    { icon: 'pi pi-database', title: 'State at every event', text: 'See the aggregate as it was after each event, and diff it against the state before.' },
    { icon: 'pi pi-send', title: 'Commands and outcomes', text: 'Follow each command and its result: succeeded, failed with a cause, or retried.' },
    { icon: 'pi pi-refresh', title: 'Trace and retry', text: 'Jump from a command to the events it produced, and retry failed commands in one click.' },
  ];
}
