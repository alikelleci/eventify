import {Component, computed, inject} from '@angular/core';
import {ButtonModule} from 'primeng/button';
import {TagModule} from 'primeng/tag';
import {SearchService} from '../services/search.service';
import {DOCS_URL} from '@eventify/ui/links';
import {BackendService} from '@eventify/ui/services/backend.service';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';

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
  imports: [ButtonModule, TagModule, ConnectedAppsComponent],
  host: { class: 'block h-full' },
})
export class HomeComponent {
  readonly search = inject(SearchService);
  private readonly backend = inject(BackendService);

  /** The applications connected right now, each with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => ({
    name: app.name,
    note: app.nodes.length === 1 ? '1 instance' : `${app.nodes.length} instances`,
  })));
  readonly connectedLabel = computed(() => {
    const count = this.backend.apps().length;
    return count === 1 ? '1 application connected' : `${count} applications connected`;
  });

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
    { icon: 'pi pi-list', title: 'Event history', text: 'See every event in order from oldest to newest, understand what happened and when.' },
    { icon: 'pi pi-database', title: 'State at every event', text: 'View the aggregate exactly as it was after each event to understand the complete picture.' },
    { icon: 'pi pi-send', title: 'Commands and outcomes', text: 'Follow each command and see clearly what happened: succeeded, failed and why.' },
    { icon: 'pi pi-refresh', title: 'Trace commands to events', text: 'See which events a command produced and understand the complete command flow.' },
  ];
}
