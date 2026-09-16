import {Component, computed, inject} from '@angular/core';
import {BackendService} from '@eventify/ui/services/backend.service';
import {stateLabel, statusLabel} from '@eventify/ui/status';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';

/**
 * The console's start page: the totals, and the applications connected to it, live, with how each one is doing.
 * Picking an application happens in the header's application switcher, which shows the same status per application.
 */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [ConnectedAppsComponent],
  host: { class: 'block h-full' },
  styles: `
    .home-stage { background: var(--p-surface-50) radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 16px 16px; }
    @media (prefers-color-scheme: dark) {
      .home-stage { background: var(--p-surface-950) radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 16px 16px; }
    }
  `,
})
export class HomeComponent {
  readonly backend = inject(BackendService);

  readonly instanceCount = computed(() => this.backend.apps().reduce((sum, app) => sum + app.nodes.length, 0));

  /** The applications that are not simply running: rebalancing, restoring, or in error, from the moment it happens. */
  private readonly needAttention = computed(() => this.backend.apps()
    .map(app => statusLabel(app.status).tone)
    .filter(tone => tone === 'busy' || tone === 'error'));

  /** The totals in the top row; the applications that need attention are coloured once there are any. */
  readonly stats = computed(() => [
    { label: 'Applications', value: String(this.backend.apps().length), warn: false },
    { label: 'Instances', value: String(this.instanceCount()), warn: false },
    { label: 'Need attention', value: String(this.needAttention().length), warn: true },
  ]);

  /** The picture in the centre, each application with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => ({
    name: app.name,
    note: stateLabel(app.status).text,
    tone: stateLabel(app.status).tone,
  })));

}
