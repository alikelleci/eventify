import {Component, computed, inject} from '@angular/core';
import {BackendService, mixedVersions} from '@eventify/ui/services/backend.service';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';

/**
 * The console's start page: the totals, and the applications connected to it, live.
 * Picking an application, with its instances and versions, happens in the header's application switcher.
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
  readonly mixedCount = computed(() => this.backend.apps().filter(app => mixedVersions(app).length > 0).length);

  /** The totals in the top row; mixed versions is coloured once there are any. */
  readonly stats = computed(() => [
    { label: 'Applications', value: this.backend.apps().length, warn: false },
    { label: 'Instances', value: this.instanceCount(), warn: false },
    { label: 'Mixed versions', value: this.mixedCount(), warn: true },
  ]);

  /** The picture in the centre, each application with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => ({
    name: app.name,
    note: app.nodes.length ? this.plural(app.nodes.length, 'instance') : 'offline',
    offline: app.nodes.length === 0,
  })));

  private plural(count: number, word: string): string {
    return `${count} ${word}${count === 1 ? '' : 's'}`;
  }
}
