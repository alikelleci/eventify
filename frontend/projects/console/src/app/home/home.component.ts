import {Component, DestroyRef, computed, inject, signal} from '@angular/core';
import {DatePipe} from '@angular/common';
import {DrawerModule} from 'primeng/drawer';
import {AppEntry, AppNode, BackendService} from '@eventify/ui/services/backend.service';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';

/** From this many applications on, a filter helps to find one. */
const FILTER_FROM = 7;

/**
 * The console's start page: the applications connected to it, live. The console with its applications is the centre;
 * below it a simple card per application, which opens a drawer with its instances.
 */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [DatePipe, DrawerModule, ConnectedAppsComponent],
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

  /** Ticks every half minute, so "connected 5 min ago" stays true without a new list. */
  private readonly now = signal(Date.now());

  readonly query = signal('');
  readonly showFilter = computed(() => this.backend.apps().length >= FILTER_FROM);
  readonly filteredApps = computed(() => {
    const query = this.query().trim().toLowerCase();
    return query ? this.backend.apps().filter(app => app.name.toLowerCase().includes(query)) : this.backend.apps();
  });

  readonly instanceCount = computed(() => this.backend.apps().reduce((sum, app) => sum + app.nodes.length, 0));
  readonly mixedCount = computed(() => this.backend.apps().filter(app => this.versions(app).length > 0).length);

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

  /** The application in the drawer, by name: the list is refreshed every few seconds with new objects. */
  private readonly detailName = signal<string | null>(null);
  readonly detail = computed(() => this.backend.apps().find(app => app.name === this.detailName()) ?? null);
  readonly drawerVisible = signal(false);

  constructor() {
    const timer = setInterval(() => this.now.set(Date.now()), 30_000);
    inject(DestroyRef).onDestroy(() => clearInterval(timer));
  }

  showDetail(app: AppEntry) {
    this.detailName.set(app.name);
    this.drawerVisible.set(true);
  }

  // By name: the list is refreshed every few seconds with new objects.
  isActive(app: AppEntry): boolean {
    return app.name === this.backend.activeApp()?.name;
  }

  /** The versions its instances run, when they differ (e.g. while an upgrade rolls out). */
  versions(app: AppEntry): string[] {
    const versions = [...new Set(app.nodes.map(node => node.version).filter((v): v is string => !!v))];
    return versions.length > 1 ? versions : [];
  }

  /** One dot per instance on a card, at most this many. */
  readonly maxDots = 8;

  /** The instance's host name, or the start of its id when it didn't send one. */
  instanceName(node: AppNode): string {
    return node.hostname ?? node.nodeId.split(':')[0];
  }

  /** How long ago the instance connected to this console (the console records the moment; a reconnect starts it over). */
  connectedFor(node: AppNode): string {
    const minutes = Math.floor((this.now() - Date.parse(node.connectedAt)) / 60_000);
    if (minutes < 1) return 'connected just now';
    if (minutes < 60) return `connected ${minutes} min ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `connected ${hours} h ago`;
    return `connected ${this.plural(Math.floor(hours / 24), 'day')} ago`;
  }

  plural(count: number, word: string): string {
    return `${count} ${word}${count === 1 ? '' : 's'}`;
  }
}
