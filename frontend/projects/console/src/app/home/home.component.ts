import {Component, DestroyRef, computed, inject, signal} from '@angular/core';
import {SearchService} from '../services/search.service';
import {AppEntry, AppNode, BackendService} from '@eventify/ui/services/backend.service';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';

/** A card lists at most this many instances; the rest are counted. */
const MAX_INSTANCES = 3;
/** From this many applications on, a filter helps to find one. */
const FILTER_FROM = 7;

/**
 * The console's start page: a live overview of the applications connected to it and their instances.
 * Picking an application makes it the one the header searches in.
 */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [ConnectedAppsComponent],
  host: { class: 'block h-full' },
  styles: `
    .home-stage { background: var(--p-surface-50) radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 14px 14px; }
    @media (prefers-color-scheme: dark) {
      .home-stage { background: var(--p-surface-950) radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 14px 14px; }
    }
  `,
})
export class HomeComponent {
  private readonly search = inject(SearchService);
  readonly backend = inject(BackendService);

  readonly maxInstances = MAX_INSTANCES;

  /** Ticks every half minute, so "connected 5 min ago" stays true without a new list. */
  private readonly now = signal(Date.now());

  readonly query = signal('');
  readonly showFilter = computed(() => this.backend.apps().length >= FILTER_FROM);
  readonly filteredApps = computed(() => {
    const query = this.query().trim().toLowerCase();
    return query ? this.backend.apps().filter(app => app.name.toLowerCase().includes(query)) : this.backend.apps();
  });

  readonly instanceCount = computed(() => this.backend.apps().reduce((sum, app) => sum + app.nodes.length, 0));
  readonly offlineCount = computed(() => this.backend.apps().filter(app => app.nodes.length === 0).length);

  /** The picture at the top, each application with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => ({
    name: app.name,
    note: app.nodes.length ? this.plural(app.nodes.length, 'instance') : 'offline',
  })));

  constructor() {
    const timer = setInterval(() => this.now.set(Date.now()), 30_000);
    inject(DestroyRef).onDestroy(() => clearInterval(timer));
  }

  /** Makes the application the one to search in, and puts the cursor in the search box. */
  open(app: AppEntry) {
    this.backend.setActiveApp(app);
    this.search.focusSearch();
  }

  // By name: the list is refreshed every few seconds with new objects.
  isActive(app: AppEntry): boolean {
    return app.name === this.backend.activeApp()?.name;
  }

  /** The Eventify versions its instances run, when they differ (e.g. while an upgrade rolls out). */
  versions(app: AppEntry): string[] {
    const versions = [...new Set(app.nodes.map(node => node.version).filter((v): v is string => !!v))];
    return versions.length > 1 ? versions : [];
  }

  /** The instance's host name, or the start of its id when it didn't send one. */
  instanceName(node: AppNode): string {
    return node.hostname ?? node.nodeId.split(':')[0];
  }

  connectedFor(node: AppNode): string {
    const minutes = Math.floor((this.now() - Date.parse(node.connectedAt)) / 60_000);
    if (minutes < 1) return 'just now';
    if (minutes < 60) return `${minutes} min`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours} h`;
    return this.plural(Math.floor(hours / 24), 'day');
  }

  plural(count: number, word: string): string {
    return `${count} ${word}${count === 1 ? '' : 's'}`;
  }
}
