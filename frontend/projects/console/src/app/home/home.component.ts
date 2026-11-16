import {Component, afterNextRender, computed, inject} from '@angular/core';
import {BackendService} from '@eventify/ui/services/backend.service';
import {appState} from '@eventify/ui/status';
import {ConnectedApp, ConnectedAppsComponent} from '@eventify/ui/components/connected-apps.component';
import {SearchService} from '../services/search.service';
import {AppSwitcherComponent} from '../layout/app-switcher.component';
import {HeaderSearchComponent} from '../layout/header-search.component';

/**
 * The console's start page: the address to open an aggregate with, the aggregates opened before, and the applications
 * connected to the console, live, with how each one is doing.
 */
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  standalone: true,
  imports: [ConnectedAppsComponent, AppSwitcherComponent, HeaderSearchComponent],
  host: { class: 'block h-full' },
  styles: `
    .home-bg { background: var(--p-surface-50); }
    .home-stage { background: radial-gradient(var(--p-surface-200) 1px, transparent 1px) 0 0 / 16px 16px; }
    @media (prefers-color-scheme: dark) {
      .home-bg { background: var(--p-surface-950); }
      .home-stage { background: radial-gradient(var(--p-surface-800) 1px, transparent 1px) 0 0 / 16px 16px; }
    }
  `,
})
export class HomeComponent {
  readonly backend = inject(BackendService);
  readonly search = inject(SearchService);

  constructor() {
    // On a desktop the cursor starts in the ID; on a phone that would pop up the keyboard.
    afterNextRender(() => { if (window.innerWidth >= 1024) this.search.focusSearch(); });
  }

  readonly instanceCount = computed(() => this.backend.apps().reduce((sum, app) => sum + app.nodes.length, 0));

  /** The state of each application: the one of its instance worst off. */
  private readonly tones = computed(() => this.backend.apps().map(app => appState(app.nodes).tone));

  /**
   * The totals in the top row. Each application with a problem counts once, under its worst state: errors (error,
   * stopped) or warnings (rebalancing, restoring, starting, stopping, no answer). Colored once there are any.
   */
  readonly stats = computed(() => [
    { label: 'Applications', value: this.backend.apps().length, color: null },
    { label: 'Instances', value: this.instanceCount(), color: null },
    { label: 'Errors', value: this.tones().filter(tone => tone === 'error').length, color: 'text-red-500' },
    { label: 'Warnings', value: this.tones().filter(tone => tone === 'busy' || tone === 'unknown').length, color: 'text-amber-500' },
  ]);

  /** The picture in the center, each application with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => {
    const state = appState(app.nodes);
    return { name: app.name, note: state.text, tone: state.tone, instances: app.nodes };
  }));

}
