import {Component, computed, inject} from '@angular/core';
import {BackendService} from '@eventify/ui/services/backend.service';
import {appState} from '@eventify/ui/status';
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

  /** The state of each application: the one of its instance worst off. */
  private readonly tones = computed(() => this.backend.apps().map(app => appState(app.nodes).tone));

  /**
   * The totals in the top row. Each application with a problem counts once, under its worst state: errors (error,
   * stopped) or warnings (rebalancing, restoring, starting, stopping, no answer). Coloured once there are any.
   */
  readonly stats = computed(() => [
    { label: 'Applications', value: this.backend.apps().length, color: null },
    { label: 'Instances', value: this.instanceCount(), color: null },
    { label: 'Errors', value: this.tones().filter(tone => tone === 'error').length, color: 'text-red-500' },
    { label: 'Warnings', value: this.tones().filter(tone => tone === 'busy' || tone === 'unknown').length, color: 'text-amber-500' },
  ]);

  /** The picture in the centre, each application with its number of instances. */
  readonly connectedApps = computed<ConnectedApp[]>(() => this.backend.apps().map(app => {
    const state = appState(app.nodes);
    return { name: app.name, note: state.text, tone: state.tone, instances: app.nodes };
  }));

}
