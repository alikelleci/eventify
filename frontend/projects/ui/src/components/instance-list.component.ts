import { Component, computed, input } from '@angular/core';
import { AppNode } from '../services/backend.service';
import { dotClass, instanceLabel, instanceState, numbered } from '../status';

/**
 * How each instance of an application is doing, one line each: "Instance 2   Error for 45 min". In number order, with the
 * dot showing which one has a problem. Shown in the tooltips of the application switcher and the home page.
 */
@Component({
  selector: 'app-instance-list',
  standalone: true,
  template: `
    <div class="flex flex-col gap-1 text-xs">
      @for (item of numbered(); track item.instance.nodeId) {
        <div class="flex items-center gap-2 whitespace-nowrap">
          <span class="h-1.5 w-1.5 shrink-0 rounded-full" [class]="dotClass(instanceState(item.instance.status).tone)"></span>
          <span class="font-medium">Instance {{ item.number }}</span>
          <span class="ml-auto pl-6 opacity-80">{{ instanceLabel(item.instance.status) }}</span>
        </div>
      } @empty {
        <div>No instances connected</div>
      }
    </div>
  `,
})
export class InstanceListComponent {
  readonly instances = input.required<AppNode[]>();

  readonly numbered = computed(() => numbered(this.instances()));
  readonly dotClass = dotClass;
  readonly instanceState = instanceState;
  readonly instanceLabel = instanceLabel;
}
