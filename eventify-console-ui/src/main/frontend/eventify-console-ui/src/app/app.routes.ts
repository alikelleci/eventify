import { Routes } from '@angular/router';
import { EventsComponent } from './events/events.component';
import { CommandsComponent } from './commands/commands.component';

export const routes: Routes = [
  { path: 'events', component: EventsComponent },
  { path: 'commands', component: CommandsComponent },
  { path: '**', redirectTo: 'events' },
];
