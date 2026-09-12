import { Routes } from '@angular/router';
import { EventsComponent } from './events/events.component';

export const routes: Routes = [
  { path: 'events', component: EventsComponent },
  { path: '**', redirectTo: 'events' },
];
