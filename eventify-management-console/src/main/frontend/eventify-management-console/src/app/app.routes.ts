import { Routes } from '@angular/router';
import { EventsComponent } from './events/events.component';

export const routes: Routes = [
  { path: '', component: EventsComponent },
  { path: ':aggregateId', component: EventsComponent },
  { path: ':aggregateId/:eventId', component: EventsComponent },
  { path: '**', redirectTo: '' },
];
