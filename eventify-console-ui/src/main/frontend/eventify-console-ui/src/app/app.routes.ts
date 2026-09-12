import { Routes } from '@angular/router';
import { AggregateComponent } from './aggregate/aggregate.component';

export const routes: Routes = [
  { path: '', component: AggregateComponent },
  { path: '**', redirectTo: '' },
];
