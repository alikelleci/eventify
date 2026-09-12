import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { CommandsPage, CorrelatedEventsPage, EventDetail, EventsPage } from './models';
import { BackendService } from './backend.service';

@Injectable({ providedIn: 'root' })
export class EventifyService {
  private readonly http = inject(HttpClient);
  private readonly backend = inject(BackendService);

  getEvents(aggregateId: string, cursor?: string | null, limit = 50): Observable<EventsPage> {
    let params = new HttpParams().set('limit', limit);
    if (cursor) params = params.set('cursor', cursor);
    return this.http.get<EventsPage>(`${this.backend.baseUrl()}/api/aggregates/${encodeURIComponent(aggregateId)}/events`, { params });
  }

  getEventDetail(aggregateId: string, eventId: string): Observable<EventDetail> {
    return this.http.get<EventDetail>(`${this.backend.baseUrl()}/api/aggregates/${encodeURIComponent(aggregateId)}/events/${encodeURIComponent(eventId)}`);
  }

  getEventsByCorrelation(aggregateId: string, correlationId: string): Observable<CorrelatedEventsPage> {
    return this.http.get<CorrelatedEventsPage>(`${this.backend.baseUrl()}/api/aggregates/${encodeURIComponent(aggregateId)}/events/by-correlation/${encodeURIComponent(correlationId)}`);
  }

  getCommands(aggregateId: string, limit = 500): Observable<CommandsPage> {
    const params = new HttpParams().set('limit', limit);
    return this.http.get<CommandsPage>(`${this.backend.baseUrl()}/api/aggregates/${encodeURIComponent(aggregateId)}/commands`, { params });
  }
}
