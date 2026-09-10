import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { EventDetail, EventsPage } from './models';

@Injectable({ providedIn: 'root' })
export class EventifyService {
  private readonly http = inject(HttpClient);

  getEvents(aggregateId: string, cursor?: string | null, limit = 50): Observable<EventsPage> {
    let params = new HttpParams().set('limit', limit);
    if (cursor) params = params.set('cursor', cursor);
    return this.http.get<EventsPage>(`/api/aggregates/${encodeURIComponent(aggregateId)}/events`, { params });
  }

  getEventDetail(aggregateId: string, eventId: string): Observable<EventDetail> {
    return this.http.get<EventDetail>(`/api/aggregates/${encodeURIComponent(aggregateId)}/events/${encodeURIComponent(eventId)}`);
  }
}
