import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { catchError, throwError } from 'rxjs';

/**
 * The login has expired (the console answers 401): reload the page, so the console sends the browser to the identity
 * provider to log in again, and back to this page afterwards.
 */
export const loginInterceptor: HttpInterceptorFn = (req, next) =>
  next(req).pipe(
    catchError(err => {
      if (err instanceof HttpErrorResponse && err.status === 401) window.location.reload();
      return throwError(() => err);
    }),
  );
