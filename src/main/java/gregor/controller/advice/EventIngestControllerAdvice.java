package gregor.controller.advice;

import gregor.controller.EventIngestController;
import gregor.exception.EventPublicationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = EventIngestController.class)
public class EventIngestControllerAdvice {

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentNotValidException.class})
    public ProblemDetail handleInvalidRequest() {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request body is malformed or invalid.");
        problemDetail.setTitle("Invalid request");
        return problemDetail;
    }

    @ExceptionHandler(EventPublicationException.class)
    public ProblemDetail handlePublicationFailure() {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "The event could not be published.");
        problemDetail.setTitle("Event publication unavailable");
        return problemDetail;
    }
}
