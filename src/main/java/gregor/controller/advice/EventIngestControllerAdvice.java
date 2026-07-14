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
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST,
                "Request body is malformed or invalid."
        );
        problem.setTitle("Invalid request");
        return problem;
    }

    @ExceptionHandler(EventPublicationException.class)
    public ProblemDetail handlePublicationFailure() {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "The event could not be published."
        );
        problem.setTitle("Event publication unavailable");
        return problem;
    }
}
