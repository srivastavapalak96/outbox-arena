package io.outboxarena.order.api;

import jakarta.validation.ConstraintViolationException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Translates bean-validation failures into clean 400 responses. */
@RestControllerAdvice
public class ApiErrorHandler {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<Map<String, Object>> handle(MethodArgumentNotValidException ex) {
    List<Map<String, String>> errors =
        ex.getBindingResult().getFieldErrors().stream()
            .map(
                f ->
                    Map.of("field", f.getField(), "message", String.valueOf(f.getDefaultMessage())))
            .toList();
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(
            Map.of(
                "timestamp",
                OffsetDateTime.now().toString(),
                "status",
                400,
                "error",
                "validation_failed",
                "details",
                errors));
  }

  @ExceptionHandler(ConstraintViolationException.class)
  public ResponseEntity<Map<String, Object>> handle(ConstraintViolationException ex) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(
            Map.of(
                "timestamp",
                OffsetDateTime.now().toString(),
                "status",
                400,
                "error",
                "constraint_violation",
                "message",
                ex.getMessage()));
  }
}
