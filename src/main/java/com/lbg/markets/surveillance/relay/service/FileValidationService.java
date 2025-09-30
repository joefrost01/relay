package com.lbg.markets.surveillance.relay.service;

import jakarta.enterprise.context.ApplicationScoped;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class FileValidationService {

    public ValidationResult validateFile(String sourceSystem, Path filePath) {
        // Implement validation logic
        return new ValidationResult(true, new ArrayList<>());
    }

    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;

        public ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}