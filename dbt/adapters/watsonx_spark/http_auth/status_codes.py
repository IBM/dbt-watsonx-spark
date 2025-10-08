"""
Module for handling HTTP status codes and their corresponding error messages.
"""
from typing import Dict, Tuple, Optional, Callable
import logging

logger = logging.getLogger("dbt.adapters.watsonx_spark.http_auth")

class StatusCodeHandler:
    """
    Class for handling HTTP status codes and their corresponding error messages.
    """
    
    # Default error messages for common status codes
    DEFAULT_MESSAGES = {
        400: "Bad request. Please check your request parameters.",
        401: "Authentication failed. Please check your credentials.",
        403: "Permission denied. You don't have access to this resource.",
        404: "Resource not found.",
        429: "Too many requests. Please try again later.",
        500: "Internal server error. Please try again later or contact support.",
        502: "Bad gateway. Please try again later.",
        503: "Service unavailable. Please try again later.",
        504: "Gateway timeout. Please try again later."
    }
    
    # Status codes that are considered retryable
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    
    @classmethod
    def is_retryable(cls, status_code: int) -> bool:
        """
        Check if a status code is retryable.
        
        Args:
            status_code: The HTTP status code
            
        Returns:
            True if the status code is retryable, False otherwise
        """
        return status_code in cls.RETRYABLE_STATUS_CODES
    
    @classmethod
    def get_error_message(cls, status_code: int, context: str = "", response_text: str = "") -> str:
        """
        Get the error message for a status code.
        
        Args:
            status_code: The HTTP status code
            context: Additional context for the error message
            response_text: The response text from the server
            
        Returns:
            The error message for the status code
        """
        message = cls.DEFAULT_MESSAGES.get(status_code, f"Unexpected status code: {status_code}")
        
        if context:
            message = f"{context}: {message}"
            
        if response_text:
            message = f"{message} Response: {response_text}"
            
        return message
    
    @classmethod
    def handle_response(cls, 
                        response, 
                        context: str = "", 
                        error_handlers: Optional[Dict[int, Callable]] = None,
                        log_errors: bool = True) -> Tuple[bool, str]:
        """
        Handle an HTTP response.
        
        Args:
            response: The HTTP response object
            context: Additional context for the error message
            error_handlers: A dictionary mapping status codes to handler functions
            log_errors: Whether to log errors
            
        Returns:
            A tuple containing a boolean indicating success and an error message if applicable
        """
        status_code = response.status_code
        
        if 200 <= status_code < 300:
            return True, ""
            
        error_message = cls.get_error_message(status_code, context, response.text)
        
        if log_errors:
            logger.error(error_message)
            
        # Call custom handler if provided
        if error_handlers and status_code in error_handlers:
            return error_handlers[status_code](response, error_message)
            
        return False, error_message

