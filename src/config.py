"""
Configuration management for DeFtunes data pipeline.

Handles environment variables, AWS settings, and pipeline configuration.
"""

import os
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class Environment(str, Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class AWSConfig:
    """AWS service configuration."""
    region: str
    s3_bucket: str
    glue_database: str
    redshift_cluster: str
    redshift_database: str
    redshift_user: str
    redshift_password: str
    
    @classmethod
    def from_env(cls) -> 'AWSConfig':
        """Create AWS config from environment variables."""
        return cls(
            region=os.getenv('AWS_REGION', 'us-east-1'),
            s3_bucket=os.getenv('S3_BUCKET', 'deftunes-data-lake'),
            glue_database=os.getenv('GLUE_DATABASE', 'deftunes_db'),
            redshift_cluster=os.getenv('REDSHIFT_CLUSTER', 'deftunes-cluster'),
            redshift_database=os.getenv('REDSHIFT_DATABASE', 'dev'),
            redshift_user=os.getenv('REDSHIFT_USER', 'deftunes_user'),
            redshift_password=os.getenv('REDSHIFT_PASSWORD', ''),
        )


@dataclass
class APIConfig:
    """API configuration."""
    base_url: str
    timeout: int
    max_retries: int
    rate_limit: int
    
    @classmethod
    def from_env(cls) -> 'APIConfig':
        """Create API config from environment variables."""
        return cls(
            base_url=os.getenv('API_BASE_URL', 'http://localhost:8000'),
            timeout=int(os.getenv('API_TIMEOUT', '30')),
            max_retries=int(os.getenv('API_MAX_RETRIES', '3')),
            rate_limit=int(os.getenv('API_RATE_LIMIT', '100')),
        )


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create database config from environment variables."""
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'deftunes'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
        )
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    environment: Environment
    log_level: str
    batch_size: int
    max_workers: int
    
    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        """Create pipeline config from environment variables."""
        return cls(
            environment=Environment(os.getenv('ENVIRONMENT', 'development')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_workers=int(os.getenv('MAX_WORKERS', '4')),
        )


class Settings:
    """Application settings."""
    
    def __init__(self):
        self.aws = AWSConfig.from_env()
        self.api = APIConfig.from_env()
        self.database = DatabaseConfig.from_env()
        self.pipeline = PipelineConfig.from_env()
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.pipeline.environment == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.pipeline.environment == Environment.DEVELOPMENT
    
    def get_s3_path(self, zone: str, table: str) -> str:
        """Get S3 path for a specific zone and table."""
        return f"s3://{self.aws.s3_bucket}/{zone}/{table}/"
    
    def get_glue_job_name(self, job_type: str) -> str:
        """Get Glue job name with environment prefix."""
        env = self.pipeline.environment.value
        return f"deftunes-{env}-{job_type}"


# Global settings instance
settings = Settings()


def get_environment() -> Environment:
    """Get current environment."""
    return settings.pipeline.environment


def is_production() -> bool:
    """Check if running in production environment."""
    return settings.is_production


def get_aws_config() -> AWSConfig:
    """Get AWS configuration."""
    return settings.aws


def get_api_config() -> APIConfig:
    """Get API configuration."""
    return settings.api


def get_database_config() -> DatabaseConfig:
    """Get database configuration."""
    return settings.database


# Environment-specific overrides
if settings.pipeline.environment == Environment.PRODUCTION:
    # Production-specific settings
    settings.pipeline.log_level = 'WARNING'
    settings.pipeline.max_workers = 8
elif settings.pipeline.environment == Environment.DEVELOPMENT:
    # Development-specific settings
    settings.pipeline.log_level = 'DEBUG'
    settings.pipeline.batch_size = 100 