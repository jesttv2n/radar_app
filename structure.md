nord-radar-worker/
├── src/
│ ├── **init**.py
│ ├── main.py # Entry point
│ ├── config.py # Configuration management
│ ├── models.py # Data models
│ ├── exceptions.py # Custom exceptions
│ ├── utils.py # Utility functions
│ ├── downloaders/
│ │ ├── **init**.py
│ │ └── dmi_downloader.py # DMI API interface
│ ├── processors/
│ │ ├── **init**.py
│ │ └── radar_processor.py # HDF5 to PNG processing
│ ├── forecasters/
│ │ ├── **init**.py
│ │ ├── fluid_dynamics_forecaster.py # Basic forecaster
│ │ └── advanced_forecasting/
│ │ ├── **init**.py
│ │ └── fluid_dynamics_advanced.py # Advanced forecaster
│ ├── uploaders/
│ │ ├── **init**.py
│ │ └── s3_uploader.py # S3 upload handler
│ └── monitoring/
│ ├── **init**.py
│ ├── metrics.py # Prometheus metrics
│ └── health.py # Health checks
├── tests/
│ ├── **init**.py
│ ├── test_downloaders.py
│ ├── test_processors.py
│ ├── test_forecasters.py
│ └── fixtures/
│ └── sample_data.h5
├── static/
│ └── TV2.ttf # Font file
├── monitoring/
│ └── prometheus.yml # Prometheus config
├── scripts/
│ ├── migrate_from_legacy.py # Migration script
│ └── performance_test.py # Performance testing
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── healthcheck.py
