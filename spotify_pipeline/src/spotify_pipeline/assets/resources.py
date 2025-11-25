"""Resources configuration for the Spotify pipeline."""

from dagster import ConfigurableResource
from pathlib import Path
from dagster import IOManager, io_manager

class FileConfig(ConfigurableResource):
    """Configuration for file paths in the pipeline."""
    
    # Path to the Spotify CSV (optional - will look in data/ folder)
    spotify_csv_path: str | None = None
    
    # Output directories
    data_dir: str = "data"
    models_dir: str = "models"
    
    @property
    def data_path(self) -> Path:
        """Get the data directory as a Path object."""
        return Path(self.data_dir).expanduser().resolve()
    
    @property
    def models_path(self) -> Path:
        """Get the models directory as a Path object."""
        return Path(self.models_dir).expanduser().resolve()

@io_manager
def custom_io_manager():
    """Custom I/O manager for handling data input and output."""
    class CustomIOManager(IOManager):
        def handle_output(self, context, obj):
            # Implement logic to handle output
            pass
        
        def load_input(self, context):
            # Implement logic to load input
            pass
    
    return CustomIOManager()