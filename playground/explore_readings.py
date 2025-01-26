import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from src.extraction import load_json_readings, get_data_summary, DEFAULT_READINGS_PATH
from rich import print
from rich.table import Table
from rich.console import Console

def load_readings():
    """Load readings data from JSON files"""
    return load_json_readings(DEFAULT_READINGS_PATH)

def display_readings_summary(readings_df):
    """Display summary statistics about the readings"""
    console = Console()
    
    # Get summary using the existing function
    readings_summary = get_data_summary(readings_df)
    
    # Create summary table
    table = Table(title="Readings Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    
    # Add summary statistics
    table.add_row("Total Readings", f"{readings_summary['total_readings']:,}")
    table.add_row("Unique Meterpoints", f"{readings_summary['unique_meterpoints']:,}")
    table.add_row("Date Range", 
                 f"{readings_summary['date_range']['start']} to {readings_summary['date_range']['end']}")
    table.add_row("Total Consumption", f"{readings_summary['total_consumption']:,.2f}")
    table.add_row("Average Consumption", f"{readings_summary['average_consumption']:.4f}")
    
    console.print(table)

def explore_readings():
    """Main function to explore readings data"""
    readings_df = load_readings()
    
    print("\n[bold cyan]Exploring Readings Data[/bold cyan]")
    
    # Display summary statistics
    display_readings_summary(readings_df)
    
    # Show sample of readings
    print("\n[bold]Sample of readings:[/bold]")
    print(readings_df.head())

if __name__ == "__main__":
    explore_readings()
